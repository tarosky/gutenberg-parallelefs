package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	log "github.com/sirupsen/logrus"

	"sync"
	"time"
)

type content []byte

type writeTask struct {
	Destination string  `json:"dest"`
	SourcePath  *string `json:"src"`
	Content     content `json:"content_b64"` // Never use Content for a large file.
}

type session struct {
	openFiles   []*os.File
	finalizeMux *sync.Mutex
	// pooledFiles []*os.File
}

const copyBufferSize = 10 * 1024
const filePoolSize = 100

// const dirPoolSize = 10

func (c *content) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	bs, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}

	*c = bs
	return nil
}

func newSession( /*poolDir string*/ ) *session {
	// pooledFileCh = make(chan *os.File)
	return &session{
		openFiles:   make([]*os.File, filePoolSize),
		finalizeMux: &sync.Mutex{},
		// pooledFiles: make([]*os.File, filePoolSize),
	}
}

func (s *session) addWriteTask(input []byte) error {
	start := time.Now()
	defer func() {
		log.Debugf("addWriteTask took %s", time.Since(start))
	}()

	var task writeTask
	if err := json.Unmarshal(input, &task); err != nil {
		return err
	}

	if task.SourcePath != nil {
		return s.copyFile(*task.SourcePath, task.Destination)
	}

	if task.Content != nil {
		return s.createFile(task.Content, task.Destination)
	}

	return fmt.Errorf("specify either src or content_b64")
}

func (s *session) copyFile(srcPath, destPath string) error {
	createDest := func() (*os.File, error) {
		start := time.Now()
		defer func() {
			log.Debugf("createDest took %s", time.Since(start))
		}()

		// dest, err := os.Create(destPath)
		dest, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return nil, err
		}
		return dest, nil
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}

	dest, err := createDest()
	if err != nil {
		return err
	}

	buf := make([]byte, copyBufferSize)

	readFromSrc := func() (int, error) {
		start := time.Now()
		defer func() {
			log.Debugf("readFromSrc took %s", time.Since(start))
		}()

		n, err := src.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		return n, nil
	}

	writeToDest := func(n int) error {
		start := time.Now()
		defer func() {
			log.Debugf("writeToDest took %s", time.Since(start))
		}()

		if _, err := dest.Write(buf[:n]); err != nil {
			return err
		}
		return nil
	}

	for {
		n, err := readFromSrc()
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		if err := writeToDest(n); err != nil {
			return err
		}
	}
	s.openFiles = append(s.openFiles, src, dest)

	return nil
}

func (s *session) createFile(content []byte, destPath string) error {
	dest, err := os.Create(destPath)
	if err != nil {
		return err
	}

	if _, err := dest.Write(content); err != nil {
		return err
	}

	s.openFiles = append(s.openFiles, dest)

	return nil
}

func (s *session) finalize() {
	s.finalizeMux.Lock()
	defer s.finalizeMux.Unlock()

	start := time.Now()
	defer func() {
		log.Debugf("finalize took %s", time.Since(start))
	}()

	wg := &sync.WaitGroup{}

	for _, f := range s.openFiles {
		wg.Add(1)
		go func(f *os.File) {
			f.Close()
			wg.Done()
		}(f)
	}
	s.openFiles = []*os.File{}

	wg.Wait()
}
