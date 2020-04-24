package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type content []byte

type writeTask struct {
	Destination string  `json:"dest"`
	SourcePath  *string `json:"src"`
	Content     content `json:"content_b64"` // Never use Content for a large file.
}

type session struct {
	openFiles           []*os.File
	finalizeMux         *sync.Mutex
	pooledFilesCh       <-chan *os.File
	finalizePooledFiles chan<- unit
	finalized           bool
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

func newSession(poolDir string) *session {
	pooledFilesCh := make(chan *os.File, filePoolSize)
	finalizePooledFiles := make(chan unit)

	go func() {
		for {
			file, err := os.OpenFile(
				poolDir+"/"+uuid.New().String()+".pool",
				os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
				0666,
			)
			if err != nil {
				log.Error(err)
				close(pooledFilesCh)
				return
			}

			select {
			case pooledFilesCh <- file:
				continue
			case _, _ = <-finalizePooledFiles:
				file.Close()
				os.Remove(file.Name())
				close(pooledFilesCh)
			}
		}
	}()

	return &session{
		openFiles:           make([]*os.File, filePoolSize),
		finalizeMux:         &sync.Mutex{},
		pooledFilesCh:       pooledFilesCh,
		finalizePooledFiles: finalizePooledFiles,
		finalized:           false,
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
	createDest := func() *os.File {
		start := time.Now()
		defer func() {
			log.Debugf("createDest took %s", time.Since(start))
		}()

		// dest, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		file := <-s.pooledFilesCh
		return file
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}

	dest := createDest()

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

	if s.finalized {
		return
	}
	defer func() {
		s.finalized = true
	}()

	s.finalizePooledFiles <- unit{}
	close(s.finalizePooledFiles)

	wg := &sync.WaitGroup{}

	for _, f := range s.openFiles {
		wg.Add(1)
		go func(f *os.File) {
			f.Close()
			wg.Done()
		}(f)
	}

	for f := range s.pooledFilesCh {
		wg.Add(1)
		go func(f *os.File) {
			f.Close()
			wg.Done()
		}(f)
	}

	s.openFiles = []*os.File{}

	wg.Wait()
}
