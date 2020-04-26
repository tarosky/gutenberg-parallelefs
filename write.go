package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type content []byte

type writeTask struct {
	Destination string  `json:"dest"`
	SourcePath  *string `json:"src"`
	Content     content `json:"content_b64"` // Never use Content for a large file.
	Precreate   bool    `json:"precreate"`
}

type precreatedFile struct {
	file  *os.File
	isNew bool
	err   error
	done  <-chan unit
}

func precreateFile(path string) *precreatedFile {
	done := make(chan unit)

	pf := &precreatedFile{
		done: done,
	}

	go func() {
		dir := filepath.Dir(path)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				pf.err = err
				done <- unit{}
				close(done)
				return
			}
		}

		if file, err := os.OpenFile(path, os.O_WRONLY, 0666); err == nil {
			pf.file = file
			pf.isNew = false
			done <- unit{}
			close(done)
			return
		}

		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			pf.err = err
			done <- unit{}
			close(done)
			return
		}

		pf.file = file
		pf.isNew = true
		done <- unit{}
		close(done)
	}()

	return pf
}

func (f *precreatedFile) disposeUnused() error {
	<-f.done

	if f.isNew {
		if err := os.Remove(f.file.Name()); err != nil {
			return err
		}
	}

	return f.file.Close()
}

type session struct {
	openFiles         []*os.File
	precreatedFileMap map[string]*precreatedFile
	finalizeMux       *sync.Mutex
	finalized         bool
}

const copyBufferSize = 10 * 1024

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

func newSession() *session {
	return &session{
		openFiles:         make([]*os.File, 0),
		precreatedFileMap: map[string]*precreatedFile{},
		finalizeMux:       &sync.Mutex{},
		finalized:         false,
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

	if task.Precreate {
		s.precreateFile(task.Destination)
		return nil
	}

	return fmt.Errorf("specify any of src, content_b64, or precreate")
}

func (s *session) precreateFile(destPath string) {
	start := time.Now()
	defer func() {
		log.Debugf("precreateFile took %s", time.Since(start))
	}()

	if _, ok := s.precreatedFileMap[destPath]; !ok {
		s.precreatedFileMap[destPath] = precreateFile(destPath)
	}
}

func (s *session) copyFile(srcPath, destPath string) error {
	createDest := func() (*os.File, error) {
		start := time.Now()
		defer func() {
			log.Debugf("createDest took %s", time.Since(start))
		}()

		if pf, ok := s.precreatedFileMap[destPath]; ok {
			log.Debugf("precreated file found at: %s", destPath)

			delete(s.precreatedFileMap, destPath)

			<-pf.done
			if err := pf.err; err != nil {
				return nil, err
			}

			return pf.file, nil
		}

		log.Debug("precreated file not found")
		return os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE, 0666)
	}

	openSrc := func() (*os.File, error) {
		start := time.Now()
		defer func() {
			log.Debugf("openSrc took %s", time.Since(start))
		}()

		return os.Open(srcPath)
	}

	src, err := openSrc()
	if err != nil {
		return err
	}
	s.openFiles = append(s.openFiles, src)

	dest, err := createDest()
	if err != nil {
		return err
	}
	s.openFiles = append(s.openFiles, dest)

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

	var writtenBytes int64
	defer func() {
		start := time.Now()
		defer func() {
			log.Debugf("truncate(defer) took %s", time.Since(start))
		}()

		dest.Truncate(writtenBytes)
	}()

	writeToDest := func(n int) error {
		start := time.Now()
		defer func() {
			log.Debugf("writeToDest took %s", time.Since(start))
		}()

		wb, err := dest.Write(buf[:n])
		if err != nil {
			return err
		}

		writtenBytes += int64(wb)
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

	wg := &sync.WaitGroup{}

	for _, f := range s.openFiles {
		wg.Add(1)
		go func(f *os.File) {
			f.Close()
			wg.Done()
		}(f)
	}

	for _, pf := range s.precreatedFileMap {
		wg.Add(1)
		go func(pf *precreatedFile) {
			pf.disposeUnused()
			wg.Done()
		}(pf)
	}

	s.openFiles = []*os.File{}

	wg.Wait()
}
