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
const filePoolSize = 20
const createFilesWorkerCount = 10

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

func createFiles(
	poolDir string,
	pooledFilesCh chan<- *os.File,
	finalizePooledFiles <-chan unit,
) {
	createdFileCh := make(chan *os.File)
	quitCh := make(chan unit)
	propagateErrorCh := make(chan unit, createFilesWorkerCount)

	go func() {
		finalize := func(file *os.File) {
			file.Close()
			os.Remove(file.Name())
			log.Tracef("file disposed at main goroutine: %s", file.Name())
			close(quitCh)
			close(pooledFilesCh)
		}

		for {
			file := <-createdFileCh
			select {
			case pooledFilesCh <- file:
				log.Tracef("file consumed at main goroutine: %s", file.Name())
			case <-finalizePooledFiles:
				log.Debug(
					"createFiles main goroutine: finalizePooledFiles received")
				finalize(file)
				return
			case <-propagateErrorCh:
				log.Debug(
					"createFiles main goroutine: propagateErrorCh received")
				finalize(file)
				return
			}
		}
	}()

	for i := 0; i < createFilesWorkerCount; i++ {
		go func(workerID int) {
			for {
				file, err := os.OpenFile(
					poolDir+"/"+uuid.New().String()+".pool",
					os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
					0666,
				)
				log.Tracef(
					"file created: workerID: %d, path: %s",
					workerID,
					file.Name())

				if err != nil {
					log.Error(err)
					propagateErrorCh <- unit{}
					return
				}

				select {
				case createdFileCh <- file:
					log.Tracef(
						"file added to pool: workerID: %d, path: %s",
						workerID,
						file.Name())
					continue
				case <-quitCh:
					file.Close()
					os.Remove(file.Name())
					log.Tracef(
						"file disposed: workerID: %d, path: %s",
						workerID,
						file.Name())
					return
				}
			}
		}(i)
	}
}

func newSession(poolDir string) *session {
	pooledFilesCh := make(chan *os.File, filePoolSize)
	finalizePooledFiles := make(chan unit)

	createFiles(poolDir, pooledFilesCh, finalizePooledFiles)

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
		return <-s.pooledFilesCh
	}

	moveDest := func(file *os.File) error {
		start := time.Now()
		defer func() {
			log.Debugf("moveDest took %s", time.Since(start))
		}()

		return os.Rename(file.Name(), destPath)
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

	dest := createDest()

	if err := moveDest(dest); err != nil {
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
			os.Remove(f.Name())
			log.Tracef("file disposed at finalizer: %s", f.Name())
			wg.Done()
		}(f)
	}

	s.openFiles = []*os.File{}

	wg.Wait()
}
