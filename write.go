package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"sync"
)

type content []byte

type writeTask struct {
	Destination string  `json:"dest"`
	SourcePath  *string `json:"src"`
	Content     content `json:"content_b64"` // Never use Content for a large file.
}

type session struct {
	wg *sync.WaitGroup
}

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
		wg: &sync.WaitGroup{},
	}
}

func (s *session) addWriteTask(input []byte) error {
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
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}

	dest, err := os.Create(destPath)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dest, src); err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		if err := src.Close(); err != nil {
			// Ignore
		}

		if err := dest.Close(); err != nil {
			// Ignore
		}

		s.wg.Done()
	}()

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

	s.wg.Add(1)
	go func() {
		if err := dest.Close(); err != nil {
			// Ignore
		}

		s.wg.Done()
	}()

	return nil
}

func (s *session) finalize() {
	s.wg.Wait()
}
