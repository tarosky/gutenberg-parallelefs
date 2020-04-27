package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type content []byte

type writeTask struct {
	Destination string  `json:"dest"`
	SourcePath  *string `json:"src"`
	Content     content `json:"content_b64"` // Never use Content for a large file.
	Precreate   bool    `json:"precreate"`
	Existence   bool    `json:"existence"`
	Mkdir       bool    `json:"mkdir"`
}

type precreatedFile struct {
	file  *os.File
	isNew bool
	err   error
	done  <-chan unit
}

const (
	valFalse   = "false"
	valTrue    = "true"
	valInvalid = "invalid"
)

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

type dirTree struct {
	children   map[string]*dirTree
	name       string
	parent     *dirTree
	precreated bool
	pathCache  *string
}

func newDirTree(name string, parent *dirTree, precreated bool) *dirTree {
	return &dirTree{
		children:   map[string]*dirTree{},
		name:       name,
		parent:     parent,
		precreated: precreated,
	}
}

func (t *dirTree) createChild(name string, precreate bool) (*dirTree, error) {
	path := t.getPath() + name
	stat, err := os.Stat(path)
	if err != nil {
		if err := os.Mkdir(path, 0755); err != nil {
			return nil, err
		}
		t.children[name] = newDirTree(name, t, precreate)
		return t.children[name], nil
	}

	if stat.IsDir() {
		t.children[name] = newDirTree(name, t, false)
		return t.children[name], nil
	}

	return nil, fmt.Errorf(
		"cannot create directory: file already exists: %s", path)
}

// getPath returns the dir path with a trailing slash.
func (t *dirTree) getPath() string {
	if t.pathCache == nil {
		var path string
		if t.parent == nil {
			path = "/"
		} else {
			path = t.parent.getPath() + t.name + "/"
		}
		t.pathCache = &path
	}

	return *t.pathCache
}

func (t *dirTree) mkDir(absDirPath string) error {
	if absDirPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absDirPath)
	}

	// Root directory
	if len(absDirPath) == 1 {
		return fmt.Errorf(
			"cannot mkdir directory: already exists: %s",
			absDirPath)
	}

	return t.mkDirInternal(strings.Split(absDirPath[1:], "/"))
}

func (t *dirTree) mkDirInternal(dirParts []string) error {
	if len(dirParts) == 0 {
		log.Fatalf("dirParts must contain at least one element")
	}

	if t.precreated {
		return fmt.Errorf("parent directory doesn't exist")
	}

	child, ok := t.children[dirParts[0]]
	if !ok {
		path := t.getPath() + filepath.Join(strings.Join(dirParts, "/"))
		return os.Mkdir(path, 0755)
	}

	if len(dirParts) == 1 {
		if child.precreated {
			child.precreated = false
			return nil
		}
		return fmt.Errorf("directory already exists")
	}

	return child.mkDirInternal(dirParts[1:])
}

func (t *dirTree) ensureDir(absDirPath string, precreate bool) (*dirTree, error) {
	if absDirPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absDirPath)
	}

	// Root directory
	if len(absDirPath) == 1 {
		return t, nil
	}

	return t.ensureDirInternal(strings.Split(absDirPath[1:], "/"), precreate)
}

func (t *dirTree) ensureDirInternal(dirParts []string, precreate bool) (*dirTree, error) {
	if len(dirParts) < 1 {
		log.Fatalf("dirParts must contain at least one element")
	}

	child, ok := t.children[dirParts[0]]
	if !ok {
		var err error
		child, err = t.createChild(dirParts[0], precreate)
		if err != nil {
			return nil, err
		}
	} else {
		child.precreated = child.precreated && precreate
	}

	if len(dirParts) < 2 {
		return child, nil
	}

	return child.ensureDirInternal(dirParts[1:], precreate)
}

func (t *dirTree) clean() error {
	eg := &errgroup.Group{}

	for _, c := range t.children {
		c := c
		eg.Go(c.clean)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	if !t.precreated {
		return nil
	}

	path := t.getPath()

	dir, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer dir.Close()

	if _, err := dir.Readdirnames(1); err != nil {
		if err == io.EOF {
			return os.Remove(path)
		}
		return err
	}

	return nil
}

func (t *dirTree) find(absDirPath string) *dirTree {
	if absDirPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absDirPath)
	}

	// Root directory
	if len(absDirPath) == 1 {
		return t
	}

	return t.findInternal(strings.Split(absDirPath[1:], "/"))
}

func (t *dirTree) findInternal(dirParts []string) *dirTree {
	if len(dirParts) == 0 {
		log.Fatalf("dirParts must contain at least one element")
	}

	child, ok := t.children[dirParts[0]]
	if !ok {
		return nil
	}

	if len(dirParts) == 1 {
		return child
	}

	return child.findInternal(dirParts[1:])
}

type session struct {
	openFiles         []*os.File
	precreatedFileMap map[string]*precreatedFile
	finalizeMux       *sync.Mutex
	finalized         bool
	precreatedDirTree *dirTree
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
		precreatedDirTree: newDirTree("", nil, false),
	}
}

func (s *session) addWriteTask(input []byte) (string, error) {
	start := time.Now()
	defer func() {
		log.Debugf("addWriteTask took %s", time.Since(start))
	}()

	var task writeTask
	if err := json.Unmarshal(input, &task); err != nil {
		return valInvalid, err
	}

	normalizePath := func(path string) (string, error) {
		start := time.Now()
		defer func() {
			log.Debugf("normalizePath took %s", time.Since(start))
		}()

		// There's an assumption that no symbolic link exists.
		return filepath.Abs(path)
	}

	precreateDirs := func(path string) error {
		start := time.Now()
		defer func() {
			log.Debugf("precreateDirs took %s", time.Since(start))
		}()

		_, err := s.precreatedDirTree.ensureDir(filepath.Dir(path), true)
		return err
	}

	destPath, err := normalizePath(task.Destination)
	if err != nil {
		return valInvalid, err
	}

	if task.SourcePath != nil {
		return s.copyFile(*task.SourcePath, destPath)
	}

	if task.Content != nil {
		return s.createFile(task.Content, destPath)
	}

	if task.Precreate {
		if err := precreateDirs(destPath); err != nil {
			return valTrue, err
		}

		if err := s.precreateFile(destPath); err != nil {
			return valTrue, err
		}

		return valTrue, nil
	}

	if task.Existence {
		if s.existence(destPath) {
			return valTrue, nil
		}
		return valFalse, nil
	}

	if task.Mkdir {
		if err := s.mkdir(destPath); err != nil {
			return valFalse, err
		}
		return valTrue, err
	}

	return valInvalid, fmt.Errorf("specify any of src, content_b64, or precreate")
}

// mkdir returns true only if the directory is newly created.
func (s *session) mkdir(destPath string) error {
	start := time.Now()
	defer func() {
		log.Debugf("mkdir took %s", time.Since(start))
	}()

	return s.precreatedDirTree.mkDir(destPath)
}

func (s *session) existence(destPath string) bool {
	start := time.Now()
	defer func() {
		log.Debugf("existence took %s", time.Since(start))
	}()

	if f, ok := s.precreatedFileMap[destPath]; ok {
		<-f.done
		return !f.isNew
	}

	if t := s.precreatedDirTree.find(destPath); t != nil {
		return !t.precreated
	}

	_, err := os.Stat(destPath)
	return !os.IsNotExist(err)
}

func (s *session) precreateFile(destPath string) error {
	start := time.Now()
	defer func() {
		log.Debugf("precreateFile took %s", time.Since(start))
	}()

	if _, ok := s.precreatedFileMap[destPath]; ok {
		return nil
	}

	s.precreatedFileMap[destPath] = precreateFile(destPath)

	return nil
}

func (s *session) copyFile(srcPath, destPath string) (string, error) {
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
		return valFalse, err
	}
	s.openFiles = append(s.openFiles, src)

	dest, err := createDest()
	if err != nil {
		return valFalse, err
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
			return valFalse, err
		}
		if n == 0 {
			break
		}
		if err := writeToDest(n); err != nil {
			return valFalse, err
		}
	}

	return valTrue, nil
}

func (s *session) createFile(content []byte, destPath string) (string, error) {
	dest, err := os.Create(destPath)
	if err != nil {
		return valFalse, err
	}

	if _, err := dest.Write(content); err != nil {
		return valFalse, err
	}

	s.openFiles = append(s.openFiles, dest)

	return valTrue, nil
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

	if err := s.precreatedDirTree.clean(); err != nil {
		log.Error(err)
	}
}
