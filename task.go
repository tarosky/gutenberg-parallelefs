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

type task struct {
	Destination     string  `json:"dest"`
	SourcePath      *string `json:"src"`
	Content         content `json:"content_b64"` // Never use Content for a large file.
	Precreate       bool    `json:"precreate"`
	Existence       bool    `json:"existence"`
	Mkdir           bool    `json:"mkdir"`
	ListDir         bool    `json:"listdir"`
	Delete          bool    `json:"delete"`
	DeleteRecursive bool    `json:"delete_recursive"`
}

type precreatedFile struct {
	name   string
	parent *dirTree
	file   *futureFile
	done   <-chan *futureFile
}

type futureFile struct {
	file  *os.File
	isNew bool
	err   error
}

const (
	valFalse   = "false"
	valTrue    = "true"
	valInvalid = "null"
)

func (f *precreatedFile) getFutureFile() *futureFile {
	if f.file == nil {
		f.file = <-f.done
	}

	return f.file
}

func (f *precreatedFile) disposeUnused() error {
	fut := f.getFutureFile()
	if fut.err != nil {
		log.Error(fut.err)
		return nil
	}

	if fut.isNew {
		if err := os.Remove(fut.file.Name()); err != nil {
			return err
		}
	}

	return fut.file.Close()
}

type dirTree struct {
	childDirs  map[string]*dirTree
	childFiles map[string]*precreatedFile
	name       string
	parent     *dirTree
	precreated bool
	pathCache  *string
}

func newDirTree(name string, parent *dirTree, precreated bool) *dirTree {
	return &dirTree{
		childDirs:  map[string]*dirTree{},
		childFiles: map[string]*precreatedFile{},
		name:       name,
		parent:     parent,
		precreated: precreated,
	}
}

func createDirTree(parent *dirTree, name string, precreate bool) (*dirTree, error) {
	path := parent.getPath() + name
	stat, err := os.Stat(path)
	if err != nil {
		if err := os.Mkdir(path, 0755); err != nil {
			return nil, err
		}
		return newDirTree(name, parent, precreate), nil
	}

	if stat.IsDir() {
		return newDirTree(name, parent, false), nil
	}

	return nil, fmt.Errorf(
		"cannot create directory: file already exists: %s", path)
}

func (t *dirTree) precreateFile(name string) *precreatedFile {
	path := t.getPath() + name
	done := make(chan *futureFile)

	t.childFiles[name] = &precreatedFile{
		done:   done,
		parent: t,
	}

	go func() {
		defer close(done)

		if file, err := os.OpenFile(path, os.O_WRONLY, 0666); err == nil {
			done <- &futureFile{
				file:  file,
				isNew: false,
			}
			return
		}

		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			done <- &futureFile{
				err: err,
			}
			return
		}

		done <- &futureFile{
			file:  file,
			isNew: true,
		}
	}()

	return t.childFiles[name]
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

func (t *dirTree) registerDirInternal(dirParts []string, precreated bool) (*dirTree, error) {
	if len(dirParts) == 0 {
		log.Fatalf("dirParts must contain at least one element")
	}

	if len(dirParts) == 1 {
		dir, ok := t.childDirs[dirParts[0]]
		if !ok {
			t.childDirs[dirParts[0]] = newDirTree(dirParts[0], t, precreated)
			return t.childDirs[dirParts[0]], nil
		}
		return nil, fmt.Errorf("directory already registered: %s", dir.getPath())
	}

	if _, ok := t.childDirs[dirParts[0]]; !ok {
		t.childDirs[dirParts[0]] = newDirTree(dirParts[0], t, false)
	}

	return t.childDirs[dirParts[0]].registerDirInternal(dirParts[1:], precreated)
}

func (t *dirTree) addFileInternal(pathParts []string) (*precreatedFile, error) {
	if len(pathParts) < 1 {
		log.Fatalf("pathParts must contain at least one element")
	}

	if len(pathParts) == 1 {
		file, ok := t.childFiles[pathParts[0]]
		if !ok {
			return t.precreateFile(pathParts[0]), nil
		}
		return file, nil
	}

	dir, ok := t.childDirs[pathParts[0]]
	if !ok {
		var err error
		dir, err = createDirTree(t, pathParts[0], true)
		if err != nil {
			return nil, err
		}

		t.childDirs[pathParts[0]] = dir
	}

	return dir.addFileInternal(pathParts[1:])
}

func (t *dirTree) mkDirInternal(dirParts []string) error {
	if len(dirParts) == 0 {
		log.Fatalf("dirParts must contain at least one element")
	}

	if t.precreated {
		return fmt.Errorf("parent directory doesn't exist")
	}

	dir, ok := t.childDirs[dirParts[0]]
	if !ok {
		path := t.getPath() + filepath.Join(strings.Join(dirParts, "/"))
		return os.Mkdir(path, 0755)
	}

	if len(dirParts) == 1 {
		if dir.precreated {
			dir.precreated = false
			return nil
		}
		return fmt.Errorf("directory already exists")
	}

	return dir.mkDirInternal(dirParts[1:])
}

func (t *dirTree) clean() error {
	eg := &errgroup.Group{}

	for _, f := range t.childFiles {
		f := f
		eg.Go(f.disposeUnused)
	}

	for _, d := range t.childDirs {
		d := d
		eg.Go(d.clean)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	t.childFiles = map[string]*precreatedFile{}
	t.childDirs = map[string]*dirTree{}

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

func (t *dirTree) done() {
	for _, f := range t.childFiles {
		f.getFutureFile()
	}

	for _, d := range t.childDirs {
		d.done()
	}
}

func (t *dirTree) findDirInternal(dirParts []string) *dirTree {
	if len(dirParts) == 0 {
		log.Fatalf("dirParts must contain at least one element")
	}

	dir, ok := t.childDirs[dirParts[0]]
	if !ok {
		return nil
	}

	if len(dirParts) == 1 {
		return dir
	}

	return dir.findDirInternal(dirParts[1:])
}

func (t *dirTree) usePrecreatedFile(pathParts []string) *futureFile {
	if len(pathParts) < 1 {
		log.Fatalf("pathParts must contain at least one element")
	}

	if len(pathParts) == 1 {
		file, ok := t.childFiles[pathParts[0]]
		if !ok {
			return nil
		}
		fut := file.getFutureFile()
		delete(t.childFiles, pathParts[0])
		t.precreated = false
		return fut
	}

	dir, ok := t.childDirs[pathParts[0]]
	if !ok {
		return nil
	}

	fut := dir.usePrecreatedFile(pathParts[1:])
	t.precreated = false
	return fut
}

type session struct {
	openFiles         []*os.File
	finalizeMux       *sync.Mutex
	finalized         bool
	precreatedDirTree *dirTree
}

const copyBufferSize = 64 * 1024

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
		finalizeMux:       &sync.Mutex{},
		finalized:         false,
		precreatedDirTree: newDirTree("", nil, false),
	}
}

func (s *session) addTask(input []byte) (string, error) {
	start := time.Now()
	defer func() {
		log.Debugf("addTask took %s", time.Since(start))
	}()

	var task task
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

	if task.ListDir {
		files, err := s.listDir(destPath)
		if err != nil {
			return "[]", err
		}

		j, err := json.Marshal(files)
		if err != nil {
			return "[]", err
		}

		return string(j), nil
	}

	if task.Delete {
		succeeded, err := s.deleteSingle(destPath)
		var res string
		if succeeded {
			res = valTrue
		} else {
			res = valFalse
		}

		return res, err
	}

	if task.DeleteRecursive {
		succeeded, err := s.deleteRecursive(destPath)
		var res string
		if succeeded {
			res = valTrue
		} else {
			res = valFalse
		}

		return res, err
	}

	return valInvalid, fmt.Errorf("need more parameters")
}

func (s *session) deleteRecursive(path string) (bool, error) {
	start := time.Now()
	defer func() {
		log.Debugf("deleteRecursive took %s", time.Since(start))
	}()

	return s.delete(path, true)
}

func (s *session) deleteSingle(path string) (bool, error) {
	start := time.Now()
	defer func() {
		log.Debugf("deleteSingle took %s", time.Since(start))
	}()

	return s.delete(path, false)
}

func (s *session) delete(path string, recursive bool) (bool, error) {
	if f := s.findPrecreatedFile(path); f != nil {
		return !f.isNew, nil
	}

	if t := s.findPrecreatedDir(path); t != nil {
		if t.precreated {
			return false, nil
		}

		names, err := s.listDir(path)
		if err != nil {
			return false, err
		}

		if len(names) == 0 {
			t.precreated = true
			return true, nil
		}

		if recursive {
			eg := &errgroup.Group{}
			for _, n := range names {
				path2 := path + "/" + n
				eg.Go(func() error {
					succeeded, err := s.delete(path2, true)
					if err != nil {
						return err
					}

					if !succeeded {
						return fmt.Errorf("failed to delete: %s", path2)
					}

					return nil
				})
			}

			if err := eg.Wait(); err != nil {
				return false, err
			}

			t.precreated = true
			return true, nil
		}

		return false, nil
	}

	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	if !fi.IsDir() {
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}

			return false, err
		}

		return true, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	names, err := f.Readdirnames(-1)
	if err != nil {
		return false, err
	}

	precreatedFileExists := false
	unmanagedFileExists := false
	for _, n := range names {
		if f := s.findPrecreatedFile(path + "/" + n); f != nil {
			if f.isNew {
				precreatedFileExists = true
			}
		} else {
			unmanagedFileExists = true
		}
	}

	if recursive {
		eg := &errgroup.Group{}
		for _, n := range names {
			path2 := path + "/" + n
			eg.Go(func() error {
				succeeded, err := s.delete(path2, true)
				if err != nil {
					return err
				}

				if !succeeded {
					return fmt.Errorf("failed to delete: %s", path2)
				}

				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			return false, err
		}

		if precreatedFileExists {
			if _, err := s.registerPrecreatedDir(path, true); err != nil {
				return false, err
			}

			return true, nil
		}

		if err := os.Remove(path); err != nil {
			return false, err
		}

		return true, nil
	}

	if precreatedFileExists {
		if unmanagedFileExists {
			return false, nil
		}

		if _, err := s.registerPrecreatedDir(path, true); err != nil {
			return false, err
		}

		return true, nil
	}

	if err := os.Remove(path); err != nil {
		return false, err
	}

	return true, nil
}

func (s *session) listDir(dirPath string) ([]string, error) {
	start := time.Now()
	defer func() {
		log.Debugf("listDir took %s", time.Since(start))
	}()

	f, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	resNames := make([]string, 0, len(names))
	for _, n := range names {
		path := dirPath + "/" + n
		if t := s.findPrecreatedDir(path); t != nil {
			if t.precreated {
				continue
			}
			resNames = append(resNames, n)
			continue
		}

		if f := s.findPrecreatedFile(path); f != nil {
			if f.isNew {
				continue
			}
			resNames = append(resNames, n)
			continue
		}

		resNames = append(resNames, n)
	}

	return resNames, nil
}

// mkdir returns true only if the directory is newly created.
func (s *session) mkdir(destPath string) error {
	start := time.Now()
	defer func() {
		log.Debugf("mkdir took %s", time.Since(start))
	}()

	return s.mkPrecreatedDir(destPath)
}

func (s *session) existence(destPath string) bool {
	start := time.Now()
	defer func() {
		log.Debugf("existence took %s", time.Since(start))
	}()

	if f := s.findPrecreatedFile(destPath); f != nil {
		return !f.isNew
	}

	if t := s.findPrecreatedDir(destPath); t != nil {
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

	if _, err := s.addPrecreatedFile(destPath); err != nil {
		return err
	}

	return nil
}

func (s *session) copyFile(srcPath, destPath string) (string, error) {
	createDest := func() (*os.File, error) {
		start := time.Now()
		defer func() {
			log.Debugf("createDest took %s", time.Since(start))
		}()

		if f := s.usePrecreatedFile(destPath); f != nil {
			log.Debugf("precreated file found at: %s", destPath)

			if f.err != nil {
				return nil, f.err
			}

			return f.file, nil
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

	destStat, err := dest.Stat()
	if err != nil {
		return valFalse, err
	}

	destOldBytes := destStat.Size()

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

		if destOldBytes <= writtenBytes {
			log.Debugf(
				"truncation omitted: old: %d bytes, new: %d bytes",
				destOldBytes,
				writtenBytes)
			return
		}

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

	if err := s.precreatedDirTree.clean(); err != nil {
		log.Error(err)
	}
}

func (s *session) done() {
	s.precreatedDirTree.done()
}

func (s *session) mkPrecreatedDir(absDirPath string) error {
	if absDirPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absDirPath)
	}

	// Root directory
	if len(absDirPath) == 1 {
		return fmt.Errorf(
			"cannot mkdir directory: already exists: %s",
			absDirPath)
	}

	return s.precreatedDirTree.mkDirInternal(strings.Split(absDirPath[1:], "/"))
}

func (s *session) registerPrecreatedDir(absDirPath string, precreated bool) (*dirTree, error) {
	if absDirPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absDirPath)
	}

	// Root directory
	if len(absDirPath) == 1 {
		return s.precreatedDirTree, nil
	}

	return s.precreatedDirTree.registerDirInternal(strings.Split(absDirPath[1:], "/"), precreated)
}

func (s *session) findPrecreatedDir(absDirPath string) *dirTree {
	if absDirPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absDirPath)
	}

	// Root directory
	if len(absDirPath) == 1 {
		return s.precreatedDirTree
	}

	return s.precreatedDirTree.findDirInternal(strings.Split(absDirPath[1:], "/"))
}

func (s *session) addPrecreatedFile(absPath string) (*precreatedFile, error) {
	if absPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absPath)
	}

	// Root directory
	if len(absPath) == 1 {
		return nil, fmt.Errorf("directory already exists: %s", absPath)
	}

	return s.precreatedDirTree.addFileInternal(strings.Split(absPath[1:], "/"))
}

func (s *session) findPrecreatedFile(absPath string) *futureFile {
	name := filepath.Base(absPath)
	if name == "/" {
		return nil
	}

	dir := s.findPrecreatedDir(filepath.Dir(absPath))
	if dir == nil {
		return nil
	}

	file, ok := dir.childFiles[name]
	if !ok {
		return nil
	}

	return file.getFutureFile()
}

func (s *session) usePrecreatedFile(absPath string) *futureFile {
	if absPath[0] != '/' {
		log.Fatalf("path must be absolute: %s", absPath)
	}

	// Root directory
	if len(absPath) == 1 {
		return nil
	}

	return s.precreatedDirTree.usePrecreatedFile(strings.Split(absPath[1:], "/"))
}
