package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type testFS struct {
	baseDir    string
	symlinkDir string
}

const (
	testRootDir  = "."
	testDir1     = "subdir"
	testDir2     = "anotherdir"
	testDir1Dir2 = "subdir/anotherdir"

	testFile1         = "test.txt"
	testFile2         = "test2.txt"
	testTargetFile1   = "target.txt"
	testTargetFile2   = "target2.txt"
	testTargetDir1    = "target"
	testDir1File1     = "subdir/test.txt"
	testDir1File2     = "subdir/test2.txt"
	testDir1Dir2File1 = "subdir/anotherdir/test.txt"
	testDir1Dir2File2 = "subdir/anotherdir/test2.txt"

	testContent1 = "test-string"
	testContent2 = "another-text"

	testDirPerm1  = os.FileMode(0707)
	testFilePerm1 = os.FileMode(0606)
	testFilePerm2 = os.FileMode(0660)

	testResTrue  = "true"
	testResFalse = "false"
)

// 1MiB string
var testLongContent1 = strings.Repeat("long-test-string", 16*1024)

func createTestFS() *testFS {
	wd, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}

	return &testFS{
		baseDir:    wd + "/work/test/base",
		symlinkDir: wd + "/work/test/symlink",
	}
}

func (f *testFS) path(path string) string {
	return f.baseDir + "/" + path
}

func (f *testFS) file(path string) *testFile {
	return newTestFile(path, f.path(path), nil)
}

func (f *testFS) fileWithSymlink(path string, target string) *testFile {
	symlinkTarget := f.symlinkDir + "/" + target
	return newTestFile(path, f.path(path), &symlinkTarget)
}

func (f *testFS) dir(path string) *testDirectory {
	return newTestDirectory(path, f.path(path), nil)
}

func (f *testFS) dirWithSymlink(path string, target string) *testDirectory {
	symlinkTarget := f.symlinkDir + "/" + target
	return newTestDirectory(path, f.path(path), &symlinkTarget)
}

func b64String(content string) string {
	return base64.StdEncoding.EncodeToString([]byte(content))
}

func jsonSortedSlice(content string) []string {
	data := []string{}
	if err := json.Unmarshal([]byte(content), &data); err != nil {
		log.Panic(err)
	}

	sort.Strings(data)

	return data
}

func taskf(format string, a ...any) []byte {
	return fmt.Appendf([]byte{}, format, a...)
}

type testFile struct {
	relPath       string
	path          string
	symlinkTarget *string
}

func newTestFile(relPath, path string, symlinkTarget *string) *testFile {
	if symlinkTarget != nil {
		target, err := filepath.Abs(*symlinkTarget)
		if err != nil {
			log.Panic(err)
		}
		symlinkTarget = &target
	}

	return &testFile{
		relPath:       relPath,
		path:          path,
		symlinkTarget: symlinkTarget,
	}
}

func (f *testFile) read() string {
	bs, err := os.ReadFile(f.path)
	if err != nil {
		log.Panic(err)
	}

	return string(bs)
}

func (f *testFile) isSymlink() bool {
	fi, err := os.Lstat(f.path)
	if err != nil {
		log.Panic(err)
	}

	return fi.Mode()&fs.ModeSymlink != 0
}

func (f *testFile) write(content string) *testFile {
	if f.symlinkTarget != nil {
		if err := os.Symlink(*f.symlinkTarget, f.path); err != nil {
			log.Panic(err)
		}
	}
	if err := os.WriteFile(f.path, []byte(content), 0644); err != nil {
		log.Panic(err)
	}
	return f
}

func (f *testFile) symlink() *testFile {
	if f.symlinkTarget != nil {
		if err := os.Symlink(*f.symlinkTarget, f.path); err != nil {
			log.Panic(err)
		}
	}
	return f
}

func (f *testFile) chmod(mode os.FileMode) *testFile {
	if err := os.Chmod(f.path, mode); err != nil {
		log.Panic(err)
	}
	return f
}

func (f *testFile) mode() os.FileMode {
	s, err := os.Stat(f.path)
	if err != nil {
		log.Panic(err)
	}
	return s.Mode().Perm()
}

func (f *testFile) symlinkedMode() os.FileMode {
	s, err := os.Stat(*f.symlinkTarget)
	if err != nil {
		log.Panic(err)
	}
	return s.Mode().Perm()
}

func (f *testFile) exists() bool {
	st, err := os.Stat(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Panic(err)
	}
	if st.IsDir() {
		log.Panicf("the path is not file: %s", f.path)
	}
	return true
}

func (f *testFile) symlinkExists() bool {
	st, err := os.Lstat(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Panic(err)
	}
	return st.Mode()&fs.ModeSymlink != 0
}

type testDirectory struct {
	relPath       string
	path          string
	symlinkTarget *string
}

func newTestDirectory(relPath, path string, symlinkTarget *string) *testDirectory {
	if symlinkTarget != nil {
		target, err := filepath.Abs(*symlinkTarget)
		if err != nil {
			log.Panic(err)
		}
		symlinkTarget = &target
	}

	return &testDirectory{
		relPath:       relPath,
		path:          path,
		symlinkTarget: symlinkTarget,
	}
}

func (d *testDirectory) ls() []string {
	fis, err := os.ReadDir(d.path)
	if err != nil {
		log.Panic(err)
	}

	names := make([]string, 0, len(fis))
	for _, fi := range fis {
		names = append(names, fi.Name())
	}

	sort.Strings(names)
	return names
}

func (d *testDirectory) create() *testDirectory {
	if d.symlinkTarget != nil {
		if err := os.Symlink(*d.symlinkTarget, d.path); err != nil {
			log.Panic(err)
		}
		if err := os.Mkdir(*d.symlinkTarget, 0755); err != nil {
			log.Panic(err)
		}
	} else {
		if err := os.Mkdir(d.path, 0755); err != nil {
			log.Panic(err)
		}
	}
	return d
}

func (d *testDirectory) symlink() *testDirectory {
	if d.symlinkTarget != nil {
		if err := os.Symlink(*d.symlinkTarget, d.path); err != nil {
			log.Panic(err)
		}
	}
	return d
}

func (d *testDirectory) exists() bool {
	st, err := os.Stat(d.path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Panic(err)
	}
	if !st.IsDir() {
		log.Panicf("the path is not directory: %s", d.path)
	}
	return true
}

func (d *testDirectory) symlinkExists() bool {
	st, err := os.Lstat(d.path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Panic(err)
	}
	return st.Mode()&fs.ModeSymlink != 0
}

func (d *testDirectory) mode() os.FileMode {
	s, err := os.Stat(d.path)
	if err != nil {
		log.Panic(err)
	}
	return s.Mode().Perm()
}

type testpack struct {
	t      *testing.T
	assert *assert.Assertions
	sess   *session
	fs     *testFS
}

func TestMain(m *testing.M) {
	// log.SetLevel(log.DebugLevel)
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stderr)

	os.Exit(m.Run())
}

func run(test func(*testpack)) func(*testing.T) {
	return func(t *testing.T) {
		sess := newSession()
		defer sess.finalize()
		fs := createTestFS()
		as := assert.New(t)
		if err := os.RemoveAll(fs.baseDir); err != nil {
			log.Panic(err)
		}
		if err := os.RemoveAll(fs.symlinkDir); err != nil {
			log.Panic(err)
		}
		if err := os.MkdirAll(fs.baseDir, 0755); err != nil {
			log.Panic(err)
		}
		if err := os.MkdirAll(fs.symlinkDir, 0755); err != nil {
			log.Panic(err)
		}

		test(&testpack{
			t:      t,
			assert: as,
			sess:   sess,
			fs:     fs,
		})
	}
}

func Test_CopyFile(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)

		testFile2.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile1.path,
			testFile2.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, testFile1.read())
		p.assert.False(testFile1.isSymlink())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)
		testFile2 := p.fs.file(testFile2)

		testFile1.symlink()
		testFile2.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile1.path,
			testFile2.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, testFile1.read())
		p.assert.True(testFile1.isSymlink())
	}))

	t.Run("overwrite", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)

		testFile1.write(testContent1)
		testFile2.write(testContent2)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile1.path,
			testFile2.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent2, testFile1.read())
		p.assert.False(testFile1.isSymlink())
	}))

	t.Run("overwrite - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)
		testFile2 := p.fs.file(testFile2)

		testFile1.write(testContent1)
		testFile2.write(testContent2)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile1.path,
			testFile2.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent2, testFile1.read())
		p.assert.True(testFile1.isSymlink())
	}))

	t.Run("parent dir doesn't exist", run(func(p *testpack) {
		testDir1File1 := p.fs.file(testDir1File1)
		testFile2 := p.fs.file(testFile2)

		testFile2.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testDir1File1.path,
			testFile2.path))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("chmod", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)

		testFile2.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s", "perm": %d}`,
			testFile1.path,
			testFile2.path,
			testFilePerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testFilePerm1, testFile1.mode())
	}))

	t.Run("chmod - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)
		testFile2 := p.fs.file(testFile2)

		testFile1.symlink()
		testFile2.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s", "perm": %d}`,
			testFile1.path,
			testFile2.path,
			testFilePerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testFilePerm1, testFile1.mode())
		p.assert.Equal(testFilePerm1, testFile1.symlinkedMode())
	}))

	t.Run("overwrite chmod", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)

		testFile1.write(testContent1)
		testFile2.write(testContent2)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s", "perm": %d}`,
			testFile1.path,
			testFile2.path,
			testFilePerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testFilePerm1, testFile1.mode())
	}))
}

func Test_CopyFile_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile2.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile2.path,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()

		p.assert.Equal(testContent1, testFile2.read())
		p.assert.False(testFile2.isSymlink())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.fileWithSymlink(testFile2, testTargetFile2)

		testFile1.write(testContent1)
		testFile2.symlink()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile2.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile2.path,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()

		p.assert.Equal(testContent1, testFile2.read())
		p.assert.True(testFile2.isSymlink())
	}))

	t.Run("overwrite", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)

		// Check if truncate works.
		testFile1.write(testLongContent1)
		testFile2.write(testContent2)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile1.path,
			testFile2.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()

		p.assert.Equal(testContent2, testFile1.read())
		p.assert.False(testFile1.isSymlink())
	}))

	t.Run("overwrite - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)
		testFile2 := p.fs.file(testFile2)

		// Check if truncate works.
		testFile1.write(testLongContent1)
		testFile2.write(testContent2)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testFile1.path,
			testFile2.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()

		p.assert.Equal(testContent2, testFile1.read())
		p.assert.True(testFile1.isSymlink())
	}))

	t.Run("deep file", run(func(p *testpack) {
		testDir1File1 := p.fs.file(testDir1File1)
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testDir1File1.path,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, testDir1File1.read())
	}))

	t.Run("discard - symlink dir - no target dir", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.symlink()
		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testDir1File1.exists())
		p.assert.True(testDir1.exists())

		p.sess.finalize()
		p.assert.False(testDir1File1.exists())
		p.assert.False(testDir1.exists())
	}))

	t.Run("discard - symlink dir - with target dir", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.create()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		p.sess.done()
		p.assert.True(testDir1File1.exists())
		p.assert.True(testDir1.exists())

		p.sess.finalize()
		p.assert.False(testDir1File1.exists())
		p.assert.True(testDir1.exists())
	}))

	t.Run("discard - symlink file", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.symlink()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testFile1.exists())

		p.sess.finalize()
		p.assert.False(testFile1.exists())
	}))

	t.Run("two deep files, first one discarded", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)
		testDir1File2 := p.fs.file(testDir1File2)

		testFile1.write(testContent1)
		testFile2.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testDir1File2.path,
			testFile2.path))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2.relPath}, testDir1.ls())
	}))

	t.Run("two deep files, second one discarded", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)
		testDir1File1 := p.fs.file(testDir1File1)
		testDir1File2 := p.fs.file(testDir1File2)
		testDir1 := p.fs.dir(testDir1)

		testFile1.write(testContent1)
		testFile2.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testDir1File1.path,
			testFile1.path))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1.relPath}, testDir1.ls())
	}))

	t.Run("two deep files with different levels, shallower one discarded", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)
		testDir1Dir2File1 := p.fs.file(testDir1Dir2File1)
		testDir1Dir2 := p.fs.dir(testDir1Dir2)
		testDir1File2 := p.fs.file(testDir1File2)
		testDir1 := p.fs.dir(testDir1)
		testDir2 := p.fs.dir(testDir2)

		testFile1.write(testContent1)
		testFile2.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1Dir2File1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testDir1Dir2File1.path,
			testFile1.path))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1.relPath}, testDir1Dir2.ls())
		p.assert.Equal([]string{testDir2.relPath}, testDir1.ls())
	}))

	t.Run("two deep files with different levels, deeper one discarded", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)
		testDir1Dir2File1 := p.fs.file(testDir1Dir2File1)
		testDir1File2 := p.fs.file(testDir1File2)
		testDir1 := p.fs.dir(testDir1)

		testFile1.write(testContent1)
		testFile2.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1Dir2File1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			testDir1File2.path,
			testFile2.path))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2.relPath}, testDir1.ls())
	}))
}

func Test_CreateFile(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testContent1, testFile1.read())

		p.sess.finalize()
		p.assert.Equal(testContent1, testFile1.read())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.symlink()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testContent1, testFile1.read())

		p.sess.finalize()
		p.assert.Equal(testContent1, testFile1.read())
	}))

	t.Run("long input", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testLongContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testLongContent1, testFile1.read())
	}))

	t.Run("overwrite", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testContent2, testFile1.read())
	}))

	t.Run("overwrite - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testContent2, testFile1.read())
	}))

	t.Run("parent dir doesn't exist", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path("foo/test.txt"),
			b64String(testContent1)))

		p.assert.Equal(testResFalse, res)
		p.assert.Error(err)
	}))

	t.Run("chmod", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s", "perm": %d}`,
			testFile1.path,
			b64String(testContent1),
			testFilePerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testFilePerm1, testFile1.mode())
	}))

	t.Run("overwrite chmod", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s", "perm": %d}`,
			testFile1.path,
			b64String(testContent2),
			testFilePerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testFilePerm1, testFile1.mode())
	}))
}

func Test_CreateFile_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, testFile1.read())

		p.sess.finalize()
		p.assert.Equal(testContent1, testFile1.read())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.symlink()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, testFile1.read())

		p.sess.finalize()
		p.assert.Equal(testContent1, testFile1.read())
	}))

	t.Run("overwrite", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		// Check if truncate works.
		testFile1.write(testLongContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()
		p.assert.Equal(testContent2, testFile1.read())
	}))

	t.Run("chmod", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true, "perm": %d}`,
			testFile1.path,
			testFilePerm1))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s", "perm": %d}`,
			testFile1.path,
			b64String(testContent1),
			testFilePerm2))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testFilePerm2, testFile1.mode())

		p.sess.finalize()
		p.assert.Equal(testFilePerm2, testFile1.mode())
	}))

	t.Run("chmod overwrite", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1).chmod(testFilePerm1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true, "perm": %d}`,
			testFile1.path,
			testFilePerm2))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s", "perm": %d}`,
			testFile1.path,
			b64String(testContent1),
			testFilePerm2))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testFilePerm2, testFile1.mode())

		p.sess.finalize()
		p.assert.Equal(testFilePerm2, testFile1.mode())
	}))

	t.Run("chmod overwrite, mode changed again", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1).chmod(testFilePerm1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true, "perm": %d}`,
			testFile1.path,
			testFilePerm2))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s", "perm": %d}`,
			testFile1.path,
			b64String(testContent1),
			testFilePerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testFilePerm1, testFile1.mode())

		p.sess.finalize()
		p.assert.Equal(testFilePerm1, testFile1.mode())
	}))

	t.Run("deep file", run(func(p *testpack) {
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testDir1File1.path,
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, testDir1File1.read())
	}))

	t.Run("two deep files, first one discarded", run(func(p *testpack) {
		testFile2 := p.fs.file(testFile2)
		testDir1File1 := p.fs.file(testDir1File1)
		testDir1File2 := p.fs.file(testDir1File2)
		testDir1 := p.fs.dir(testDir1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testDir1File2.path,
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2.relPath}, testDir1.ls())
	}))

	t.Run("two deep files, second one discarded", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testDir1File1 := p.fs.file(testDir1File1)
		testDir1File2 := p.fs.file(testDir1File2)
		testDir1 := p.fs.dir(testDir1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testDir1File1.path,
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1.relPath}, testDir1.ls())
	}))

	t.Run("two deep files with different levels, shallower one discarded", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testDir1 := p.fs.dir(testDir1)
		testDir2 := p.fs.dir(testDir2)
		testDir1Dir2 := p.fs.dir(testDir1Dir2)
		testDir1Dir2File1 := p.fs.file(testDir1Dir2File1)
		testDir1File2 := p.fs.file(testDir1File2)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1Dir2File1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testDir1Dir2File1.path,
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1.relPath}, testDir1Dir2.ls())
		p.assert.Equal([]string{testDir2.relPath}, testDir1.ls())
	}))

	t.Run("two deep files with different levels, deeper one discarded", run(func(p *testpack) {
		testFile2 := p.fs.file(testFile2)
		testDir1 := p.fs.dir(testDir1)
		testDir1Dir2File1 := p.fs.file(testDir1Dir2File1)
		testDir1File2 := p.fs.file(testDir1File2)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1Dir2File1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testDir1File2.path,
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2.relPath}, testDir1.ls())
	}))
}

func Test_CreateFile_Delete_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()
		p.assert.True(testFile1.exists())
		p.assert.Equal(testContent2, testFile1.read())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			testFile1.path,
			b64String(testContent2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()
		p.assert.True(testFile1.exists())
		p.assert.Equal(testContent2, testFile1.read())
	}))
}

func Test_Delete(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(testFile1.exists())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(testFile1.exists())
		p.assert.True(testFile1.symlinkExists())
	}))

	t.Run("empty directory", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(testDir1.exists())
	}))

	t.Run("empty directory - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(testDir1.exists())
		p.assert.True(testDir1.symlinkExists())
	}))

	t.Run("non-empty directory", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.create()
		testDir1File1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testDir1.path))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))
}

func Test_Delete_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testFile1.exists())

		p.sess.finalize()
		p.assert.False(testFile1.exists())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testFile1.exists())

		p.sess.finalize()
		p.assert.False(testFile1.exists())
	}))

	t.Run("existing", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testFile1.exists())

		p.sess.finalize()
		p.assert.False(testFile1.exists())
	}))

	t.Run("existing - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testFile1.exists())

		p.sess.finalize()
		p.assert.False(testFile1.exists())
	}))

	t.Run("speculative directory", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))

	t.Run("speculative directory - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.symlink()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))

	t.Run("non-empty directory", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)
		testDir1File2 := p.fs.file(testDir1File2)

		testDir1.create()
		testDir1File1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testDir1.path))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))

	t.Run("non-empty directory - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)
		testDir1File1 := p.fs.file(testDir1File1)
		testDir1File2 := p.fs.file(testDir1File2)

		testDir1.create()
		testDir1File1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			testDir1.path))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))
}

func Test_DeleteRecursive(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.create()
		testDir1File1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(testDir1File1.exists())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.create()
		testDir1File1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(testDir1File1.exists())
		p.assert.False(testDir1.exists())
		p.assert.True(testDir1.symlinkExists())
	}))

	t.Run("file", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(testFile1.exists())
	}))

	t.Run("file - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(testFile1.exists())
		p.assert.True(testFile1.symlinkExists())
	}))

	t.Run("empty directory", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(testDir1.exists())
	}))

	t.Run("empty directory - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(testDir1.exists())
		p.assert.True(testDir1.symlinkExists())
	}))

	t.Run("nonexistent", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("nonexistent - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.symlink()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.assert.False(testFile1.exists())
		p.assert.True(testFile1.symlinkExists())
	}))
}

func Test_DeleteRecursive_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1File1.exists())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.symlink()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1File1.exists())

		p.sess.finalize()
		p.assert.False(testDir1File1.exists())
		p.assert.False(testDir1.exists())
		p.assert.True(testDir1.symlinkExists())
	}))

	t.Run("speculative file", run(func(p *testpack) {
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1File1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1File1.exists())
	}))

	t.Run("speculative file - symlink", run(func(p *testpack) {
		testDir1 := p.fs.fileWithSymlink(testDir1, testTargetDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.symlink()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1File1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1File1.exists())

		p.sess.finalize()
		p.assert.False(testDir1File1.exists())
		p.assert.False(testDir1.exists())
		p.assert.True(testDir1.symlinkExists())
	}))

	t.Run("speculative file included", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.create()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		{
			res, err := p.sess.addTask(taskf(
				`{"dest": "%s", "delete_recursive": true}`,
				testDir1.path))

			p.assert.NoError(err)
			p.assert.Equal(testResTrue, res)

			p.sess.done()
			p.assert.True(testDir1File1.exists())
		}
		{
			res, err := p.sess.addTask(taskf(
				`{"dest": "%s", "mkdir": true}`,
				testDir1.path))

			p.assert.NoError(err)
			p.assert.Equal(testResTrue, res)
		}
	}))

	t.Run("mixed", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)
		testDir1File2 := p.fs.file(testDir1File2)

		testDir1.create()
		testDir1File1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(testDir1File1.exists())
		p.assert.True(testDir1File2.exists())
	}))

	t.Run("deep", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1Dir2File1 := p.fs.file(testDir1Dir2File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1Dir2File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1Dir2File1.exists())
	}))

	t.Run("deep mixed", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1Dir2 := p.fs.dir(testDir1Dir2)
		testDir1Dir2File1 := p.fs.file(testDir1Dir2File1)
		testDir1Dir2File2 := p.fs.file(testDir1Dir2File2)

		testDir1.create()
		testDir1Dir2.create()
		testDir1Dir2File1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1Dir2File2.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(testDir1Dir2File1.exists())
		p.assert.True(testDir1Dir2File2.exists())
	}))

	t.Run("existing file", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		testDir1.create()
		testDir1File1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testDir1File1.exists())

		p.sess.finalize()
		p.assert.False(testDir1.exists())
	}))
}

func Test_Existence(t *testing.T) {
	t.Run("existent", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))

	t.Run("existent - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))

	t.Run("nonexistent", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("nonexistent - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.symlink()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("directory treated as existent", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))
}

func Test_Existence_Speculate(t *testing.T) {
	t.Run("speculative new file treated as nonexistent", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculative": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("speculative existing file treated as existent", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))

	t.Run("speculative directory treated as nonexistent", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))
}

func Test_ListDir(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testRootDir := p.fs.dir(testRootDir)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			testRootDir.path))

		p.assert.NoError(err)
		p.assert.Equal([]string{testDir1.relPath}, jsonSortedSlice(res))
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)
		testRootDir := p.fs.dir(testRootDir)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			testRootDir.path))

		p.assert.NoError(err)
		p.assert.Equal([]string{testDir1.relPath}, jsonSortedSlice(res))
	}))

	t.Run("empty", run(func(p *testpack) {
		testRootDir := p.fs.dir(testRootDir)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			testRootDir.path))

		p.assert.NoError(err)
		p.assert.Equal([]string{}, jsonSortedSlice(res))
	}))
}

func Test_ListDir_Speculate(t *testing.T) {
	t.Run("speculative new file is omitted", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)
		testRootDir := p.fs.dir(testRootDir)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile2.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			testRootDir.path))

		p.assert.NoError(err)
		p.assert.Equal([]string{testFile1.relPath}, jsonSortedSlice(res))
	}))

	t.Run("speculative existing file isn't omitted", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testFile2 := p.fs.file(testFile2)
		testRootDir := p.fs.dir(testRootDir)

		testFile1.write(testContent1)
		testFile2.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			testRootDir.path))

		p.assert.NoError(err)
		p.assert.Equal([]string{testFile1.relPath, testFile2.relPath}, jsonSortedSlice(res))
	}))

	t.Run("speculative directory is omitted", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testDir1File1 := p.fs.file(testDir1File1)
		testRootDir := p.fs.dir(testRootDir)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			testRootDir.path))

		p.assert.NoError(err)
		p.assert.Equal([]string{testFile1.relPath}, jsonSortedSlice(res))
	}))
}

func Test_Mkdir(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)

		testDir1.symlink()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
		p.assert.True(testDir1.symlinkExists())
	}))

	t.Run("chmod", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true, "perm": %d}`,
			testDir1.path,
			testDirPerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testDirPerm1, testDir1.mode())
	}))

	t.Run("already exists", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("already exists - symlink", run(func(p *testpack) {
		testDir1 := p.fs.dirWithSymlink(testDir1, testTargetDir1)

		testDir1.create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)
	}))
}

func Test_Mkdir_Speculate(t *testing.T) {
	t.Run("mkdir already speculative directory", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))

	t.Run("mkdir already speculative directory twice fails", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(testDir1.exists())
	}))

	t.Run("already speculative directory persists after mkdir", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testDir1.path))

		p.sess.finalize()

		p.assert.True(testDir1.exists())
	}))

	t.Run("same name as speculative file", run(func(p *testpack) {
		// The path looks like a file but is actually a directory.
		testFile1 := p.fs.dir(testFile1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testFile1.exists())
	}))

	t.Run("directory of the same name as speculative file persists", run(func(p *testpack) {
		// The path looks like a file but is actually a directory.
		testFile1 := p.fs.dir(testFile1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			testFile1.path))

		p.sess.finalize()

		p.assert.True(testFile1.exists())
	}))
}

func Test_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testFile1.exists())
	}))

	t.Run("typical - symlink", run(func(p *testpack) {
		testFile1 := p.fs.fileWithSymlink(testFile1, testTargetFile1)

		testFile1.symlink()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(testFile1.exists())
	}))

	t.Run("chmod", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true, "perm": %d}`,
			testFile1.path,
			testFilePerm1))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.Equal(testFilePerm1, testFile1.mode())
	}))

	t.Run("deep file", run(func(p *testpack) {
		testDir1 := p.fs.dir(testDir1)
		testDir1File1 := p.fs.file(testDir1File1)
		testRootDir := p.fs.dir(testRootDir)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File1.path))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal([]string{testDir1.relPath}, testRootDir.ls())

		p.sess.done()
		p.assert.True(testDir1File1.exists())
	}))

	t.Run("discarded new file", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testRootDir := p.fs.dir(testRootDir)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.sess.finalize()

		p.assert.Equal([]string{}, testRootDir.ls())
	}))

	t.Run("discarded existing file", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)
		testRootDir := p.fs.dir(testRootDir)

		testFile1.write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testFile1.path))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1.relPath}, testRootDir.ls())
		p.assert.Equal(testContent1, testFile1.read())
	}))

	t.Run("never change perm when file exists", run(func(p *testpack) {
		testFile1 := p.fs.file(testFile1)

		testFile1.write(testContent1).chmod(testFilePerm1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true, "perm": %d}`,
			testFile1.path,
			testFilePerm2))

		p.sess.done()
		p.assert.Equal(testFilePerm1, testFile1.mode())

		p.sess.finalize()

		p.assert.Equal(testFilePerm1, testFile1.mode())
	}))

	t.Run("two deep files with different levels, both discarded", run(func(p *testpack) {
		testDir1File2 := p.fs.file(testDir1File2)
		testDir1Dir2File1 := p.fs.file(testDir1Dir2File1)
		testRootDir := p.fs.dir(testRootDir)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1File2.path))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			testDir1Dir2File1.path))

		p.sess.finalize()

		p.assert.Equal([]string{}, testRootDir.ls())
	}))
}
