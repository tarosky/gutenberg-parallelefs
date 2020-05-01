package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type testFS struct {
	baseDir string
}

const (
	testRootDir  = "."
	testDir1     = "subdir"
	testDir1Dir1 = "subdir/subdir"

	testFile1         = "test.txt"
	testFile2         = "test2.txt"
	testDir1File1     = "subdir/test.txt"
	testDir1File2     = "subdir/test2.txt"
	testDir1Dir1File1 = "subdir/subdir/test.txt"
	testDir1Dir1File2 = "subdir/subdir/test2.txt"

	testContent1 = "test-string"

	testResTrue  = "true"
	testResFalse = "false"
)

func createTestFS() *testFS {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	return &testFS{
		baseDir: wd + "/work/test",
	}
}

func (f *testFS) path(path string) string {
	return f.baseDir + "/" + path
}

func (f *testFS) file(path string) *testFile {
	return newTestFile(f.path(path))
}

func (f *testFS) dir(path string) *testDirectory {
	return newTestDirectory(f.path(path))
}

func b64String(content string) string {
	return base64.StdEncoding.EncodeToString([]byte(testContent1))
}

func taskf(format string, a ...interface{}) []byte {
	return []byte(fmt.Sprintf(format, a...))
}

type testFile struct {
	path string
}

func newTestFile(path string) *testFile {
	return &testFile{
		path: path,
	}
}

func (f *testFile) read() string {
	bs, err := ioutil.ReadFile(f.path)
	if err != nil {
		log.Fatal(err)
	}

	return string(bs)
}

func (f *testFile) write(content string) {
	if err := ioutil.WriteFile(f.path, []byte(content), 0644); err != nil {
		log.Fatal(err)
	}
}

type testDirectory struct {
	path string
}

func newTestDirectory(path string) *testDirectory {
	return &testDirectory{
		path: path,
	}
}

func (d *testDirectory) ls() []string {
	fis, err := ioutil.ReadDir(d.path)
	if err != nil {
		log.Fatal(err)
	}

	names := make([]string, 0, len(fis))
	for _, fi := range fis {
		names = append(names, fi.Name())
	}

	sort.Strings(names)
	return names
}

func (d *testDirectory) create() {
	if err := os.Mkdir(d.path, 0755); err != nil {
		log.Fatal(err)
	}
}

func (d *testDirectory) exists() bool {
	_, err := os.Stat(d.path)
	return !os.IsNotExist(err)
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
		os.RemoveAll(fs.baseDir)
		os.Mkdir(fs.baseDir, 0755)

		test(&testpack{
			t:      t,
			assert: as,
			sess:   sess,
			fs:     fs,
		})
	}
}

func Test_CopyFile(t *testing.T) {
	t.Run("normal", run(func(p *testpack) {
		p.fs.file(testFile2).write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testFile1),
			p.fs.path(testFile2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal(testContent1, p.fs.file(testFile1).read())
	}))

	t.Run("parent dir doesn't exist", run(func(p *testpack) {
		p.fs.file(testFile2).write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File1),
			p.fs.path(testFile2)))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)
	}))
}

func Test_CopyFile_Precreate(t *testing.T) {
	t.Run("normal, fulfilled by copy", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testFile2)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testFile2),
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal(testContent1, p.fs.file(testFile2).read())
	}))

	t.Run("deep file, fulfilled by copy", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File1),
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal(testContent1, p.fs.file(testDir1File1).read())
	}))

	t.Run("two deep files, first one discarded, fulfilled by copy", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File2)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File2),
			p.fs.path(testFile2)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files, second one discarded, fulfilled by copy", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File2)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File1),
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files with different levels, shallower one discarded, fulfilled by content", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1Dir1File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1Dir1File1),
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1Dir1).ls())
		p.assert.Equal([]string{testDir1}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files with different levels, deeper one discarded, fulfilled by content", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1Dir1File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File2),
			p.fs.path(testFile2)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	}))
}

func Test_CreateFile(t *testing.T) {
	t.Run("normal", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testFile1),
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal(testContent1, p.fs.file(testFile1).read())
	}))

	t.Run("parent dir doesn't exist", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path("foo/test.txt"),
			b64String(testContent1)))

		p.assert.Equal(testResFalse, res)
		p.assert.Error(err)
	}))
}

func Test_CreateFile_Precreate(t *testing.T) {
	t.Run("normal, fulfilled by content", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testFile1),
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal(testContent1, p.fs.file(testFile1).read())
	}))

	t.Run("deep file, fulfilled by content", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testDir1File1),
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal(testContent1, p.fs.file(testDir1File1).read())
	}))

	// t.Run("two deep files, first one discarded, fulfilled by content", run(func(p *testpack) {
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1File1)))
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1File2)))

	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "content_b64": "%s"}`,
	// 		p.fs.path(testDir1File2),
	// 		b64String(testContent1)))

	// 	p.sess.finalize()

	// 	p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	// }))

	// t.Run("two deep files, second one discarded, fulfilled by content", run(func(p *testpack) {
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1File1)))
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1File2)))

	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "content_b64": "%s"}`,
	// 		p.fs.path(testDir1File1),
	// 		b64String(testContent1)))

	// 	p.sess.finalize()

	// 	p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1).ls())
	// }))

	// t.Run("two deep files with different levels, shallower one discarded, fulfilled by content", run(func(p *testpack) {
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1File2)))
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1Dir1File1)))

	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "content_b64": "%s"}`,
	// 		p.fs.path(testDir1Dir1File1),
	// 		b64String(testContent1)))

	// 	p.sess.finalize()

	// 	p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1Dir1).ls())
	// 	p.assert.Equal([]string{testDir1}, p.fs.dir(testDir1).ls())
	// }))

	// t.Run("two deep files with different levels, deeper one discarded, fulfilled by content", run(func(p *testpack) {
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1File2)))
	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "precreate": true}`,
	// 		p.fs.path(testDir1Dir1File1)))

	// 	p.sess.addTask(taskf(
	// 		`{"dest": "%s", "content_b64": "%s"}`,
	// 		p.fs.path(testDir1File2),
	// 		b64String(testContent1)))

	// 	p.sess.finalize()

	// 	p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	// }))
}

func Test_Existence(t *testing.T) {
	t.Run("existent", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))

	t.Run("inexistent", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("directory treated as existent", run(func(p *testpack) {
		p.fs.dir(testDir1).create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))
}

func Test_Existence_Precreate(t *testing.T) {
	t.Run("precreated new file treated as inexistent", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreated": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("precreated existing file treated as existent", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreated": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))

	t.Run("precreated directory treated as inexistent", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreated": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))
}

func Test_Precreate(t *testing.T) {
	t.Run("normal", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))

	t.Run("deep file", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal([]string{testDir1}, p.fs.dir(testRootDir).ls())
	}))

	t.Run("discarded new file", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{}, p.fs.dir(testRootDir).ls())
	}))

	t.Run("discarded existing file", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testRootDir).ls())
		p.assert.Equal(testContent1, p.fs.file(testFile1).read())
	}))

	t.Run("two deep files with different levels, both discarded", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1Dir1File1)))

		p.sess.finalize()

		p.assert.Equal([]string{}, p.fs.dir(testRootDir).ls())
	}))
}

func Test_Mkdir(t *testing.T) {
	t.Run("normal", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.True(p.fs.dir(testDir1).exists())
	}))

	t.Run("already exists", run(func(p *testpack) {
		p.fs.dir(testDir1).create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)
	}))
}

func Test_Mkdir_Precreate(t *testing.T) {
	t.Run("mkdir already precreated directory", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.True(p.fs.dir(testDir1).exists())
	}))

	t.Run("mkdir already precreated directory twice fails", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)
		p.assert.True(p.fs.dir(testDir1).exists())
	}))

	t.Run("already precreated directory persists after mkdir", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testDir1File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.sess.finalize()

		p.assert.True(p.fs.dir(testDir1).exists())
	}))

	t.Run("same name as precreated file", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			// The path looks like a file but is actually a directory.
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.True(p.fs.dir(testFile1).exists())
	}))

	t.Run("directory of the same name as precreated file persists", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "precreate": true}`,
			p.fs.path(testFile1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			// The path looks like a file but is actually a directory.
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.True(p.fs.dir(testFile1).exists())
	}))
}