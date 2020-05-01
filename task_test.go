package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type testFS struct {
	baseDir string
}

const (
	testFile1    = "test.txt"
	testFile2    = "test2.txt"
	testDirFile1 = "subdir/test.txt"

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

type testpack struct {
	t      *testing.T
	assert *assert.Assertions
	sess   *session
	fs     *testFS
}

func run(test func(*testpack)) func(*testing.T) {
	return func(t *testing.T) {
		sess := newSession()
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

func TestCreateFile(t *testing.T) {
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

func TestCopyFile(t *testing.T) {
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
			p.fs.path(testDirFile1),
			p.fs.path(testFile2)))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)
	}))
}
