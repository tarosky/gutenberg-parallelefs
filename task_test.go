package main

import (
	"encoding/base64"
	"encoding/json"
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
	testDir2     = "anotherdir"
	testDir1Dir2 = "subdir/anotherdir"

	testFile1         = "test.txt"
	testFile2         = "test2.txt"
	testDir1File1     = "subdir/test.txt"
	testDir1File2     = "subdir/test2.txt"
	testDir1Dir2File1 = "subdir/anotherdir/test.txt"
	testDir1Dir2File2 = "subdir/anotherdir/test2.txt"

	testContent1     = "test-string"
	testContent2     = "another-text"
	testLongContent1 = "long-long-test-string"

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
	return base64.StdEncoding.EncodeToString([]byte(content))
}

func jsonSortedSlice(content string) []string {
	data := []string{}
	if err := json.Unmarshal([]byte(content), &data); err != nil {
		log.Fatal(err)
	}

	sort.Strings(data)

	return data
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

func (f *testFile) exists() bool {
	_, err := os.Stat(f.path)
	return !os.IsNotExist(err)
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
	t.Run("typical", run(func(p *testpack) {
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

func Test_CopyFile_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile2)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testFile2),
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()
		p.assert.Equal(testContent1, p.fs.file(testFile2).read())
	}))

	t.Run("overwrite", run(func(p *testpack) {
		// Check if truncate works.
		p.fs.file(testFile1).write(testLongContent1)
		p.fs.file(testFile2).write(testContent2)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile2)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testFile1),
			p.fs.path(testFile2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()
		p.assert.Equal(testContent2, p.fs.file(testFile1).read())
	}))

	t.Run("deep file", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File1),
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, p.fs.file(testDir1File1).read())
	}))

	t.Run("two deep files, first one discarded", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File2),
			p.fs.path(testFile2)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files, second one discarded", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File1),
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files with different levels, shallower one discarded", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1Dir2File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1Dir2File1),
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1Dir2).ls())
		p.assert.Equal([]string{testDir2}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files with different levels, deeper one discarded", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1Dir2File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "src": "%s"}`,
			p.fs.path(testDir1File2),
			p.fs.path(testFile2)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	}))
}

func Test_CreateFile(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testFile1),
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
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

func Test_CreateFile_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testFile1),
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, p.fs.file(testFile1).read())

		p.sess.finalize()
		p.assert.Equal(testContent1, p.fs.file(testFile1).read())
	}))

	t.Run("overwrite", run(func(p *testpack) {
		// Check if truncate works.
		p.fs.file(testFile1).write(testLongContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testFile1),
			b64String(testContent2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()
		p.assert.Equal(testContent2, p.fs.file(testFile1).read())
	}))

	t.Run("deep file", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testDir1File1),
			b64String(testContent1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.Equal(testContent1, p.fs.file(testDir1File1).read())
	}))

	t.Run("two deep files, first one discarded", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testDir1File2),
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files, second one discarded", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testDir1File1),
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files with different levels, shallower one discarded", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1Dir2File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testDir1Dir2File1),
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testDir1Dir2).ls())
		p.assert.Equal([]string{testDir2}, p.fs.dir(testDir1).ls())
	}))

	t.Run("two deep files with different levels, deeper one discarded", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1Dir2File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testDir1File2),
			b64String(testContent1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile2}, p.fs.dir(testDir1).ls())
	}))
}

func Test_CreateFile_Delete_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "content_b64": "%s"}`,
			p.fs.path(testFile1),
			b64String(testContent2)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.finalize()
		p.assert.True(p.fs.file(testFile1).exists())
		p.assert.Equal(testContent2, p.fs.file(testFile1).read())
	}))
}

func Test_Delete(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(p.fs.file(testFile1).exists())
	}))

	t.Run("empty directory", run(func(p *testpack) {
		p.fs.dir(testDir1).create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(p.fs.file(testDir1).exists())
	}))

	t.Run("non-empty directory", run(func(p *testpack) {
		p.fs.dir(testDir1).create()
		p.fs.file(testDir1File1).write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testDir1)))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.file(testDir1).exists())
	}))
}

func Test_Delete_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.file(testFile1).exists())
	}))

	t.Run("existing", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(p.fs.file(testFile1).exists())

		p.sess.finalize()
		p.assert.False(p.fs.file(testFile1).exists())
	}))

	t.Run("speculative directory", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.file(testDir1).exists())
	}))

	t.Run("non-empty directory", run(func(p *testpack) {
		p.fs.dir(testDir1).create()
		p.fs.file(testDir1File1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete": true}`,
			p.fs.path(testDir1)))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.file(testDir1).exists())
	}))
}

func Test_DeleteRecursive(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.fs.dir(testDir1).create()
		p.fs.file(testDir1File1).write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(p.fs.file(testDir1File1).exists())
	}))

	t.Run("file", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(p.fs.file(testFile1).exists())
	}))

	t.Run("empty directory", run(func(p *testpack) {
		p.fs.dir(testDir1).create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.assert.False(p.fs.dir(testDir1).exists())
	}))

	t.Run("inexistent", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))
}

func Test_DeleteRecursive_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.file(testDir1File1).exists())
	}))

	t.Run("speculative file", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1File1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.file(testDir1File1).exists())
	}))

	t.Run("speculative file included", run(func(p *testpack) {
		p.fs.dir(testDir1).create()

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		{
			res, err := p.sess.addTask(taskf(
				`{"dest": "%s", "delete_recursive": true}`,
				p.fs.path(testDir1)))

			p.assert.NoError(err)
			p.assert.Equal(testResTrue, res)

			p.sess.done()
			p.assert.True(p.fs.file(testDir1File1).exists())
		}
		{
			res, err := p.sess.addTask(taskf(
				`{"dest": "%s", "mkdir": true}`,
				p.fs.path(testDir1)))

			p.assert.NoError(err)
			p.assert.Equal(testResTrue, res)
		}
	}))

	t.Run("mixed", run(func(p *testpack) {
		p.fs.dir(testDir1).create()
		p.fs.file(testDir1File1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(p.fs.file(testDir1File1).exists())
		p.assert.True(p.fs.file(testDir1File2).exists())
	}))

	t.Run("deep", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1Dir2File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.file(testDir1Dir2File1).exists())
	}))

	t.Run("deep mixed", run(func(p *testpack) {
		p.fs.dir(testDir1).create()
		p.fs.dir(testDir1Dir2).create()
		p.fs.file(testDir1Dir2File1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1Dir2File2)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.False(p.fs.file(testDir1Dir2File1).exists())
		p.assert.True(p.fs.file(testDir1Dir2File2).exists())
	}))

	t.Run("existing file", run(func(p *testpack) {
		p.fs.dir(testDir1).create()
		p.fs.file(testDir1File1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "delete_recursive": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(p.fs.file(testDir1File1).exists())

		p.sess.finalize()
		p.assert.False(p.fs.dir(testDir1).exists())
	}))
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

func Test_Existence_Speculate(t *testing.T) {
	t.Run("speculative new file treated as inexistent", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculative": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))

	t.Run("speculative existing file treated as existent", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
	}))

	t.Run("speculative directory treated as inexistent", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "existence": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResFalse, res)
	}))
}

func Test_ListDir(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		p.fs.dir(testDir1).create()

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			p.fs.path(testRootDir)))

		p.assert.NoError(err)
		p.assert.Equal([]string{testDir1}, jsonSortedSlice(res))
	}))

	t.Run("empty", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			p.fs.path(testRootDir)))

		p.assert.NoError(err)
		p.assert.Equal([]string{}, jsonSortedSlice(res))
	}))
}

func Test_ListDir_Speculate(t *testing.T) {
	t.Run("speculative new file is omitted", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile2)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			p.fs.path(testRootDir)))

		p.assert.NoError(err)
		p.assert.Equal([]string{testFile1}, jsonSortedSlice(res))
	}))

	t.Run("speculative existing file isn't omitted", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)
		p.fs.file(testFile2).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			p.fs.path(testRootDir)))

		p.assert.NoError(err)
		p.assert.Equal([]string{testFile1, testFile2}, jsonSortedSlice(res))
	}))

	t.Run("speculative directory is omitted", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "listdir": true}`,
			p.fs.path(testRootDir)))

		p.assert.NoError(err)
		p.assert.Equal([]string{testFile1}, jsonSortedSlice(res))
	}))
}

func Test_Mkdir(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
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

func Test_Mkdir_Speculate(t *testing.T) {
	t.Run("mkdir already speculative directory", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(p.fs.dir(testDir1).exists())
	}))

	t.Run("mkdir already speculative directory twice fails", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.assert.Error(err)
		p.assert.Equal(testResFalse, res)

		p.sess.done()
		p.assert.True(p.fs.dir(testDir1).exists())
	}))

	t.Run("already speculative directory persists after mkdir", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			p.fs.path(testDir1)))

		p.sess.finalize()

		p.assert.True(p.fs.dir(testDir1).exists())
	}))

	t.Run("same name as speculative file", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			// The path looks like a file but is actually a directory.
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(p.fs.dir(testFile1).exists())
	}))

	t.Run("directory of the same name as speculative file persists", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		p.sess.addTask(taskf(
			`{"dest": "%s", "mkdir": true}`,
			// The path looks like a file but is actually a directory.
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.True(p.fs.dir(testFile1).exists())
	}))
}

func Test_Speculate(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)

		p.sess.done()
		p.assert.True(p.fs.file(testFile1).exists())
	}))

	t.Run("deep file", run(func(p *testpack) {
		res, err := p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File1)))

		p.assert.NoError(err)
		p.assert.Equal(testResTrue, res)
		p.assert.Equal([]string{testDir1}, p.fs.dir(testRootDir).ls())

		p.sess.done()
		p.assert.True(p.fs.file(testDir1File1).exists())
	}))

	t.Run("discarded new file", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{}, p.fs.dir(testRootDir).ls())
	}))

	t.Run("discarded existing file", run(func(p *testpack) {
		p.fs.file(testFile1).write(testContent1)

		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testFile1)))

		p.sess.finalize()

		p.assert.Equal([]string{testFile1}, p.fs.dir(testRootDir).ls())
		p.assert.Equal(testContent1, p.fs.file(testFile1).read())
	}))

	t.Run("two deep files with different levels, both discarded", run(func(p *testpack) {
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1File2)))
		p.sess.addTask(taskf(
			`{"dest": "%s", "speculate": true}`,
			p.fs.path(testDir1Dir2File1)))

		p.sess.finalize()

		p.assert.Equal([]string{}, p.fs.dir(testRootDir).ls())
	}))
}
