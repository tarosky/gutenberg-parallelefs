package main

import (
	"bytes"
	"testing"
)

func Test_Reader(t *testing.T) {
	t.Run("typical", run(func(p *testpack) {
		reader := bytes.NewReader([]byte(testContent1 + "\n" + testContent2))
		readChan := connReader(reader)

		line, ok := <-readChan

		p.assert.True(ok)
		p.assert.Equal([]byte(testContent1), line)

		line, ok = <-readChan

		p.assert.True(ok)
		p.assert.Equal([]byte(testContent2), line)

		line, ok = <-readChan
		p.assert.False(ok)
		p.assert.Nil(line)
	}))

	t.Run("trailing newline", run(func(p *testpack) {
		reader := bytes.NewReader([]byte(testContent1 + "\n" + testContent2 + "\n"))
		readChan := connReader(reader)

		line, ok := <-readChan

		p.assert.True(ok)
		p.assert.Equal([]byte(testContent1), line)

		line, ok = <-readChan

		p.assert.True(ok)
		p.assert.Equal([]byte(testContent2), line)

		line, ok = <-readChan
		p.assert.False(ok)
		p.assert.Nil(line)
	}))

	t.Run("long input", run(func(p *testpack) {
		reader := bytes.NewReader([]byte(testLongContent1))
		readChan := connReader(reader)

		line, ok := <-readChan

		p.assert.True(ok)
		p.assert.Equal([]byte(testLongContent1), line)

		line, ok = <-readChan
		p.assert.False(ok)
		p.assert.Nil(line)
	}))
}
