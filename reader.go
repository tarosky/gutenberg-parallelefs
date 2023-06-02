package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"

	log "github.com/sirupsen/logrus"
)

func connReader(conn io.Reader) <-chan []byte {
	recvLine := make(chan []byte)
	recv := bufio.NewReader(conn)

	// Abandon this goroutine on termination
	// since conn.Read() blocks everything.
	go func() {
		defer close(recvLine)

		for {
			temp, isPrefix, err := recv.ReadLine()
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					log.Error(err)
				}
				return
			}

			buf := bytes.NewBuffer([]byte{})
			buf.Write(temp)

			if isPrefix {
				for {
					b, cont, err := recv.ReadLine()
					if err != nil {
						if err != io.EOF {
							log.Fatal(err)
						}
						break
					}

					if _, err := buf.Write(b); err != nil {
						log.Fatal(err)
					}

					if !cont {
						break
					}
				}
			}

			recvLine <- buf.Bytes()
		}
	}()

	return recvLine
}
