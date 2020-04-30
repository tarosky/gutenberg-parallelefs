package main

import (
	"bufio"
	"io"

	log "github.com/sirupsen/logrus"
)

func connReader(conn io.Reader) <-chan []byte {
	recvLine := make(chan []byte)
	recv := bufio.NewScanner(conn)

	// Abandon this goroutine on termination
	// since conn.Read() blocks everything.
	go func() {
		defer close(recvLine)

		for {
			if ok := recv.Scan(); !ok {
				if recv.Err() != nil {
					// This will be called when the connection is closed.
					log.Debug(recv.Err())
				}

				return
			}

			recvLine <- recv.Bytes()
		}
	}()

	return recvLine
}
