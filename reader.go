package main

import (
	"bufio"
	"net"

	log "github.com/sirupsen/logrus"
)

func connReader(conn net.Conn) <-chan []byte {
	readCh := make(chan []byte)
	recv := bufio.NewScanner(conn)

	go func() {
		for {
			if ok := recv.Scan(); !ok {
				// not EOF
				if recv.Err() != nil {
					log.Error(recv.Err())
				}

				close(readCh)
				return
			}

			readCh <- recv.Bytes()
		}
	}()

	return readCh
}
