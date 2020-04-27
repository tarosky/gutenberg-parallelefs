package main

import (
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli/v2"
)

type unit struct{}

func main() {
	// log.SetLevel(log.DebugLevel)
	log.SetLevel(log.TraceLevel)
	log.SetOutput(os.Stderr)

	app := &cli.App{
		Name:  "parallelefs",
		Usage: "This program writes files in parallel to speed up EFS.",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:     "socket",
				Aliases:  []string{"s"},
				Required: true,
				Usage:    "path to the socket file to be created",
			},
		},
		Action: func(c *cli.Context) error {
			socket, err := filepath.Abs(c.Path("socket"))
			if err != nil {
				return err
			}

			listen(socket)
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func listen(socket string) {
	if err := os.Remove(socket); err != nil {
		// Ignore error
	}

	wg := &sync.WaitGroup{}

	listener, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Debugf("started listening")

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			wg.Add(1)
			go handleConnection(conn, wg)
		}
	}()

	sigCh := interruptionNotification()

	// Wait until interrupted
	<-sigCh
	log.Debugf("quitting")

	// Wait until connections are finalized
	wg.Wait()
}

func interruptionNotification() <-chan os.Signal {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, os.Kill)
	return sigCh
}

func handleConnection(conn net.Conn, wg *sync.WaitGroup) {
	sess := newSession()

	// "defer" doesn't work with interruption.
	sigCh := interruptionNotification()
	go func() {
		<-sigCh
		sess.finalize()
		conn.Close()
		wg.Done()
	}()

	// Abandon this goroutine on termination
	// since conn.Read() blocks everything.
	log.Debugf("started new session")

	readCh := connReader(conn)
	for {
		msg, ok := <-readCh
		if !ok {
			return
		}

		log.Debugf("received: %d bytes", len(msg))
		log.Debugf("content: '%s'", string(msg))

		if len(msg) == 0 {
			sess.finalize()
			conn.Write([]byte("true\n"))
			return
		}

		res, err := sess.addWriteTask(msg)
		if err != nil {
			log.Error(err)
		}

		conn.Write([]byte(res + "\n"))
	}
}
