package main

import (
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli/v2"
)

type unit struct{}

func main() {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02T15:04:05Z07:00.000000",
	})

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
			&cli.BoolFlag{
				Name:     "panic",
				Required: false,
				Usage:    "Intentionally panic at the end of program for debugging",
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
		After: func(c *cli.Context) error {
			// Check remaining goroutines for debugging
			if c.Bool("panic") {
				panic("end of program")
			}

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

	listener, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Debugf("started listening")

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// This will be called immediately after closing the listener.
				return
			}

			go func() {
				defer conn.Close()
				handleConnection(ctx, conn)
			}()
		}
	}()

	// Wait until interrupted
	<-interruptionNotification()
	log.Debugf("quitting")
	cancel()
}

func interruptionNotification() <-chan os.Signal {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, os.Kill)
	return sigCh
}

func handleConnection(ctx context.Context, conn io.ReadWriter) {
	sess := newSession()
	defer sess.finalize()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Debugf("started new session")

	recvLine := connReader(conn)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-recvLine:
			if !ok {
				cancel()
				continue
			}

			log.Debugf("received: %d bytes", len(msg))
			log.Debugf("content: '%s'", string(msg))

			// Empty request means the end of this session.
			if len(msg) == 0 {
				sess.finalize()
				conn.Write([]byte("true\n"))
				cancel()
				continue
			}

			res, err := sess.addTask(msg)
			if err != nil {
				log.Error(err)
			}

			resbs := []byte(res + "\n")
			conn.Write(resbs)
			log.Debugf("sent: %d bytes", len(resbs))
			log.Debugf("content: '%s'", string(resbs))
		}
	}
}
