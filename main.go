package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli/v2"
)

func main() {
	log.SetLevel(log.DebugLevel)

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
			socket := c.Path("socket")
			// fmt.Printf("Hello %s", socketPath)
			listen(socket)
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func handleSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	go func() {
		<-c
		os.Exit(0)
	}()
}

func listen(socket string) {
	if err := os.Remove(socket); err != nil {
		// Ignore error
	}

	listener, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Error(err)
		}
	}()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Error(err)
				continue
			}

			go handleConnection(conn)
		}
	}()

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, os.Kill)
	<-sigCh
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	recv := bufio.NewScanner(conn)
	for recv.Scan() {
		msg := string(recv.Bytes())

		if msg == "exit" {
			return
		}

		fmt.Println("res> " + msg)
	}
}
