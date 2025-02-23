package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli/v2"
)

func main() {
	log.SetLevel(log.DebugLevel)

	app := &cli.App{
		Name:  "client",
		Usage: "parallelefs client",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:     "socket",
				Aliases:  []string{"s"},
				Required: true,
				Usage:    "path to the socket file",
			},
		},
		Action: func(c *cli.Context) error {
			socket := c.Path("socket")
			connect(socket)
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Panic(err)
	}
}

func connect(socket string) {
	conn, err := net.Dial("unix", socket)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error(err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	inputCh := make(chan string)
	responseCh := make(chan string)

	go func() {
		for {
			fmt.Print("Input: ")
			var input string
			fmt.Scanln(&input)
			inputCh <- input
			res := <-responseCh
			fmt.Println("Response: " + res)
		}
	}()

	go func() {
		recv := bufio.NewScanner(conn)
		for recv.Scan() {
			responseCh <- string(recv.Bytes())
		}
	}()

	for {
		select {
		case <-sigCh:
			return
		case input := <-inputCh:
			if _, err := conn.Write([]byte(input + "\n")); err != nil {
				log.Error(err)
			}
		}
	}
}
