package main

import (
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

const fileTest1 = "work/test1"
const fileTest2 = "work/test2"

func main() {
	log.SetLevel(log.DebugLevel)

	if err := os.MkdirAll("work", os.ModePerm); err != nil {
		log.Fatal(err)
	}

	file, err := os.Create(fileTest1)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	file.WriteString("foo\n")

	time.Sleep(3 * time.Second)

	if err := os.Rename(fileTest1, fileTest2); err != nil {
		log.Fatal(err)
	}

	file.WriteString("bar\n")
}
