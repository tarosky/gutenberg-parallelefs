package main

import (
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

const FileTest1 = "work/test1"

func main() {
	log.SetLevel(log.DebugLevel)

	if err := os.MkdirAll("work", os.ModePerm); err != nil {
		log.Fatal(err)
	}

	file, err := os.Create(FileTest1)
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

	// if err := os.Chmod(FileTest1, 0400); err != nil {
	// 	log.Fatal(err)
	// }

	if err := os.Remove(FileTest1); err != nil {
		log.Fatal(err)
	}

	file.WriteString("bar\n")
}
