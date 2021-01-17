package main

import (
	"os"

	log "github.com/sirupsen/logrus"
)

const fileTest1 = "work/test1"
const fileTest2 = "work/test2"

func main() {
	log.SetLevel(log.DebugLevel)

	if err := os.MkdirAll("work", os.ModePerm); err != nil {
		log.Panic(err)
	}

	file, err := os.OpenFile(fileTest1, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Panic(err)
		}
	}()

	var newSize int64

	n, err := file.WriteString("h\n")
	if err != nil {
		log.Error(err)
	}

	newSize += int64(n)
	file.Truncate(newSize)
}
