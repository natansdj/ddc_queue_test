package utils

import (
	"log"
)

type AddTask struct {
	Number1 int
	Number2 int
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
