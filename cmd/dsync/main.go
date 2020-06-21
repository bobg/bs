package main

import (
	"context"
	"log"
)

func main() {
	err := runPrimary(context.Background(), "/home/bobg/go/src/github.com/interstellar/seq", "xxx")
	if err != nil {
		log.Fatal(err)
	}
}
