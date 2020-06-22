package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
)

func main() {
	var (
		primary = flag.Bool("primary", false, "run as primary")
		replica = flag.Bool("replica", false, "run as replica")
		root    = flag.String("root", "", "root dir")
		addr    = flag.String("addr", "localhost:", "primary: replica is at this addr; replica: listen on this addr")
	)
	flag.Parse()

	if !*primary && !*replica {
		log.Fatal("must supply one of -primary or -replica")
	}
	if *primary && *replica {
		log.Fatal("cannot supply both -primary and -replica")
	}
	if *root == "" {
		log.Fatal("must specify root")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		sig := <-sigCh
		log.Printf("got signal %s", sig)
		cancel()
	}()

	if *primary {
		err := runPrimary(ctx, *root, "http://"+*addr)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		err := runReplica(ctx, *root, *addr)
		if err != nil {
			log.Fatal(err)
		}
	}
}
