package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"log"
	"os"
	"time"

	"github.com/bobg/bs"
	"github.com/bobg/bs/file"
)

func main() {
	var (
		anchor = flag.String("anchor", "", "anchor")
		get    = flag.Bool("get", false, "get")
		put    = flag.Bool("put", false, "put")
		root   = flag.String("root", "", "root")
		atstr  = flag.String("at", "", "at")
	)
	flag.Parse()

	if *get && *put {
		log.Fatal("-get and -put are mutually exclusive")
	}
	if !*get && !*put {
		log.Fatal("must supply one of -get and -put")
	}

	var (
		err       error
		ctx       = context.Background()
		filestore = file.New(*root)
	)

	at := time.Now()
	if *atstr != "" {
		at, err = parsetime(*atstr)
		if err != nil {
			log.Fatalf("parsing -at time: %s", err)
		}
	}

	if *put {
		ref, err := bs.SplitWrite(ctx, filestore, os.Stdin, nil)
		if err != nil {
			log.Fatalf("writing to filestore: %s", err)
		}
		if *anchor != "" {
			err = filestore.PutAnchor(ctx, ref, bs.Anchor(*anchor), at)
			if err != nil {
				log.Fatalf("storing anchor: %s", err)
			}
		}
	} else {
		var ref bs.Ref
		if *anchor != "" {
			ref, err = filestore.GetAnchor(ctx, bs.Anchor(*anchor), at)
			if err != nil {
				log.Fatalf("getting anchor: %s", err)
			}
		} else if flag.NArg() != 1 {
			log.Fatalf("usage: %s -get REF", os.Args[0])
		} else {
			_, err = hex.Decode(ref[:], []byte(flag.Arg(0)))
			if err != nil {
				log.Fatalf("parsing command-line ref: %s", err)
			}
		}
		err = bs.SplitRead(ctx, filestore, ref, os.Stdout)
		if err != nil {
			log.Fatalf("reading from filestore: %s", err)
		}
	}
}

var layouts = []string{
	time.RFC3339Nano, time.RFC3339, time.ANSIC, time.UnixDate,
}

func parsetime(s string) (time.Time, error) {
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil { // sic
			return t, nil
		}
	}
	return time.Time{}, errors.New("could not parse time")
}
