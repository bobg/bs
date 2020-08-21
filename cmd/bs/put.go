package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/bobg/hashsplit"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/split"
)

func (c maincmd) put(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var (
		a       = fs.String("anchor", "", "anchor to assign to added ref")
		dosplit = fs.Bool("split", false, "get a split tree instead of a single blob")
		atstr   = fs.String("at", "", "timestamp for anchor (default: now)")
		bits    = fs.Uint("bits", 0, "with -split, the number of bits to split on (to control chunk size)")
	)
	err := fs.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	var (
		ref   bs.Ref
		added bool
	)
	if *dosplit {
		var splitter *hashsplit.Splitter
		if *bits > 0 {
			splitter = &hashsplit.Splitter{
				SplitBits: *bits,
			}
		}
		ref, err = split.Write(ctx, c.s, os.Stdin, splitter)
		if err != nil {
			return errors.Wrap(err, "splitting stdin to store")
		}
	} else {
		blob, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return errors.Wrap(err, "reading stdin")
		}
		ref, added, err = c.s.Put(ctx, blob, nil)
		if err != nil {
			return errors.Wrap(err, "storing blob")
		}
	}

	if *a != "" {
		at := time.Now()
		if *atstr != "" {
			at, err = parsetime(*atstr)
			if err != nil {
				return errors.Wrap(err, "parsing -at")
			}
		}

		_, _, err = anchor.Put(ctx, c.s, *a, ref, at)
		if err != nil {
			return errors.Wrapf(err, "associating anchor %s with blob %s at time %s", *a, ref, at)
		}
	}

	log.Printf("ref %s (added: %v)", ref, added)

	return nil
}
