package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

func (c maincmd) put(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var (
		anchor  = fs.String("anchor", "", "anchor to assign to added ref")
		dosplit = fs.Bool("split", false, "get a split tree instead of a single blob")
		atstr   = fs.String("at", "", "timestamp for anchor (default: now)")
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
		ref, err = split.Write(ctx, c.s, os.Stdin, nil)
		if err != nil {
			return errors.Wrap(err, "splitting stdin to store")
		}
	} else {
		blob, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return errors.Wrap(err, "reading stdin")
		}
		ref, added, err = c.s.Put(ctx, blob)
		if err != nil {
			return errors.Wrap(err, "storing blob")
		}
	}

	if *anchor != "" {
		at := time.Now()
		if *atstr != "" {
			at, err = parsetime(*atstr)
			if err != nil {
				return errors.Wrap(err, "parsing -at")
			}
		}

		err = c.s.PutAnchor(ctx, bs.Anchor(*anchor), at, ref)
		if err != nil {
			return errors.Wrapf(err, "associating anchor %s with blob %s at time %s", *anchor, ref, at)
		}
	}

	log.Printf("ref %s (added: %v)", ref, added)

	return nil
}
