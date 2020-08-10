package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/fs"
)

func (c maincmd) ingest(ctx context.Context, fset *flag.FlagSet, args []string) error {
	var (
		a     = fset.String("anchor", "", "anchor to assign to ingested ref")
		atstr = fset.String("at", "", "timestamp for anchor (default: now)")
	)
	err := fset.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	args = fset.Args()
	if len(args) == 0 {
		return errors.New("missing path to ingest")
	}

	dir := fs.NewDir()
	ref, err := dir.Ingest(ctx, c.s, args[0])
	if err != nil {
		return errors.Wrapf(err, "ingesting %s", args[0])
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

	log.Printf("ref %s", ref)

	return nil
}
