package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/split"
)

func (c maincmd) put(ctx context.Context, a string, dosplit bool, atstr string, bits, fanout uint, _ []string) error {
	var (
		ref   bs.Ref
		err   error
		added bool
	)
	if dosplit {
		w := split.NewWriter(ctx, c.s, split.Bits(bits))
		_, err = io.Copy(w, os.Stdin)
		if err != nil {
			return errors.Wrap(err, "splitting stdin to store")
		}
		err = w.Close()
		if err != nil {
			return errors.Wrap(err, "finishing splitting stdin to store")
		}
		ref = w.Root
	} else {
		blob, err := io.ReadAll(os.Stdin)
		if err != nil {
			return errors.Wrap(err, "reading stdin")
		}
		ref, added, err = c.s.Put(ctx, blob)
		if err != nil {
			return errors.Wrap(err, "storing blob")
		}
	}

	if a != "" {
		at := time.Now()
		if atstr != "" {
			at, err = parsetime(atstr)
			if err != nil {
				return errors.Wrap(err, "parsing -at")
			}
		}

		err = anchor.Put(ctx, c.s, a, ref, at)
		if err != nil {
			return errors.Wrapf(err, "associating anchor %s with blob %s at time %s", a, ref, at)
		}
	}

	if dosplit {
		fmt.Printf("ref %s\n", ref)
	} else {
		fmt.Printf("ref %s (added: %v)\n", ref, added)
	}

	return nil
}
