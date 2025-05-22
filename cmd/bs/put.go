package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bobg/errors"

	"github.com/bobg/bs/v2"
	"github.com/bobg/bs/v2/anchor"
	"github.com/bobg/bs/v2/split"
)

func (c maincmd) put(ctx context.Context, a string, dosplit bool, atstr string, bits, fanout int, _ []string) error {
	var (
		ref   bs.Ref
		err   error
		added bool
	)
	if dosplit {
		var err error
		ref, err = split.Split(ctx, c.s, os.Stdin, split.Bits(bits), split.Fanout(fanout))
		if err != nil {
			return errors.Wrap(err, "splitting stdin to store")
		}
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
