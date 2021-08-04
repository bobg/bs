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

func (c maincmd) get(ctx context.Context, a string, refstr string, dosplit bool, atstr string, args []string) error {
	if (a == "" && refstr == "") || (a != "" && refstr != "") {
		return errors.New("must supply one of -anchor or -ref")
	}

	var (
		ref bs.Ref
		err error
	)

	if a != "" {
		at := time.Now()
		if atstr != "" {
			at, err = parsetime(atstr)
			if err != nil {
				return errors.Wrap(err, "parsing -at")
			}
		}

		as, ok := c.s.(anchor.Store)
		if !ok {
			return fmt.Errorf("%T is not an anchor.Store", c.s)
		}

		ref, err = as.GetAnchor(ctx, a, at)
		if err != nil {
			return errors.Wrapf(err, "getting anchor %s at time %s", a, at)
		}
	} else {
		ref, err = bs.RefFromHex(refstr)
		if err != nil {
			return errors.Wrapf(err, "decoding ref %s", refstr)
		}
	}

	if dosplit {
		r, err := split.NewReader(ctx, c.s, ref)
		if err != nil {
			return errors.Wrapf(err, "creating split reader for %s", ref)
		}
		_, err = io.Copy(os.Stdout, r)
		return errors.Wrap(err, "copying to stdout")
	}

	blob, err := c.s.Get(ctx, ref)
	if err != nil {
		return errors.Wrapf(err, "getting blob %s", ref)
	}
	_, err = os.Stdout.Write(blob)
	return errors.Wrap(err, "writing blob to stdout")
}

func (c maincmd) getAnchor(ctx context.Context, atstr string, args []string) error {
	if len(args) == 0 {
		return errors.New("missing anchor")
	}

	var (
		a   = args[0]
		err error
	)

	at := time.Now()
	if atstr != "" {
		at, err = parsetime(atstr)
		if err != nil {
			return errors.Wrap(err, "parsing -at")
		}
	}

	as, ok := c.s.(anchor.Store)
	if !ok {
		return fmt.Errorf("%T is not an anchor.Store", c.s)
	}

	ref, err := as.GetAnchor(ctx, a, at)
	if err != nil {
		return errors.Wrapf(err, "getting anchor %s at time %s", a, at)
	}

	fmt.Printf("%s\n", ref)
	return nil
}
