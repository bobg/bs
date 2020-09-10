package main

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/fs"
)

func (c maincmd) addToDir(ctx context.Context, a, atstr, refstr string, args []string) error {
	if len(args) == 0 {
		return errors.New("missing path to add")
	}

	var (
		at  = time.Now()
		err error
	)
	if atstr != "" {
		at, err = parsetime(atstr)
		if err != nil {
			return errors.Wrap(err, "parsing -at")
		}
	}

	var (
		ref bs.Ref
		dir *fs.Dir
	)
	if refstr != "" {
		// Note: User may supply both -ref and -anchor,
		// in which case -ref is the "before" ref of the dir and -anchor is assigned afterwards.
		ref, err = bs.RefFromHex(refstr)
		if err != nil {
			return errors.Wrapf(err, "parsing -ref %s", refstr)
		}
	} else if a != "" {
		ref, err = c.s.GetAnchor(ctx, a, at)
		if err != nil && !errors.Is(err, bs.ErrNotFound) {
			return errors.Wrapf(err, "getting anchor %s as of %s", a, at)
		}
	}
	if ref != (bs.Ref{}) {
		err = dir.Load(ctx, c.s, ref)
		if err != nil {
			return errors.Wrapf(err, "loading dir at %s", ref)
		}
	} else {
		dir = fs.NewDir()
	}

	ref, err = dir.Add(ctx, c.s, args[0], at)
	if err != nil {
		return errors.Wrapf(err, "adding %s to dir", args[0])
	}

	if a != "" {
		_, _, err = anchor.Put(ctx, c.s, a, ref, at)
		if err != nil {
			return errors.Wrapf(err, "adding anchor %s for dir %s as of %s", a, ref, at)
		}
	}

	log.Printf("ref %s", ref)

	return nil
}
