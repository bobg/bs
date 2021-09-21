package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/fs"
)

func (c maincmd) addToDir(ctx context.Context, a, atstr, refstr, path string, _ []string) error {
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

	var ref bs.Ref
	if refstr != "" {
		// Note: User may supply both -ref and -anchor,
		// in which case -ref is the "before" ref of the dir and -anchor is assigned afterwards.
		ref, err = bs.RefFromHex(refstr)
		if err != nil {
			return errors.Wrapf(err, "parsing -ref %s", refstr)
		}
	} else if a != "" {
		ref, err = anchor.Get(ctx, c.s, a, at)
		if err != nil && !errors.Is(err, bs.ErrNotFound) {
			return errors.Wrapf(err, "getting anchor %s as of %s", a, at)
		}
	}

	var dir *fs.Dir
	if ref.IsZero() {
		dir = fs.NewDir()
	} else {
		dir = new(fs.Dir)
		err = dir.Load(ctx, c.s, ref)
		if err != nil {
			return errors.Wrapf(err, "loading dir at %s", ref)
		}
	}

	ref, err = dir.Add(ctx, c.s, path)
	if err != nil {
		return errors.Wrapf(err, "adding %s to dir", path)
	}

	if a != "" {
		err = anchor.Put(ctx, c.s, a, ref, at)
		if err != nil {
			return errors.Wrapf(err, "adding anchor %s for dir %s as of %s", a, ref, at)
		}
	}

	fmt.Printf("ref %s\n", ref)

	return nil
}
