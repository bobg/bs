package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/fs"
)

func (c maincmd) ls(ctx context.Context, a, refstr, atstr string, _ []string) error {
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

		ref, err = anchor.Get(ctx, as, a, at)
		if err != nil {
			return errors.Wrapf(err, "getting anchor %s at time %s", a, at)
		}
	} else {
		ref, err = bs.RefFromHex(refstr)
		if err != nil {
			return errors.Wrapf(err, "decoding ref %s", refstr)
		}
	}

	var d fs.Dir
	err = d.Load(ctx, c.s, ref)
	if err != nil {
		return errors.Wrapf(err, "loading dir at ref %s", ref)
	}

	var (
		names   sort.StringSlice
		dirents = make(map[string]*fs.Dirent)
	)

	err = d.Each(ctx, c.s, func(name string, dirent *fs.Dirent) error {
		names = append(names, name)
		dirents[name] = dirent
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "iterating over dir contents")
	}
	names.Sort()
	for _, name := range names {
		dirent := dirents[name]
		mode := os.FileMode(dirent.Mode)
		switch {
		case mode.IsDir():
			fmt.Printf("%s/ %s\n", name, dirent.Item)

		case (mode & os.ModeSymlink) == os.ModeSymlink:
			fmt.Printf("%s -> %s\n", name, dirent.Item)

		default:
			fmt.Printf("%s %s\n", name, dirent.Item)
		}
	}

	return nil
}
