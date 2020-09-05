package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
)

func (c maincmd) listRefs(ctx context.Context, fs *flag.FlagSet, args []string) error {
	start := fs.String("start", "", "start after this ref")
	err := fs.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	var startRef bs.Ref
	if *start != "" {
		startRef, err = bs.RefFromHex(*start)
		if err != nil {
			return errors.Wrap(err, "parsing start ref")
		}
	}

	return c.s.ListRefs(ctx, startRef, func(ref bs.Ref, types []bs.Ref) error {
		fmt.Printf("%s", ref)
		for _, typ := range types {
			fmt.Printf(" %s", typ)
		}
		fmt.Print("\n")
		return nil
	})
}

func (c maincmd) listAnchors(ctx context.Context, fs *flag.FlagSet, args []string) error {
	start := fs.String("start", "", "start after this anchor name")
	err := fs.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	return c.s.ListAnchors(ctx, *start, func(name string, ref bs.Ref, at time.Time) error {
		fmt.Printf("%s %s %s\n", name, ref, at)
		return nil
	})
}
