package main

import (
	"context"
	"flag"
	"fmt"

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

	return c.s.ListRefs(ctx, startRef, func(ref, typ bs.Ref) error {
		fmt.Printf("%s %s\n", ref, typ)
		return nil
	})
}
