package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
)

func (c maincmd) listRefs(ctx context.Context, start string, args []string) error {
	var (
		startRef bs.Ref
		err      error
	)

	if start != "" {
		startRef, err = bs.RefFromHex(start)
		if err != nil {
			return errors.Wrap(err, "parsing start ref")
		}
	}

	return c.s.ListRefs(ctx, startRef, func(ref bs.Ref) error {
		fmt.Printf("%s\n", ref)
		return nil
	})
}

func (c maincmd) listAnchors(ctx context.Context, args []string) error {
	return anchor.Each(ctx, c.s, func(name string, ref bs.Ref, at time.Time) error {
		fmt.Printf("%s %s %s\n", name, ref, at)
		return nil
	})
}
