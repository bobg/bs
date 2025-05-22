package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bobg/errors"

	"github.com/bobg/bs/v2"
	"github.com/bobg/bs/v2/anchor"
)

func (c maincmd) listRefs(ctx context.Context, start string, _ []string) error {
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

	refs, errptr := c.s.ListRefs(ctx, startRef)
	for ref := range refs {
		fmt.Printf("%s\n", ref)
	}

	return *errptr
}

func (c maincmd) listAnchors(ctx context.Context, _ []string) error {
	return anchor.Each(ctx, c.s, func(name string, ref bs.Ref, at time.Time) error {
		fmt.Printf("%s %s %s\n", name, ref, at)
		return nil
	})
}
