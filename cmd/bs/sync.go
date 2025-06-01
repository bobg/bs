package main

import (
	"context"

	"github.com/bobg/errors"

	"github.com/bobg/bs/v2"
	"github.com/bobg/bs/v2/store"
)

func (c maincmd) sync(ctx context.Context, args []string) error {
	stores := []bs.Store{c.s}
	for _, arg := range args {
		s, err := store.FromConfigFile(ctx, arg)
		if err != nil {
			return errors.Wrapf(err, "reading %s", arg)
		}
		stores = append(stores, s)
	}

	return store.Sync(ctx, stores)
}
