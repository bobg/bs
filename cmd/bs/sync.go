package main

import (
	"context"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

func (c maincmd) sync(ctx context.Context, args []string) error {
	stores := []bs.Store{c.s}
	for _, arg := range args {
		s, err := storeFromConfig(ctx, arg)
		if err != nil {
			return errors.Wrapf(err, "reading %s", arg)
		}
		stores = append(stores, s)
	}

	return store.Sync(ctx, stores)
}
