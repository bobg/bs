package store

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/bobg/bs"
)

// Sync synchronizes two or more stores.
// It runs ListRefs on all input stores.
// When a ref is found to be in some but not all stores,
// its blob and types are added to the stores where it's missing.
//
// TODO: Two stores may have the same ref but differ in the set of types for it.
// This function will not synchronize the missing type information in that case, but it should.
func Sync(ctx context.Context, stores []bs.Store) error {
	if len(stores) < 2 {
		return nil
	}

	type typedRef struct {
		ref   bs.Ref
		types []bs.Ref
	}

	type tuple struct {
		n     int
		s     bs.Store
		ch    <-chan typedRef
		ref   *bs.Ref
		types []bs.Ref
	}

	eg, ctx2 := errgroup.WithContext(ctx)

	tuples := make([]*tuple, 0, len(stores))
	for i, s := range stores {
		i, s := i, s
		ch := make(chan typedRef)
		eg.Go(func() error {
			defer close(ch)
			return s.ListRefs(ctx2, bs.Ref{}, func(ref bs.Ref, types []bs.Ref) error {
				select {
				case <-ctx2.Done():
					return ctx2.Err()
				case ch <- typedRef{ref: ref, types: types}:
				}
				return nil
			})
		})
		tuples = append(tuples, &tuple{n: i, s: s, ch: ch})
	}

	errch := make(chan error)

	go func() {
		err := eg.Wait()
		if err != nil {
			errch <- err
		}
		close(errch)
	}()

	havers := tuples
	for {
		var any bool
		for _, tup := range havers {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err, ok := <-errch:
				if ok && err != nil {
					return err
				}
			case tr, ok := <-tup.ch:
				if ok {
					any = true
					tup.ref = &tr.ref
					tup.types = tr.types
				} else {
					tup.ref = nil
				}
			}
		}
		if !any {
			// We've reached the end of input on all channels.
			return <-errch
		}

		sort.Slice(tuples, func(i, j int) bool {
			ri := tuples[i].ref
			rj := tuples[j].ref
			if ri != nil {
				if rj != nil {
					return ri.Less(*rj)
				}
				return true
			}
			return false
		})

		ref := *(tuples[0].ref)

		havers = []*tuple{tuples[0]}
		i := 1
		for i < len(tuples) && tuples[i].ref != nil && *(tuples[i].ref) == ref {
			havers = append(havers, tuples[i])
			i++
		}

		if i == len(tuples) {
			continue
		}

		needers := tuples[i:]

		blob, types, err := havers[0].s.Get(ctx, ref)
		if err != nil {
			return errors.Wrapf(err, "getting blob for %s", ref)
		}

		for _, tup := range needers {
			if len(types) == 0 {
				_, _, err = tup.s.Put(ctx, blob, nil)
				if err != nil {
					return errors.Wrapf(err, "storing blob for %s", ref)
				}
			} else {
				for _, typ := range types {
					_, _, err = tup.s.Put(ctx, blob, &typ)
					if err != nil {
						return errors.Wrapf(err, "storing blob for %s", ref)
					}
				}
			}
		}
	}
}
