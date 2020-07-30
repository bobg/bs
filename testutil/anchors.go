package testutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bobg/bs"
)

func Anchors(ctx context.Context, t *testing.T, store bs.Store) {
	var (
		a1 = bs.Anchor("anchor1")
		a2 = bs.Anchor("anchor2")
		a3 = bs.Anchor("anchor3")

		r1a = bs.Ref{0x1a}
		r1b = bs.Ref{0x1b}
		r2  = bs.Ref{0x2}

		t1 = time.Date(1977, 8, 5, 12, 0, 0, 0, time.FixedZone("UTC-4", -4*60*60))
		t2 = t1.Add(time.Hour)
	)

	err := store.PutAnchor(ctx, r1a, a1, t1)
	if err != nil {
		t.Fatal(err)
	}
	err = store.PutAnchor(ctx, r1b, a1, t2)
	if err != nil {
		t.Fatal(err)
	}
	err = store.PutAnchor(ctx, r2, a2, t1)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		a       bs.Anchor
		tm      time.Time
		want    bs.Ref
		wantErr error
	}{
		{a: a1, tm: t1, want: r1a},
		{a: a1, tm: t1.Add(time.Minute), want: r1a},
		{a: a1, tm: t2, want: r1b},
		{a: a1, tm: t2.Add(time.Minute), want: r1b},
		{a: a1, tm: t1.Add(-time.Minute), wantErr: bs.ErrNotFound},
		{a: a1, tm: t2.Add(-time.Minute), want: r1a},

		{a: a2, tm: t1, want: r2},
		{a: a2, tm: t1.Add(time.Minute), want: r2},
		{a: a2, tm: t1.Add(-time.Minute), wantErr: bs.ErrNotFound},

		{a: a3, tm: t2, wantErr: bs.ErrNotFound},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case_%02d", i+1), func(t *testing.T) {
			got, err := store.GetAnchor(ctx, c.a, c.tm)
			if c.wantErr != nil && errors.Is(err, c.wantErr) {
				// ok
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}
