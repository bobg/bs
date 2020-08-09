package testutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
)

// Anchors tests writing, reading, and listing anchors.
func Anchors(ctx context.Context, t *testing.T, store anchor.Store) {
	var (
		a1 = "anchor1"
		a2 = "anchor2"
		a3 = "anchor3"

		r1a = bs.Ref{0x1a}
		r1b = bs.Ref{0x1b}
		r2  = bs.Ref{0x2}

		t1 = time.Date(1977, 8, 5, 12, 0, 0, 0, time.FixedZone("UTC-4", -4*60*60))
		t2 = t1.Add(time.Hour)
	)

	_, _, err := anchor.Put(ctx, store, a1, r1a, t1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = anchor.Put(ctx, store, a1, r1b, t2)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = anchor.Put(ctx, store, a2, r2, t1)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		a       string
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
