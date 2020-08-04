package testutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bobg/bs"
)

// Anchors tests writing, reading, and listing anchors.
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

	err := store.PutAnchor(ctx, a1, t1, r1a)
	if err != nil {
		t.Fatal(err)
	}
	err = store.PutAnchor(ctx, a1, t2, r1b)
	if err != nil {
		t.Fatal(err)
	}
	err = store.PutAnchor(ctx, a2, t1, r2)
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
			got, _, err := store.GetAnchor(ctx, c.a, c.tm)
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

	type anchorTimeRef struct {
		a bs.Anchor
		t time.Time
		r bs.Ref
	}

	var (
		wantAnchorTimeRefs = []anchorTimeRef{
			{a: a1, t: t1, r: r1a},
			{a: a1, t: t2, r: r1b},
			{a: a2, t: t1, r: r2},
		}
		gotAnchorTimeRefs []anchorTimeRef
	)
	gotAnchorFn := func(a bs.Anchor, t time.Time, r bs.Ref) error {
		gotAnchorTimeRefs = append(gotAnchorTimeRefs, anchorTimeRef{a: a, t: t, r: r})
		return nil
	}

	err = store.ListAnchors(ctx, "", gotAnchorFn)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotAnchorTimeRefs) != len(wantAnchorTimeRefs) {
		t.Fatalf("got %d anchors, want %d", len(gotAnchorTimeRefs), len(wantAnchorTimeRefs))
	}
	for i, gotAnchorTimeRef := range gotAnchorTimeRefs {
		wantAnchorTimeRef := wantAnchorTimeRefs[i]
		if gotAnchorTimeRef.a != wantAnchorTimeRef.a ||
			gotAnchorTimeRef.r != wantAnchorTimeRef.r ||
			!gotAnchorTimeRef.t.Equal(wantAnchorTimeRef.t) {
			t.Fatalf("got %+v, want %+v", gotAnchorTimeRefs, wantAnchorTimeRefs)
		}
	}
}
