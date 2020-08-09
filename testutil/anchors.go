package testutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

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

		t1Time = time.Date(1977, 8, 5, 12, 0, 0, 0, time.FixedZone("UTC-4", -4*60*60))
		t2Time = t1Time.Add(time.Hour)
		t1     = timestamppb.New(t1Time)
		t2     = timestamppb.New(t2Time)
	)

	_, _, err := bs.PutProto(ctx, store, &anchor.Anchor{Name: a1, Ref: r1a[:], At: t1})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = bs.PutProto(ctx, store, &anchor.Anchor{Name: a1, Ref: r1b[:], At: t2})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = bs.PutProto(ctx, store, &anchor.Anchor{Name: a2, Ref: r2[:], At: t1})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		a       string
		tm      time.Time
		want    bs.Ref
		wantErr error
	}{
		{a: a1, tm: t1Time, want: r1a},
		{a: a1, tm: t1Time.Add(time.Minute), want: r1a},
		{a: a1, tm: t2Time, want: r1b},
		{a: a1, tm: t2Time.Add(time.Minute), want: r1b},
		{a: a1, tm: t1Time.Add(-time.Minute), wantErr: bs.ErrNotFound},
		{a: a1, tm: t2Time.Add(-time.Minute), want: r1a},

		{a: a2, tm: t1Time, want: r2},
		{a: a2, tm: t1Time.Add(time.Minute), want: r2},
		{a: a2, tm: t1Time.Add(-time.Minute), wantErr: bs.ErrNotFound},

		{a: a3, tm: t2Time, wantErr: bs.ErrNotFound},
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
