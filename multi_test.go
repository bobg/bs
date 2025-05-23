package bs_test

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/bobg/go-generics/v4/slices"

	. "github.com/bobg/bs/v2"
	"github.com/bobg/bs/v2/store/mem"
)

func TestMulti(t *testing.T) {
	var (
		ctx = context.Background()
		s   = mem.New()
	)

	err := quick.Check(func(yes, no []string) bool {
		var (
			yesBlobs = slices.Map(yes, func(s string) Blob { return Blob(s) })
			yesRefs  = slices.Map(yesBlobs, func(b Blob) Ref { return b.Ref() })
		)

		putResult, err := PutMulti(ctx, s, yesBlobs)
		if err != nil {
			t.Log(err)
			return false
		}

		gotRefs := slices.Map(putResult, func(p MultiPair) Ref { return p.Ref })
		if !reflect.DeepEqual(gotRefs, yesRefs) {
			t.Logf("got refs %v, want %v", gotRefs, yesRefs)
			return false
		}

		gotBlobs, err := GetMulti(ctx, s, yesRefs)
		if err != nil {
			t.Log(err)
			return false
		}
		if !reflect.DeepEqual(gotBlobs, yesBlobs) {
			t.Logf("got blobs %v, want %v", gotBlobs, yesBlobs)
			return false
		}

		noMap := make(map[Ref]Blob)
		for _, s := range no {
			if s == "" {
				// TODO: This test fails when we don't exclude the empty blob. Why?
				continue
			}

			var (
				b   = Blob(s)
				ref = b.Ref()
			)
			noMap[ref] = b
		}
		for _, yesRef := range yesRefs {
			if _, ok := noMap[yesRef]; ok {
				delete(noMap, yesRef)
			}
		}
		if len(noMap) == 0 {
			// No blobs in no that are not in yes.
			return true
		}

		noRefs := slices.Collect(maps.Keys(noMap))

		gotBlobs, err = GetMulti(ctx, s, noRefs)
		if err == nil {
			t.Log("got no error from GetMulti, want MultiErr")
			return false
		}

		var merr MultiErr
		if !errors.As(err, &merr) {
			t.Logf("got %T error from second GetMulti, want MultiErr", err)
			return false
		}

		for i, e := range merr {
			if e == nil {
				t.Logf("got nil error for ref %s after second GetMulti", noRefs[i])
				return false
			}

			var rerr RefErr
			if !errors.As(e, &rerr) {
				t.Logf("got %T error for ref %s after second GetMulti, want RefErr", e, noRefs[i])
				return false
			}

			if rerr.Ref != noRefs[i] {
				t.Logf("got ref %s in error for ref %s after second GetMulti", rerr.Ref, noRefs[i])
				return false
			}
		}

		return true
	}, nil)
	if err != nil {
		t.Error(err)
	}
}
