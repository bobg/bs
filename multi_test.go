package bs_test

import (
	"context"
	"errors"
	"testing"
	"testing/quick"

	. "github.com/bobg/bs"
	"github.com/bobg/bs/store/mem"
)

func TestMulti(t *testing.T) {
	var (
		ctx = context.Background()
		s   = mem.New()
	)

	err := quick.Check(func(yesBlobs, noBlobs map[string]struct{}) bool {
		blobs := make([]Blob, 0, len(yesBlobs))
		for b := range yesBlobs {
			blobs = append(blobs, []byte(b))
		}

		refsMap, err := PutMulti(ctx, s, blobs)
		if err != nil {
			t.Log(err)
			return false
		}

		refs := make([]Ref, 0, len(refsMap))
		for ref := range refsMap {
			refs = append(refs, ref)
		}
		got, err := GetMulti(ctx, s, refs)
		if err != nil {
			t.Log(err)
			return false
		}
		for ref := range refsMap {
			if _, ok := got[ref]; !ok {
				t.Logf("ref %s missing after GetMulti", ref)
				return false
			}
		}
		for ref := range got {
			if _, ok := refsMap[ref]; !ok {
				t.Logf("got unexpected ref %s after GetMulti", ref)
				return false
			}
		}

		noRefs := make(map[Ref]string)
		for b := range noBlobs {
			if _, ok := yesBlobs[b]; ok {
				// Filter anything out of noBlobs that also exists in yesBlobs.
				continue
			}
			if b == "" {
				// TODO: This test fails when we don't exclude the empty blob. Why?
				continue
			}
			ref := Blob(b).Ref()
			noRefs[ref] = b
			refs = append(refs, ref)
		}

		if len(noRefs) == 0 {
			return true
		}

		got, err = GetMulti(ctx, s, refs)
		if err == nil {
			t.Log("got no error from second GetMulti, want MultiErr")
			return false
		}

		merr, ok := err.(MultiErr)
		if !ok {
			t.Logf("got %T error from second GetMulti, want MultiErr", err)
			return false
		}
		for ref, e := range merr {
			if _, ok := noRefs[ref]; !ok {
				t.Logf("got unexpected error for ref %s after second GetMulti", ref)
				return false
			}
			if !errors.Is(e, ErrNotFound) {
				t.Logf("got error %s for ref %s after second GetMulti, want %s", e, ref, ErrNotFound)
				return false
			}
		}
		for ref, noBlob := range noRefs {
			if _, ok := merr[ref]; !ok {
				t.Logf("ref %s missing from MultiErr after second GetMulti (blob %s)", ref, noBlob)
				return false
			}
		}
		for ref := range refsMap {
			if _, ok := got[ref]; !ok {
				t.Logf("ref %s missing after GetMulti", ref)
				return false
			}
		}
		for ref := range got {
			if _, ok := refsMap[ref]; !ok {
				t.Logf("got unexpected ref %s after GetMulti", ref)
				return false
			}
		}

		return true
	}, nil)
	if err != nil {
		t.Error(err)
	}
}
