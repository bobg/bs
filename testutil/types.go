package testutil

import (
	"context"
	"testing"
	"testing/quick"

	"github.com/bobg/bs"
)

// Types tests storing and retrieving types for refs.
func Types(ctx context.Context, t *testing.T, store bs.TStore) {
	f := func(ref bs.Ref, types [][]byte) bool {
		typesMap := make(map[string]struct{})
		for _, typ := range types {
			typesMap[string(typ)] = struct{}{}
			err := store.PutType(ctx, ref, typ)
			if err != nil {
				t.Logf("Error storing type: %s", err)
				return false
			}
		}
		got, err := store.GetTypes(ctx, ref)
		if err != nil {
			t.Logf("Error getting types: %s", err)
			return false
		}

		if len(got) != len(typesMap) {
			t.Logf("Got %d types, want %d", len(got), len(types))
			return false
		}

		for _, g := range got {
			if _, ok := typesMap[string(g)]; !ok {
				t.Logf("Got invalid type %x", g)
				return false
			}
		}

		return true
	}
	err := quick.Check(f, nil)
	if err != nil {
		t.Error(err)
	}
}
