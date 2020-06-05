package bs

import (
	"sort"
	"time"
)

type TimeRef struct {
	T time.Time
	R Ref
}

func FindAnchor(pairs []TimeRef, at time.Time) (Ref, error) {
	index := sort.Search(len(pairs), func(n int) bool {
		return !pairs[n].T.Before(at)
	})
	if index == len(pairs) {
		return Zero, ErrNotFound
	}
	if pairs[index].T.Equal(at) {
		return pairs[index].R, nil
	}
	if index == 0 {
		return Zero, ErrNotFound
	}
	return pairs[index-1].R, nil
}
