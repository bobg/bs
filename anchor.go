package bs

import (
	"sort"
	"time"
)

// TimeRef is a blob-reference / timestamp pair.
// Abstractly, an anchor maps to one or more TimeRefs.
type TimeRef struct {
	T time.Time
	R Ref
}

// FindAnchor is a helper for finding the latest blob reference
// in a list of TimeRefs, sorted by time,
// whose timestamp is not later than `at`.
func FindAnchor(pairs []TimeRef, at time.Time) (Ref, time.Time, error) {
	if len(pairs) == 0 {
		return Ref{}, time.Time{}, ErrNotFound
	}
	index := sort.Search(len(pairs), func(n int) bool {
		return !pairs[n].T.Before(at)
	})
	if index < len(pairs) && pairs[index].T.Equal(at) {
		return pairs[index].R, pairs[index].T, nil
	}
	if index == 0 {
		return Ref{}, time.Time{}, ErrNotFound
	}
	return pairs[index-1].R, pairs[index-1].T, nil
}
