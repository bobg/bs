package bs

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// GetMulti gets multiple blobs with a single call.
// By default this is implemented as a bunch of concurrent individual Get calls.
// However, if g implements MultiGetter, its GetMulti method is used instead.
// The return value is a mapping of input refs to the blobs that were found in g.
// The returned error may be a MultiErr,
// mapping input refs to errors encountered retrieving those specific refs.
// This function may return a successful partial result even in case of error.
// In particular, when the error return is a MultiErr,
// every input ref appears in either the result map or the MultiErr map.
func GetMulti(ctx context.Context, g Getter, refs []Ref) (map[Ref]Blob, error) {
	if m, ok := g.(MultiGetter); ok {
		return m.GetMulti(ctx, refs)
	}

	type triple struct {
		ref  Ref
		blob Blob
		err  error
	}

	var (
		res = make(map[Ref]Blob)
		ch  = make(chan triple)
		wg  sync.WaitGroup
	)

	for _, ref := range refs {
		ref := ref
		wg.Add(1)
		go func() {
			blob, err := g.Get(ctx, ref)
			ch <- triple{ref: ref, blob: blob, err: err}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var errmap MultiErr

	for trip := range ch {
		if trip.err != nil {
			if errmap == nil {
				errmap = make(MultiErr)
			}
			errmap[trip.ref] = trip.err
			continue
		}
		res[trip.ref] = trip.blob
	}

	// Go typed-nil pitfall: can't just return errmap if errmap is nil.
	if errmap != nil {
		return res, errmap
	}
	return res, nil
}

// MultiErr is a type of error returned by GetMulti and PutMulti.
// It maps individual refs to errors encountered trying to Get or Put them.
type MultiErr map[Ref]error

// Error implements the error interface.
func (e MultiErr) Error() string {
	var strs []string
	for ref, err := range e {
		strs = append(strs, fmt.Sprintf("%s: %s", ref, err))
	}
	return "error(s): " + strings.Join(strs, "; ")
}

// PutMulti stores multiple blobs with a single call.
// By default this is implemented as a bunch of concurrent individual Put calls.
// However, if s implements MultiPutter, its PutMulti method is used instead.
// The return value is a mapping of input blobs' refs to a boolean indicating whether each was a new addition to s.
// The returned error may be a MultiErr,
// mapping input blobs' refs to errors encountered writing those specific blobs.
// This function may return a successful partial result even in case of error.
// In particular, when the error return is a MultiErr,
// the ref of every input blob appears in either the result map or the MultiErr map.
func PutMulti(ctx context.Context, s Store, blobs []Blob) (map[Ref]bool, error) {
	if m, ok := s.(MultiPutter); ok {
		return m.PutMulti(ctx, blobs)
	}

	type triple struct {
		ref   Ref
		added bool
		err   error
	}

	var (
		res = make(map[Ref]bool)
		ch  = make(chan triple)
		wg  sync.WaitGroup
	)

	for _, blob := range blobs {
		blob := blob
		wg.Add(1)
		go func() {
			ref, added, err := s.Put(ctx, blob)
			ch <- triple{ref: ref, added: added, err: err}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var errmap MultiErr

	for trip := range ch {
		if trip.err != nil {
			if errmap == nil {
				errmap = make(MultiErr)
			}
			errmap[trip.ref] = trip.err
			continue
		}
		res[trip.ref] = trip.added
	}

	// Go typed-nil pitfall: can't just return errmap if errmap is nil.
	if errmap != nil {
		return res, errmap
	}
	return res, nil
}
