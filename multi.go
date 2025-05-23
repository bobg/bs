package bs

import (
	"context"
	"fmt"
	"strings"

	"github.com/bobg/go-generics/v4/parallel"
	"github.com/bobg/go-generics/v4/slices"
)

// GetMulti gets multiple blobs with a single call.
// By default this is implemented as a bunch of concurrent individual Get calls.
// However, if g implements MultiGetter, its GetMulti method is used instead.
// The return value is a sequence of blobs corresponding to the input refs.
// The returned error may be a MultiErr,
// mapping input refs to errors encountered retrieving those specific refs.
// This function may return a successful partial result even in case of error.
// In particular, when the error return is a MultiErr,
// every input ref appears in either the result map or the MultiErr map.
func GetMulti(ctx context.Context, g Getter, refs []Ref) ([]Blob, error) {
	if m, ok := g.(MultiGetter); ok {
		return m.GetMulti(ctx, refs)
	}

	type tuple struct {
		blob Blob
		err  error
	}

	var anyErrs bool

	vals, err := parallel.Values(ctx, len(refs), func(ctx context.Context, i int) (tuple, error) {
		blob, err := g.Get(ctx, refs[i])
		if err != nil {
			anyErrs = true
		}
		return tuple{blob: blob, err: err}, nil
	})
	if err != nil {
		return nil, err
	}

	result := slices.Map(vals, func(v tuple) Blob { return v.blob })
	if !anyErrs {
		return result, nil
	}

	errs, _ := slices.Mapx(vals, func(i int, v tuple) (error, error) {
		if v.err == nil {
			return nil, nil
		}
		return RefErr{Ref: refs[i], Err: v.err}, nil
	})
	return result, MultiErr(errs)
}

// MultiErr is a type of error returned by GetMulti and PutMulti.
// It contains one slot for each Ref or Blob of the input.
// Each slot is nil for success, or the error produced by the corresponding input.
type MultiErr []error

type RefErr struct {
	Ref Ref
	Err error
}

func (e RefErr) Error() string {
	return fmt.Sprintf("%s: %s", e.Ref, e.Err)
}

func (e RefErr) Unwrap() error {
	return e.Err
}

// Error implements the error interface.
func (e MultiErr) Error() string {
	errs := e.Unwrap()
	strs := slices.Map(errs, func(err error) string { return err.Error() })
	return strings.Join(strs, "; ")
}

func (e MultiErr) Unwrap() []error {
	errs := []error(e)
	return slices.Filter(errs, func(err error) bool { return err != nil })
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
func PutMulti(ctx context.Context, s Store, blobs []Blob) ([]MultiPair, error) {
	if m, ok := s.(MultiPutter); ok {
		return m.PutMulti(ctx, blobs)
	}

	type triple struct {
		ref   Ref
		added bool
		err   error
	}

	var anyErrs bool

	vals, err := parallel.Values(ctx, len(blobs), func(ctx context.Context, i int) (triple, error) {
		ref, added, err := s.Put(ctx, blobs[i])
		if err != nil {
			anyErrs = true
		}
		return triple{ref: ref, added: added, err: err}, nil
	})
	if err != nil {
		return nil, err
	}

	result := slices.Map(vals, func(v triple) MultiPair {
		return MultiPair{Ref: v.ref, Added: v.added}
	})
	if !anyErrs {
		return result, nil
	}

	errs, _ := slices.Mapx(vals, func(i int, v triple) (error, error) {
		if v.err == nil {
			return nil, nil
		}
		if v.ref.IsZero() {
			return v.err, nil
		}
		return RefErr{Ref: v.ref, Err: v.err}, nil
	})
	return result, MultiErr(errs)
}
