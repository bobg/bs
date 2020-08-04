package dsync

import (
	"context"
	"time"

	"github.com/bobg/bs"
)

var _ bs.Store = &Streamer{}

// Streamer is a blob store that wraps another blob store,
// delegating all operations to it.
// Additionally, when writing to the store,
// each call to Put or PutMulti sends any new refs on one channel
// (but not refs that already existed in the store),
// and each call to PutAnchor sends anchors on another.
type Streamer struct {
	s       bs.Store
	refs    chan<- bs.Ref
	anchors chan<- AnchorTuple
}

// AnchorTuple packages up the parameters in a call to PutAnchor.
type AnchorTuple struct {
	A   bs.Anchor
	Ref bs.Ref
	T   time.Time
}

// NewStreamer creates a Streamer wrapping blob store `s`.
// If `refs` is non-nil,
// the Streamer will send each newly added ref to it
// (but not refs that were already present)
// in calls to Put and PutMulti.
// If `anchors` is non-nil,
// the Streamer will send AnchorTuples for each call to PutAnchor.
// All channel sends are blocking.
func NewStreamer(s bs.Store, refs chan<- bs.Ref, anchors chan<- AnchorTuple) *Streamer {
	return &Streamer{s: s, refs: refs, anchors: anchors}
}

// Get implements bs.Store.Get.
func (s *Streamer) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	return s.s.Get(ctx, ref)
}

// GetMulti implements bs.Store.GetMulti.
func (s *Streamer) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	return s.s.GetMulti(ctx, refs)
}

// GetAnchor implements bs.Store.GetAnchor.
func (s *Streamer) GetAnchor(ctx context.Context, a bs.Anchor, t time.Time) (bs.Ref, time.Time, error) {
	return s.s.GetAnchor(ctx, a, t)
}

// ListRefs implements bs.Store.ListRefs.
func (s *Streamer) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	return s.s.ListRefs(ctx, start, f)
}

// ListAnchors implements bs.Store.ListAnchors.
func (s *Streamer) ListAnchors(ctx context.Context, start bs.Anchor, f func(bs.Anchor, time.Time, bs.Ref) error) error {
	return s.s.ListAnchors(ctx, start, f)
}

// Put implements bs.Store.Put.
func (s *Streamer) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref, added, err := s.s.Put(ctx, b)
	if err == nil && added && s.refs != nil {
		select {
		case <-ctx.Done():
			return bs.Ref{}, false, ctx.Err()
		case s.refs <- ref:
		}
	}
	return ref, added, nil
}

// PutMulti implements bs.Store.PutMulti.
func (s *Streamer) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	res, err := s.s.PutMulti(ctx, blobs)
	if err != nil {
		return nil, err
	}

	if s.refs != nil {
		for _, f := range res {
			ref, added, err := f(ctx)
			if err == nil && added {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case s.refs <- ref:
				}
			}
		}
	}

	return res, nil
}

// PutAnchor implements bs.Store.PutAnchor.
func (s *Streamer) PutAnchor(ctx context.Context, a bs.Anchor, t time.Time, ref bs.Ref) error {
	err := s.s.PutAnchor(ctx, a, t, ref)
	if err == nil && s.anchors != nil {
		info := AnchorTuple{
			A:   a,
			Ref: ref,
			T:   t,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.anchors <- info:
		}
	}
	return err
}
