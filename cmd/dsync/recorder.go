package main

import (
	"context"
	"time"

	"github.com/bobg/bs"
)

var _ bs.Store = &recorder{}

// Recorder is an implementation of bs.Store that relays reads and writes to a backing store,
// but also sends information about new blob refs and anchors down a pair of channels.
type recorder struct {
	s       bs.Store
	refs    chan<- bs.Ref
	anchors chan<- anchorInfo
}

type anchorInfo struct {
	a   bs.Anchor
	ref bs.Ref
	t   time.Time
}

func newRecorder(s bs.Store, refs chan<- bs.Ref, anchors chan<- anchorInfo) *recorder {
	return &recorder{s: s, refs: refs, anchors: anchors}
}

func (r *recorder) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	return r.s.Get(ctx, ref)
}

func (r *recorder) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	return r.s.GetMulti(ctx, refs)
}

func (r *recorder) GetAnchor(ctx context.Context, a bs.Anchor, t time.Time) (bs.Ref, error) {
	return r.s.GetAnchor(ctx, a, t)
}

func (r *recorder) ListRefs(ctx context.Context, start bs.Ref) (<-chan bs.Ref, func() error, error) {
	return r.s.ListRefs(ctx, start)
}

func (r *recorder) ListAnchors(ctx context.Context, start bs.Anchor) (<-chan bs.Anchor, func() error, error) {
	return r.s.ListAnchors(ctx, start)
}

func (r *recorder) ListAnchorRefs(ctx context.Context, a bs.Anchor) (<-chan bs.TimeRef, func() error, error) {
	return r.s.ListAnchorRefs(ctx, a)
}

func (r *recorder) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref, added, err := r.s.Put(ctx, b)
	if err == nil && added && r.refs != nil {
		select {
		case <-ctx.Done():
			return bs.Zero, false, ctx.Err()
		case r.refs <- ref:
		}
	}
	return ref, added, nil
}

func (r *recorder) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	res, err := r.s.PutMulti(ctx, blobs)
	if err != nil {
		return nil, err
	}

	if r.refs != nil {
		for _, f := range res {
			ref, added, err := f(ctx)
			if err == nil && added {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case r.refs <- ref:
				}
			}
		}
	}

	return res, nil
}

func (r *recorder) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, t time.Time) error {
	err := r.s.PutAnchor(ctx, ref, a, t)
	if err == nil && r.anchors != nil {
		info := anchorInfo{
			a:   a,
			ref: ref,
			t:   t,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r.anchors <- info:
		}
	}
	return err
}
