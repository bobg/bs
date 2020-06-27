package dsync

import (
	"context"
	"time"

	"github.com/bobg/bs"
)

var _ bs.Store = &Streamer{}

type Streamer struct {
	s       bs.Store
	refs    chan<- bs.Ref
	anchors chan<- AnchorTuple
}

type AnchorTuple struct {
	A   bs.Anchor
	Ref bs.Ref
	T   time.Time
}

func NewStreamer(s bs.Store, refs chan<- bs.Ref, anchors chan<- AnchorTuple) *Streamer {
	return &Streamer{s: s, refs: refs, anchors: anchors}
}

func (s *Streamer) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	return s.s.Get(ctx, ref)
}

func (s *Streamer) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	return s.s.GetMulti(ctx, refs)
}

func (s *Streamer) GetAnchor(ctx context.Context, a bs.Anchor, t time.Time) (bs.Ref, error) {
	return s.s.GetAnchor(ctx, a, t)
}

func (s *Streamer) ListRefs(ctx context.Context, start bs.Ref) (<-chan bs.Ref, func() error, error) {
	return s.s.ListRefs(ctx, start)
}

func (s *Streamer) ListAnchors(ctx context.Context, start bs.Anchor) (<-chan bs.Anchor, func() error, error) {
	return s.s.ListAnchors(ctx, start)
}

func (s *Streamer) ListAnchorRefs(ctx context.Context, a bs.Anchor) (<-chan bs.TimeRef, func() error, error) {
	return s.s.ListAnchorRefs(ctx, a)
}

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

func (s *Streamer) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, t time.Time) error {
	err := s.s.PutAnchor(ctx, ref, a, t)
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
