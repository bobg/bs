package gcs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/bobg/bs"
)

var _ bs.Store = &Store{}

// Store is a Google Cloud Storage-based implementation of a blob store.
type Store struct {
	anchors *storage.BucketHandle
	blobs   *storage.BucketHandle
}

// New produces a new Store with blobs and anchors in the given buckets.
func New(ctx context.Context, client *storage.Client, blobBucket, anchorBucket string) *Store {
	return &Store{
		anchors: client.Bucket(anchorBucket),
		blobs:   client.Bucket(blobBucket),
	}
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	obj := s.blobs.Object(ref.String())
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	b := make([]byte, r.Size())
	_, err = r.Read(b)
	return b, err
}

// GetMulti gets multiple blobs in one call.
func (s *Store) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	result := make(bs.GetMultiResult)
	for _, ref := range refs {
		var (
			ref = ref
			ch  = make(chan struct{})
			b   []byte
			err error
		)
		go func() {
			b, err = s.Get(ctx, ref)
			close(ch)
		}()
		result[ref] = func(_ context.Context) (bs.Blob, error) {
			<-ch
			return b, err
		}
	}
	return result, nil
}

// GetAnchor gets the latest blob ref for a given anchor as of a given time.
func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	pairs, err := s.anchorPairs(ctx, a)
	if err != nil {
		return bs.Zero, err
	}
	return bs.FindAnchor(pairs, at)
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref := b.Ref()
	obj := s.blobs.Object(ref.String()).If(storage.Conditions{DoesNotExist: true})
	w := obj.NewWriter(ctx)
	_, err := w.Write(b) // TODO: are partial writes a possibility?
	if e, ok := err.(*googleapi.Error); ok && e.Code == 412 {
		return ref, false, nil
	}
	if err != nil {
		return ref, false, err
	}
	err = w.Close()
	return ref, true, err
}

// PutMulti adds multiple blobs to the store in one call.
func (s *Store) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	result := make(bs.PutMultiResult, len(blobs))
	for i, b := range blobs {
		var (
			i     = i
			b     = b
			ch    = make(chan struct{})
			ref   bs.Ref
			added bool
			err   error
		)
		go func() {
			ref, added, err = s.Put(ctx, b)
			close(ch)
		}()
		result[i] = func(_ context.Context) (bs.Ref, bool, error) {
			<-ch
			return ref, added, err
		}
	}
	return result, nil
}

// PutAnchor adds a new ref for a given anchor as of a given time.
// TODO: Need a different model for anchors.
// Apart from the retry loop needed here,
// an anchor with a lot of writes will get slower and slower without bound.
func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	var (
		name  = anchorObjName(a)
		obj   = s.anchors.Object(name)
		bkoff = backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	)

	// In case of multiple writers,
	// retry until we can read and then write an anchor
	// without the Generation changing in between.
	return backoff.Retry(
		func() error {
			attrs, err := obj.Attrs(ctx)
			if err != nil {
				return backoff.Permanent(err)
			}

			r, err := obj.NewReader(ctx)
			if err != nil {
				return backoff.Permanent(err)
			}

			var pairs []bs.TimeRef
			dec := json.NewDecoder(r)
			err = dec.Decode(&pairs)
			if err != nil {
				return backoff.Permanent(err)
			}

			pairs = append(pairs, bs.TimeRef{T: at, R: ref})
			sort.Slice(pairs, func(i, j int) bool {
				return pairs[i].T.Before(pairs[j].T)
			})

			obj = obj.If(storage.Conditions{GenerationMatch: attrs.Generation})
			w := obj.NewWriter(ctx)
			enc := json.NewEncoder(w)
			err = enc.Encode(pairs)
			if e, ok := err.(*googleapi.Error); ok && e.Code == 412 {
				// This is the retryable error.
				return e
			}
			if err != nil {
				return backoff.Permanent(err)
			}
			err = w.Close()
			if err != nil {
				return backoff.Permanent(err)
			}
			return nil
		},
		bkoff,
	)
}

// ListRefs produces all blob refs in the store, in lexical order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref) (<-chan bs.Ref, func() error, error) {
	var (
		ch = make(chan bs.Ref)
		g  errgroup.Group
	)
	g.Go(func() error {
		defer close(ch)

		// Google Cloud Storage iterators have no API for starting in the middle of a bucket.
		// But they can filter by object-name prefix.
		// So we take (the hex encoding of) `start` and repeatedly compute prefixes for the objects we want.
		// If `start` is e67a, for example, the sequence of generated prefixes is:
		//   e67b e67c e67d e67e e67f
		//   e68 e69 e6a e6b e6c e6d e6e e6f
		//   e7 e8 e9 ea eb ec ed ee ef
		//   f
		for prefix := range hexPrefixes(ctx, start.String()) {
			err := s.listRefs(ctx, prefix, ch)
			if err != nil {
				return err
			}
		}
		return ctx.Err() // in case the channel from hexPrefixes closed due to context cancellation
	})

	return ch, g.Wait, nil
}

func (s *Store) listRefs(ctx context.Context, prefix string, ch chan<- bs.Ref) error {
	iter := s.blobs.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		obj, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		ref, err := bs.RefFromHex(obj.Name)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- ref:
			// do nothing
		}
	}
}

// ListAnchors lists all anchors in the store, in lexical order.
func (s *Store) ListAnchors(ctx context.Context, start bs.Anchor) (<-chan bs.Anchor, func() error, error) {
	var (
		ch = make(chan bs.Anchor)
		g  errgroup.Group
	)
	g.Go(func() error {
		defer close(ch)

		for prefix := range hexPrefixes(ctx, anchorObjName(start)) {
			err := s.listAnchors(ctx, prefix, ch)
			if err != nil {
				return err
			}
		}
		return ctx.Err() // in case the channel from hexPrefixes closed due to context cancellation
	})
	return ch, g.Wait, nil
}

func (s *Store) listAnchors(ctx context.Context, prefix string, ch chan<- bs.Anchor) error {
	iter := s.anchors.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		obj, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		anchor, err := hex.DecodeString(obj.Name)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- bs.Anchor(anchor):
			// do nothing
		}
	}
}

// ListAnchorRefs lists all blob refs for a given anchor,
// together with their timestamps,
// in chronological order.
func (s *Store) ListAnchorRefs(ctx context.Context, a bs.Anchor) (<-chan bs.TimeRef, func() error, error) {
	var (
		ch = make(chan bs.TimeRef)
		g  errgroup.Group
	)
	g.Go(func() error {
		pairs, err := s.anchorPairs(ctx, a)
		if err != nil {
			return err
		}
		for _, pair := range pairs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- pair:
				// do nothing
			}
		}
		return nil
	})
	return ch, g.Wait, nil
}

func (s *Store) anchorPairs(ctx context.Context, a bs.Anchor) ([]bs.TimeRef, error) {
	name := anchorObjName(a)
	obj := s.anchors.Object(name)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	var pairs []bs.TimeRef
	dec := json.NewDecoder(r)
	err = dec.Decode(&pairs)
	return pairs, err
}

func anchorObjName(a bs.Anchor) string {
	return hex.EncodeToString([]byte(a))
}

func hexPrefixes(ctx context.Context, after string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)

		for len(after) > 0 {
			end := after[len(after)-1:][0]
			after = after[:len(after)-1]

			for c := end + 1; c <= 'f'; c++ { // assumes that `after` is lowercase!
				select {
				case <-ctx.Done():
					return
				case ch <- after + string(c):
					// do nothing
				}
			}
		}
	}()
	return ch
}
