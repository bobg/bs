package gcs

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/api/googleapi"

	"github.com/bobg/bs"
)

var _ bs.Store = &Store{}

type Store struct {
	client  *storage.Client
	anchors *storage.BucketHandle
	blobs   *storage.BucketHandle
}

func New(ctx context.Context, client *storage.Client, blobBucket, anchorBucket string) *Store {
	return &Store{
		client:  client,
		anchors: client.Bucket(anchorBucket),
		blobs:   client.Bucket(blobBucket),
	}
}

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

func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	name := anchorObjName(a)
	obj := s.anchors.Object(name)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return bs.Zero, err
	}

	var pairs []bs.TimeRef
	dec := json.NewDecoder(r)
	err = dec.Decode(&pairs)
	if err != nil {
		return bs.Zero, err
	}

	return bs.FindAnchor(pairs, at)
}

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

// TODO: Perform whatever escaping is needed here. Or maybe just hashing?
func anchorObjName(a bs.Anchor) string {
	return string(a)
}
