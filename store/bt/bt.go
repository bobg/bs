// Package bt implements a blob store on Google BigTable.
package bt

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

type Store struct {
	t *bigtable.Table
}

const (
	anchorcol = "anchor"
	anchorfam = "anchor"
	blobcol   = "blob"
	blobfam   = "blob"
)

var errEmptyItems = errors.New("empty items")

func New(t *bigtable.Table) *Store {
	return &Store{t: t}
}

func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	row, err := s.t.ReadRow(ctx, s.blobKey(ref))
	if err != nil {
		return nil, errors.Wrapf(err, "reading row %s", ref)
	}
	items := row[blobfam]
	if len(items) == 0 {
		return nil, errEmptyItems
	}
	return bs.Blob(items[0].Value), nil
}

func (s *Store) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	rowKeys := make(bigtable.RowList, len(refs))
	for i, ref := range refs {
		rowKeys[i] = s.blobKey(ref)
	}

	result := make(bs.GetMultiResult)

	err := s.t.ReadRows(ctx, rowKeys, func(row bigtable.Row) bool {
		key := row.Key()
		ref, err := s.refFromKey(key)
		if err != nil {
			result[ref] = func(context.Context) (bs.Blob, error) { return nil, err }
			return true
		}
		result[ref] = func(context.Context) (bs.Blob, error) {
			items := row[blobfam]
			if len(items) == 0 {
				return nil, errEmptyItems
			}
			return bs.Blob(items[0].Value), nil
		}
		return true
	})

	return result, err
}

func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, when time.Time) (bs.Ref, error) {
	var (
		found    *bs.Ref
		innerErr error
	)

	// Anchors come back in reverse chronological order
	// (since we usually want the latest one).
	// Find the first one whose timestamp is `when` or earlier.
	err := s.eachAnchorRow(ctx, a, func(row bigtable.Row) bool {
		key := row.Key()
		_, atime, err := s.anchorTimeFromKey(key)
		if err != nil {
			innerErr = errors.Wrapf(err, "parsing anchor/time from key %s", key)
			return false
		}
		if atime.After(when) {
			return true
		}
		items := row[anchorfam]
		if len(items) == 0 {
			innerErr = errEmptyItems
			return false
		}
		ref := bs.RefFromBytes(items[0].Value)
		found = &ref
		return false
	})
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "iterating over anchor rows")
	}
	if innerErr != nil {
		return bs.Ref{}, errors.Wrap(innerErr, "processing anchor row")
	}
	if found == nil {
		return bs.Ref{}, bs.ErrNotFound
	}
	return *found, nil
}

func (s *Store) ListRefs(ctx context.Context, start bs.Ref, ch chan<- bs.Ref) error {

	defer close(ch)

	var innerErr error
	rowFn := func(row bigtable.Row) bool {
		key := row.Key()
		ref, err := s.refFromKey(key)
		if err != nil {
			innerErr = errors.Wrapf(err, "extracting ref from key %s", key)
			return false
		}
		select {
		case <-ctx.Done():
			innerErr = ctx.Err()
			return false
		case ch <- ref:
			return true
		}
	}
	startKey := s.blobKey(start) + "0"
	filter := bigtable.RowKeyFilter("^b:") // blobs only
	err := s.t.ReadRows(ctx, bigtable.InfiniteRange(startKey), rowFn, bigtable.RowFilter(filter))
	if err != nil {
		return err
	}
	return innerErr
}

func (s *Store) ListAnchors(ctx context.Context, start bs.Anchor, ch chan<- bs.Anchor) error {
	defer close(ch)

	var (
		lastAnchor bs.Anchor
		innerErr   error
	)

	rowFn := func(row bigtable.Row) bool {
		key := row.Key()
		a, _, err := s.anchorTimeFromKey(key)
		if err != nil {
			innerErr = errors.Wrapf(err, "parsing anchor/time from key %s", key)
			return false
		}
		if a != lastAnchor {
			select {
			case <-ctx.Done():
				innerErr = ctx.Err()
				return false
			case ch <- a:
				lastAnchor = a
			}
		}
		return true
	}
	startKey := s.anchorKey(start, maxTime)
	filter := bigtable.RowKeyFilter("^a:") // anchors only
	err := s.t.ReadRows(ctx, bigtable.InfiniteRange(startKey), rowFn, bigtable.RowFilter(filter))
	if err != nil {
		return err
	}
	return innerErr
}

func (s *Store) ListAnchorRefs(ctx context.Context, a bs.Anchor, ch chan<- bs.TimeRef) error {
	defer close(ch)

	var (
		timeRefs []bs.TimeRef
		innerErr error
	)
	err := s.eachAnchorRow(ctx, a, func(row bigtable.Row) bool {
		key := row.Key()
		_, atime, err := s.anchorTimeFromKey(key)
		if err != nil {
			innerErr = errors.Wrapf(err, "parsing anchor/time from key %s", key)
			return false
		}
		items := row[anchorfam]
		if len(items) == 0 {
			innerErr = errEmptyItems
			return false
		}
		ref := bs.RefFromBytes(items[0].Value)
		timeRefs = append(timeRefs, bs.TimeRef{T: atime, R: ref})
		return true
	})
	if err != nil {
		return errors.Wrap(err, "iterating over anchor rows")
	}
	if innerErr != nil {
		return errors.Wrap(err, "processing anchor row")
	}

	for i := len(timeRefs) - 1; i >= 0; i-- {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ch <- timeRefs[i]:
			// ok
		}
	}

	return nil
}

func (s *Store) eachAnchorRow(ctx context.Context, a bs.Anchor, f func(bigtable.Row) bool) error {
	filter := bigtable.RowKeyFilter(fmt.Sprintf("^a:%s:\\d{20}$", regexp.QuoteMeta(string(a))))
	return s.t.ReadRows(ctx, s.anchorRange(a), f, bigtable.RowFilter(filter))
}

func (s *Store) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
	mut := bigtable.NewMutation()
	mut.Set(blobfam, blobcol, bigtable.Now(), blob)

	cmut := bigtable.NewCondMutation(bigtable.LatestNFilter(1), nil, mut)

	var alreadyPresent bool
	ref := blob.Ref()
	err := s.t.Apply(ctx, s.blobKey(ref), cmut, bigtable.GetCondMutationResult(&alreadyPresent))
	return ref, !alreadyPresent, err
}

func (s *Store) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	// "Conditional mutations cannot be applied in bulk and providing one will result in an error."
	return bs.PutMulti(ctx, s, blobs)
}

func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, when time.Time) error {
	mut := bigtable.NewMutation()
	mut.Set(anchorfam, anchorcol, bigtable.Time(when), ref[:])
	return s.t.Apply(ctx, s.anchorKey(a, when), mut)
}

func (s *Store) blobKey(ref bs.Ref) string {
	return fmt.Sprintf("b:%x", ref[:])
}

func (s *Store) refFromKey(key string) (bs.Ref, error) {
	return bs.RefFromHex(key[2:])
}

func (s *Store) anchorKey(a bs.Anchor, when time.Time) string {
	return fmt.Sprintf("a:%s:%020d", a, maxTime.Sub(when))
}

func (s *Store) anchorRange(a bs.Anchor) bigtable.RowSet {
	return bigtable.PrefixRange("a:" + string(a) + ":")
}

var anchorKeyRegex = regexp.MustCompile(`^a:(.+):(\d{20})$`)

func (s *Store) anchorTimeFromKey(key string) (bs.Anchor, time.Time, error) {
	m := anchorKeyRegex.FindStringSubmatch(key)
	if len(m) < 3 {
		return "", time.Time{}, errors.New("malformed key")
	}
	nanos, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "parsing int64")
	}
	return bs.Anchor(m[1]), maxTime.Add(time.Duration(-nanos)), nil
}

// This is from https://stackoverflow.com/a/32620397
var maxTime = time.Unix(1<<63-1-int64((1969*365+1969/4-1969/100+1969/400)*24*60*60), 999999999)

func init() {
	store.Register("bt", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
		project, ok := conf["project"].(string)
		if !ok {
			return nil, errors.New(`missing "project" parameter`)
		}
		instance, ok := conf["instance"].(string)
		if !ok {
			return nil, errors.New(`missing "instance" parameter`)
		}
		table, ok := conf["table"].(string)
		if !ok {
			return nil, errors.New(`missing "table" parameter`)
		}
		var options []option.ClientOption
		c, err := bigtable.NewClient(ctx, project, instance, options...)
		if err != nil {
			return nil, errors.Wrap(err, "creating bigtable client")
		}
		t := c.Open(table)
		return New(t), nil
	})
}
