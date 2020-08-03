// Package bt implements a blob store on Google Cloud BigTable.
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

// Store is a Google Cloud Bigtable-backed implementation of bs.Store.
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

// New produces a new Store.
func New(t *bigtable.Table) *Store {
	return &Store{t: t}
}

// Get implements bs.Store.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	row, err := s.t.ReadRow(ctx, blobKey(ref))
	if err != nil {
		return nil, errors.Wrapf(err, "reading row %s", ref)
	}
	items := row[blobfam]
	if len(items) == 0 {
		return nil, errEmptyItems
	}
	return bs.Blob(items[0].Value), nil
}

// GetMulti implements bs.Store.
func (s *Store) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	rowKeys := make(bigtable.RowList, len(refs))
	for i, ref := range refs {
		rowKeys[i] = blobKey(ref)
	}

	result := make(bs.GetMultiResult)

	err := s.t.ReadRows(ctx, rowKeys, func(row bigtable.Row) bool {
		key := row.Key()
		ref, err := refFromKey(key)
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

// GetAnchor implements bs.Store.
func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, when time.Time) (bs.Ref, time.Time, error) {
	var (
		found     *bs.Ref
		foundTime time.Time
		innerErr  error
	)

	// Anchors come back in reverse chronological order
	// (since we usually want the latest one).
	// Find the first one whose timestamp is `when` or earlier.
	err := s.eachAnchorRow(ctx, a, func(row bigtable.Row) bool {
		key := row.Key()
		_, atime, err := anchorTimeFromKey(key)
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
		foundTime = atime
		return false
	})
	if err != nil {
		return bs.Ref{}, time.Time{}, errors.Wrap(err, "iterating over anchor rows")
	}
	if innerErr != nil {
		return bs.Ref{}, time.Time{}, errors.Wrap(innerErr, "processing anchor row")
	}
	if found == nil {
		return bs.Ref{}, time.Time{}, bs.ErrNotFound
	}
	return *found, foundTime, nil
}

// ListRefs implements bs.Store.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	var innerErr error
	rowFn := func(row bigtable.Row) bool {
		key := row.Key()
		ref, err := refFromKey(key)
		if err != nil {
			innerErr = errors.Wrapf(err, "extracting ref from key %s", key)
			return false
		}
		err = f(ref)
		if err != nil {
			innerErr = err
			return false
		}
		return true
	}
	startKey := blobKey(start) + "0"
	filter := bigtable.RowKeyFilter("^b:") // blobs only
	err := s.t.ReadRows(ctx, bigtable.InfiniteRange(startKey), rowFn, bigtable.RowFilter(filter))
	if err != nil {
		return err
	}
	return innerErr
}

// ListAnchors implements bs.Store.
func (s *Store) ListAnchors(ctx context.Context, start bs.Anchor, f func(bs.Anchor, bs.TimeRef) error) error {
	var (
		lastAnchor bs.Anchor
		timeRefs   []bs.TimeRef
		innerErr   error
	)

	dump := func() error {
		for i := len(timeRefs) - 1; i >= 0; i-- {
			err := f(lastAnchor, timeRefs[i])
			if err != nil {
				return err
			}
		}
		timeRefs = nil
		return nil
	}

	rowFn := func(row bigtable.Row) bool {
		key := row.Key()
		a, atime, err := anchorTimeFromKey(key)
		if err != nil {
			innerErr = errors.Wrapf(err, "parsing anchor/time from key %s", key)
			return false
		}
		if a != lastAnchor {
			err = dump()
			if err != nil {
				innerErr = err
				return false
			}
			lastAnchor = a
		}
		items := row[anchorfam]
		if len(items) == 0 {
			innerErr = errEmptyItems
			return false
		}
		ref := bs.RefFromBytes(items[0].Value)
		timeRefs = append(timeRefs, bs.TimeRef{T: atime, R: ref})
		return true
	}

	startKey := anchorKey(start, maxTime)
	filter := bigtable.RowKeyFilter("^a:") // anchors only
	err := s.t.ReadRows(ctx, bigtable.InfiniteRange(startKey), rowFn, bigtable.RowFilter(filter))
	if err != nil {
		return err
	}
	if innerErr != nil {
		return innerErr
	}
	return dump()
}

func (s *Store) eachAnchorRow(ctx context.Context, a bs.Anchor, f func(bigtable.Row) bool) error {
	filter := bigtable.RowKeyFilter(fmt.Sprintf("^a:%s:\\d{20}$", regexp.QuoteMeta(string(a))))
	return s.t.ReadRows(ctx, anchorRange(a), f, bigtable.RowFilter(filter))
}

// Put implements bs.Store.
func (s *Store) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
	mut := bigtable.NewMutation()
	mut.Set(blobfam, blobcol, bigtable.Now(), blob)

	cmut := bigtable.NewCondMutation(bigtable.LatestNFilter(1), nil, mut)

	var alreadyPresent bool
	ref := blob.Ref()
	err := s.t.Apply(ctx, blobKey(ref), cmut, bigtable.GetCondMutationResult(&alreadyPresent))
	return ref, !alreadyPresent, err
}

// PutMulti implements bs.Store.
func (s *Store) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	// "Conditional mutations cannot be applied in bulk and providing one will result in an error."
	return bs.PutMulti(ctx, s, blobs)
}

// PutAnchor implements bs.Store.
func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, when time.Time) error {
	mut := bigtable.NewMutation()
	mut.Set(anchorfam, anchorcol, bigtable.Time(when), ref[:])
	return s.t.Apply(ctx, anchorKey(a, when), mut)
}

func blobKey(ref bs.Ref) string {
	return fmt.Sprintf("b:%x", ref[:])
}

func refFromKey(key string) (bs.Ref, error) {
	return bs.RefFromHex(key[2:])
}

func anchorKey(a bs.Anchor, when time.Time) string {
	return fmt.Sprintf("a:%s:%020d", a, maxTime.Sub(when))
}

func anchorRange(a bs.Anchor) bigtable.RowSet {
	return bigtable.PrefixRange("a:" + string(a) + ":")
}

var anchorKeyRegex = regexp.MustCompile(`^a:(.+):(\d{20})$`)

func anchorTimeFromKey(key string) (bs.Anchor, time.Time, error) {
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

		creds, ok := conf["creds"].(string)
		if !ok {
			return nil, errors.New(`missing "creds" parameter`)
		}
		options = append(options, option.WithCredentialsFile(creds))
		c, err := bigtable.NewClient(ctx, project, instance, options...)
		if err != nil {
			return nil, errors.Wrap(err, "creating bigtable client")
		}
		t := c.Open(table)
		return New(t), nil
	})
}
