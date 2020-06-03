package file

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"hash/adler32"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/bobg/bs"
)

var _ bs.Store = &Store{}

type Store struct {
	root string
}

func New(root string) *Store {
	return &Store{root: root}
}

func (s *Store) blobpath(ref bs.Ref) string {
	h := hex.EncodeToString(ref[:])
	return filepath.Join(s.root, "blobs", h[:2], h[:4], h)
}

func (s *Store) anchorpath(a bs.Anchor) string {
	sum := adler32.Checksum([]byte(a))
	var sumbytes [4]byte
	binary.BigEndian.PutUint32(sumbytes[:], sum)
	sumhex := hex.EncodeToString(sumbytes[:])
	return filepath.Join(s.root, "anchors", sumhex[:2], sumhex, url.PathEscape(string(a)))
}

func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, error) {
	b, err := ioutil.ReadFile(s.blobpath(ref))
	if os.IsNotExist(err) {
		return nil, bs.ErrNotFound
	}
	return b, err
}

func (s *Store) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	return bs.GetMulti(ctx, s, refs)
}

func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	dir := s.anchorpath(a)
	entries, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return bs.Zero, bs.ErrNotFound
	}
	if err != nil {
		return bs.Zero, err
	}

	// We might use sort.Search here (since ReadDir returns entries sorted by name),
	// which is O(log N),
	// but we want to be robust in the face of filenames that time.Parse fails to parse,
	// so O(N) it is.
	var best string
	for _, entry := range entries {
		name := entry.Name()
		parsed, err := time.Parse(time.RFC3339Nano, name)
		if err != nil {
			continue
		}
		if parsed.After(at) {
			break
		}
		best = name
	}
	if best == "" {
		return bs.Zero, bs.ErrNotFound
	}

	h, err := ioutil.ReadFile(filepath.Join(dir, best))
	if err != nil {
		return bs.Zero, err
	}

	var ref bs.Ref
	_, err = hex.Decode(ref[:], h)
	return ref, err
}

func (s *Store) Put(_ context.Context, b bs.Blob) (bs.Ref, bool, error) {
	var (
		ref  = b.Ref()
		path = s.blobpath(ref)
		dir  = filepath.Dir(path)
	)

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return ref, false, err
	}

	_, err = os.Stat(path)
	if !os.IsNotExist(err) {
		return ref, false, err
	}

	err = ioutil.WriteFile(path, b, 0444)
	return ref, true, err
}

func (s *Store) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	return bs.PutMulti(ctx, s, blobs)
}

func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	dir := s.anchorpath(a)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(
		filepath.Join(dir, at.Format(time.RFC3339Nano)),
		[]byte(hex.EncodeToString(ref[:])),
		0644,
	)
}
