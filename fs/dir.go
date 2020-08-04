// Package fs implements blob store structures for representing files and directories.
package fs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
	"github.com/bobg/bs/schema"
	"github.com/bobg/bs/split"
)

// Dir is a directory of files, symlinks, and subdirs.
// It is implemented as a schema.Map,
// with each key the name of the entry
// and each payload a serialized Dirent.
type Dir schema.Map

// NewDir produces a new, blank Dir,
// not yet written to a blob store.
func NewDir() *Dir {
	return (*Dir)(schema.NewMap())
}

// Load loads the directory at ref into d.
func (d *Dir) Load(ctx context.Context, g bs.Getter, ref bs.Ref) error {
	return bs.GetProto(ctx, g, ref, (*schema.Map)(d))
}

// Each calls a callback on each name/Dirent pair in d
// (in an indeterminate order).
// If the callback returns an error,
// Each exits with that error.
func (d *Dir) Each(ctx context.Context, g bs.Getter, f func(name string, dirent *Dirent) error) error {
	return (*schema.Map)(d).Each(ctx, g, func(pair *schema.MapPair) error {
		var dirent Dirent
		err := proto.Unmarshal(pair.Payload, &dirent)
		if err != nil {
			return err
		}
		return f(string(pair.Key), &dirent)
	})
}

// Ingest adds the directory hierarchy rooted at path to d.
// Each new file, dir, or symlink encountered gets a new "inode" anchor.
// It returns the possibly-updated Ref for d.
func (d *Dir) Ingest(ctx context.Context, store bs.Store, path string) (bs.Ref, error) {
	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "reading dir %s", path)
	}

	var dref bs.Ref

	for _, info := range infos {
		if info.IsDir() {
			subdir := (*Dir)(schema.NewMap())
			subdirRef, err := subdir.Ingest(ctx, store, filepath.Join(path, info.Name()))
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "ingesting subdir %s/%s", path, info.Name())
			}

			subdirAnchor := newAnchor()
			err = store.PutAnchor(ctx, subdirAnchor, time.Now(), subdirRef)
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "storing anchor for new dir %s/%s", path, info.Name())
			}

			dirent, err := proto.Marshal(&Dirent{
				Mode: uint32(info.Mode()),
				Item: string(subdirAnchor),
			})
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "marshaling dirent for new dir %s/%s", path, info.Name())
			}

			dref, _, err = (*schema.Map)(d).Set(ctx, store, []byte(info.Name()), dirent)
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "updating dir with new dir entry %s/%s", path, info.Name())
			}
		} else if (info.Mode() & os.ModeSymlink) == os.ModeSymlink {
			target, err := os.Readlink(filepath.Join(path, info.Name()))
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "reading symlink %s/%s", path, info.Name())
			}
			dref, _, err = (*schema.Map)(d).Set(ctx, store, []byte(info.Name()), []byte(target))
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "updating dir with new symlink entry %s/%s", path, info.Name())
			}
		} else if (info.Mode() & os.ModeType) != 0 {
			return bs.Ref{}, errors.Wrapf(err, "%s/%s has unsupported file type %v", path, info.Name(), info.Mode()&os.ModeType)
		} else {
			// Regular file.
			dref, err = d.ingestFile(ctx, store, path, info)
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "ingesting file %s/%s", path, info.Name())
			}
		}
	}

	if (dref == bs.Ref{}) {
		dref, err = bs.ProtoRef((*schema.Map)(d))
	}
	return dref, errors.Wrap(err, "computing self ref")
}

func (d *Dir) ingestFile(ctx context.Context, store bs.Store, dirpath string, info os.FileInfo) (bs.Ref, error) {
	name := info.Name()
	f, err := os.Open(filepath.Join(dirpath, name))
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "opening %s/%s", dirpath, name)
	}
	defer f.Close()

	fref, err := split.Write(ctx, store, f, nil)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "split-writing to store from %s/%s", dirpath, name)
	}

	fileAnchor := newAnchor()
	err = store.PutAnchor(ctx, fileAnchor, time.Now(), fref)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "storing new file anchor %s", fileAnchor)
	}

	dirent, err := proto.Marshal(&Dirent{
		Mode: uint32(info.Mode()),
		Item: string(fileAnchor),
	})
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "marshaling dirent proto")
	}

	dref, _, err := (*schema.Map)(d).Set(ctx, store, []byte(name), dirent)
	return dref, errors.Wrapf(err, "updating dir with file entry %s", name)
}

func newAnchor() bs.Anchor {
	var buf [32]byte
	rand.Read(buf[:])
	return bs.Anchor(hex.EncodeToString(buf[:]))
}
