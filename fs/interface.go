package fs

import (
	"context"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

// In this file, we implement various interfaces from the Go stdlib's io/fs package.

var (
	_ fs.FS         = (*FS)(nil)
	_ fs.ReadDirFS  = (*FS)(nil)
	_ fs.StatFS     = (*FS)(nil)
	_ fs.File       = (*fsFile)(nil)
	_ io.ReadSeeker = (*fsFile)(nil)
	_ fs.FileInfo   = (*fsFileInfo)(nil)
	_ fs.DirEntry   = (*fsDirEntry)(nil)
)

// FS implements io/fs.FS.
type FS struct {
	Ctx context.Context

	g       bs.Getter
	rootRef bs.Ref
	rootDir *Dir // cached
}

// NewFS created a new *FS reading from the given bs.Getter and rooted at the given ref.
// The given context object is stored in the FS and used in subsequent calls to Open, Stat, ReadDir, etc.
// This is an antipattern but acceptable when an object must adhere to a context-free stdlib interface
// (https://github.com/golang/go/wiki/CodeReviewComments#contexts).
// Callers may replace the context object during the lifetime of the FS as needed.
func NewFS(ctx context.Context, g bs.Getter, rootRef bs.Ref) *FS {
	return &FS{
		Ctx:     ctx,
		g:       g,
		rootRef: rootRef,
	}
}

// Open opens the file at the given path in FS.
//
// TODO: This presently handles only plain files,
// but should handle directories and symlinks as well.
//
// TODO: Symlinks in the given path should be traversed but are not at the moment.
func (f *FS) Open(name string) (result fs.File, err error) {
	defer func() {
		if err != nil {
			err = &fs.PathError{
				Op:   "open",
				Path: name,
				Err:  err,
			}
		}
	}()

	err = f.ensureRoot(name)
	if err != nil {
		return nil, err
	}
	dirent, err := f.rootDir.Find(f.Ctx, f.g, name)
	if err != nil {
		return nil, errors.Wrapf(err, "finding %s", name)
	}
	if dirent.IsDir() {
		return nil, errors.Wrapf(err, "%s is a directory", name)
	}
	if dirent.IsLink() {
		return nil, errors.Wrapf(err, "%s is a symlink", name)
	}
	fileRef, err := bs.RefFromHex(dirent.Item)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing ref %s in dirent %s", dirent.Item, name)
	}
	r, err := split.NewReader(f.Ctx, f.g, fileRef)
	if err != nil {
		return nil, errors.Wrapf(err, "creating split.Reader for file ref %s", name)
	}
	return &fsFile{
		name: filepath.Base(name),
		size: int64(r.Size()),
		r:    r,
	}, nil
}

// ReadDir implements io/fs.ReadDirFS.
func (f *FS) ReadDir(name string) (result []fs.DirEntry, err error) {
	defer func() {
		if err != nil {
			err = &fs.PathError{
				Op:   "readdir",
				Path: name,
				Err:  err,
			}
		}
	}()

	err = f.ensureRoot(name)
	if err != nil {
		return nil, err
	}
	dirent, err := f.rootDir.Find(f.Ctx, f.g, name)
	if err != nil {
		return nil, errors.Wrapf(err, "finding %s", name)
	}
	dir, err := dirent.Dir(f.Ctx, f.g)
	if err != nil {
		return nil, errors.Wrapf(err, "reading dir at %s", name)
	}
	err = dir.Each(f.Ctx, f.g, func(entryname string, entry *Dirent) error {
		size, err := entry.Size(f.Ctx, f.g)
		if err != nil {
			return errors.Wrapf(err, "getting size of %s", entryname)
		}
		result = append(result, &fsDirEntry{
			name:   entryname,
			dirent: entry,
			size:   size,
		})
		return nil
	})
	return result, errors.Wrapf(err, "iterating over entries at %s", name)
}

// Stat implements io/fs.StatFS.
func (f *FS) Stat(name string) (result fs.FileInfo, err error) {
	defer func() {
		if err != nil {
			err = &fs.PathError{
				Op:   "stat",
				Path: name,
				Err:  err,
			}
		}
	}()

	err = f.ensureRoot(name)
	if err != nil {
		return nil, err
	}
	dirent, err := f.rootDir.Find(f.Ctx, f.g, name)
	if err != nil {
		return nil, errors.Wrapf(err, "finding %s", name)
	}
	size, err := dirent.Size(f.Ctx, f.g)
	return &fsFileInfo{
		name: filepath.Base(name),
		mode: fs.FileMode(dirent.Mode),
		size: size,
	}, errors.Wrapf(err, "getting size of %s", name)
}

func (f *FS) ensureRoot(name string) error {
	if f.rootDir != nil {
		return nil
	}
	var d Dir
	err := d.Load(f.Ctx, f.g, f.rootRef)
	if err != nil {
		return errors.Wrap(err, "loading root")
	}
	f.rootDir = &d
	return nil
}

// fsFile implements io/fs.File.
type fsFile struct {
	name string
	mode fs.FileMode
	size int64
	r    *split.Reader
}

func (f *fsFile) Stat() (fs.FileInfo, error) {
	return &fsFileInfo{
		name: f.name,
		mode: f.mode,
		size: f.size,
	}, nil
}

func (f *fsFile) Read(buf []byte) (int, error) {
	return f.r.Read(buf)
}

func (f *fsFile) Seek(offset int64, whence int) (int64, error) {
	return f.r.Seek(offset, whence)
}

func (f *fsFile) Close() error {
	return nil
}

type fsFileInfo struct {
	name string
	mode fs.FileMode
	size int64
}

func (info *fsFileInfo) Name() string       { return info.name }
func (info *fsFileInfo) Size() int64        { return info.size }
func (info *fsFileInfo) Mode() fs.FileMode  { return info.mode }
func (info *fsFileInfo) ModTime() time.Time { return time.Time{} }
func (info *fsFileInfo) IsDir() bool        { return info.mode.IsDir() }
func (info *fsFileInfo) Sys() interface{}   { return nil }

type fsDirEntry struct {
	name   string
	dirent *Dirent
	size   int64
}

func (e *fsDirEntry) Name() string      { return e.name }
func (e *fsDirEntry) IsDir() bool       { return e.dirent.IsDir() }
func (e *fsDirEntry) Type() fs.FileMode { return fs.FileMode(e.dirent.Mode).Type() }
func (e *fsDirEntry) Info() (fs.FileInfo, error) {
	return &fsFileInfo{
		name: e.name,
		mode: fs.FileMode(e.dirent.Mode),
		size: e.size,
	}, nil
}
