package fs

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

// IsDir tells whether d refers to a directory.
func (d *Dirent) IsDir() bool {
	return os.FileMode(d.Mode).IsDir()
}

// Dir returns the directory referred to by d.
// It is an error to call this when !d.IsDir().
func (d *Dirent) Dir(ctx context.Context, g bs.Getter) (*Dir, error) {
	if !d.IsDir() {
		return nil, errors.New("not a directory")
	}
	dref, err := bs.RefFromHex(d.Item)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing hex ref %s", d.Item)
	}
	var dir Dir
	err = dir.Load(ctx, g, dref)
	return &dir, errors.Wrapf(err, "getting directory at %s", dref)
}

// IsLink tells whether d refers to a symlink.
// If it does, then d.Item is the target of the link.
func (d *Dirent) IsLink() bool {
	return (d.Mode & uint32(os.ModeSymlink)) == uint32(os.ModeSymlink)
}

// Size returns the size of the file represented by the given entry.
// This is 0 for directories and symlinks.
// TODO: this should not be 0 for directories and symlinks.
func (d *Dirent) Size(ctx context.Context, g bs.Getter) (int64, error) {
	if d.IsDir() || d.IsLink() {
		return 0, nil
	}
	ref, err := bs.RefFromHex(d.Item)
	if err != nil {
		return 0, errors.Wrapf(err, "parsing ref %s", d.Item)
	}
	r, err := split.NewReader(ctx, g, ref)
	if err != nil {
		return 0, errors.Wrapf(err, "creating split.Reader for file ref %s", ref)
	}
	return int64(r.Size()), nil
}
