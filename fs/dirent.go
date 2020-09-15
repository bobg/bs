package fs

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
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
