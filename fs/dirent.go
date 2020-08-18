package fs

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs/anchor"
)

func (d *Dirent) IsDir() bool {
	return os.FileMode(d.Mode).IsDir()
}

func (d *Dirent) Dir(ctx context.Context, g anchor.Getter, at time.Time) (*Dir, error) {
	if !d.IsDir() {
		return nil, errors.New("not a directory")
	}
	dref, err := g.GetAnchor(ctx, d.Item, at)
	if err != nil {
		return nil, errors.Wrapf(err, "getting anchor %s at %s", d.Item, at)
	}
	var dir Dir
	err = dir.Load(ctx, g, dref)
	return &dir, errors.Wrapf(err, "getting directory at %s", dref)
}
