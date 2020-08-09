package anchor

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
)

type Getter interface {
	// GetAnchor returns the ref associated with the given anchor
	// at or after the given time.
	GetAnchor(context.Context, string, time.Time) (bs.Ref, error)
}

type Store interface {
	bs.Store
	Getter
}

var TypeRef bs.Ref

func Init(ctx context.Context, s Store) error {
	err := bs.Init(ctx, s)
	if err != nil {
		return errors.Wrap(err, "calling bs.Init")
	}
	b, err := bs.TypeBlob(&Anchor{})
	if err != nil {
		return errors.Wrap(err, "computing Anchor type blob")
	}
	_, _, err = s.Put(ctx, b, &TypeRef)
	return errors.Wrap(err, "storing Anchor type")
}

func init() {
	var err error
	TypeRef, err = bs.TypeRef(&Anchor{})
	if err != nil {
		panic(err)
	}
}
