package anchor

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
)

type Getter interface {
	// GetAnchor returns the latest ref associated with the given anchor
	// not later than the given time.
	GetAnchor(context.Context, string, time.Time) (bs.Ref, error)
}

// Store is a blob store that can also store and retrieve anchors.
//
// Implementation note: the Put method (from bs.Store)
// must detect the case where the type ref is non-nil and equal to TypeRef().
// In that case, the implementation must unmarshal the blob as an Anchor
// in order to record it.
// If Put is called on an Anchor-typed blob that is already present in the store,
// it is free to assume that that anchor already has been recorded.
type Store interface {
	bs.Store
	Getter
}

func Init(ctx context.Context, s Store) error {
	err := bs.Init(ctx, s)
	if err != nil {
		return errors.Wrap(err, "calling bs.Init")
	}
	b, err := bs.TypeBlob(&Anchor{})
	if err != nil {
		return errors.Wrap(err, "computing Anchor type blob")
	}
	tr := TypeRef()
	_, _, err = s.Put(ctx, b, &tr)
	return errors.Wrap(err, "storing Anchor type")
}

func Put(ctx context.Context, s bs.Store, name string, ref bs.Ref, at time.Time) (bs.Ref, bool, error) {
	return bs.PutProto(ctx, s, &Anchor{Name: name, Ref: ref[:], At: timestamppb.New(at)})
}

var typeRef *bs.Ref

func TypeRef() bs.Ref {
	if typeRef == nil {
		tr, err := bs.TypeRef(&Anchor{})
		if err != nil {
			panic(err)
		}
		typeRef = &tr
	}
	return *typeRef
}
