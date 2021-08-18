package schema

import (
	"context"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/gc"
)

// LoadList loads the List at ref from g.
func LoadList(ctx context.Context, g bs.Getter, ref bs.Ref) (*List, error) {
	var l List
	err := bs.GetProto(ctx, g, ref, &l)
	return &l, err
}

// ListFromRefs creates a new list whose members are the given refs,
// and stores the resulting list in s.
// It returns the List and the ref at which it is stored.
func ListFromRefs(ctx context.Context, s bs.Store, refs []bs.Ref) (*List, bs.Ref, error) {
	l := &List{Members: make([][]byte, 0, len(refs))}
	for _, ref := range refs {
		l.Members = append(l.Members, ref[:])
	}
	ref, _, err := bs.PutProto(ctx, s, l)
	return l, ref, err
}

// ProtectList returns a gc.ProtectFunc that protects the list and its member refs from garbage collection.
// The function f is the ProtectFunc for the members of the list.
func ProtectList(f gc.ProtectFunc) gc.ProtectFunc {
	return func(ctx context.Context, g bs.Getter, ref bs.Ref) ([]gc.ProtectPair, error) {
		var (
			list   List
			result []gc.ProtectPair
		)
		err := bs.GetProto(ctx, g, ref, &list)
		if err != nil {
			return nil, errors.Wrap(err, "loading list")
		}
		for _, member := range list.Members {
			result = append(result, gc.ProtectPair{Ref: bs.RefFromBytes(member), F: f})
		}
		return result, nil
	}
}
