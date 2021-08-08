package schema

import (
	"context"

	"github.com/bobg/bs"
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
