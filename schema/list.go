package schema

import (
	"context"

	"github.com/bobg/bs"
)

func LoadList(ctx context.Context, g bs.Getter, ref bs.Ref) (*List, error) {
	var l List
	err := bs.GetProto(ctx, g, ref, &l)
	return &l, err
}

func ListFromRefs(ctx context.Context, s bs.Store, refs []bs.Ref) (*List, bs.Ref, error) {
	l := &List{Members: make([][]byte, 0, len(refs))}
	for _, ref := range refs {
		l.Members = append(l.Members, ref[:])
	}
	ref, _, err := bs.PutProto(ctx, s, l)
	return l, ref, err
}
