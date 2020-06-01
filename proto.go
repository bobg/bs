package bs

import (
	"context"

	"google.golang.org/protobuf/proto"
)

func PutProto(ctx context.Context, s Store, p proto.Message) (ref Ref, added bool, err error) {
	m, err := proto.Marshal(p)
	if err != nil {
		return Zero, false, err
	}
	return s.Put(ctx, m)
}

func GetProto(ctx context.Context, g Getter, ref Ref, p proto.Message) error {
	b, err := g.Get(ctx, ref)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, p)
}
