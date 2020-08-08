package bs

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
)

// GetProto reads a blob from a blob store and parses it into the given protocol buffer.
func GetProto(ctx context.Context, g Getter, ref Ref, p proto.Message) error {
	b, err := g.Get(ctx, ref)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b.Blob, p)
}

// Type produces the type of a protobuf message (its descriptor) as a Blob.
func Type(m proto.Message) (Blob, error) {
	return proto.Marshal(protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor()))
}
