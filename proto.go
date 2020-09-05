package bs

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// GetProto reads a blob from a blob store and parses it into the given protocol buffer.
func GetProto(ctx context.Context, g Getter, ref Ref, m proto.Message) error {
	// TODO: check type info?
	b, _, err := g.Get(ctx, ref)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, m)
}

// PutProto serializes m and stores it as a typed blob in s,
// where the type is Type(m).
// See Type for more information.
//
// Additionally, it stores Type(m) as a blob.
//
// The boolean result tells only whether m's blob was newly added,
// not m's type blob.
func PutProto(ctx context.Context, s Store, m proto.Message) (Ref, bool, error) {
	typeProto := Type(m)

	var typeRef Ref
	if _, ok := m.(*descriptorpb.DescriptorProto); ok {
		typeRef = TypeTypeRef
	} else {
		var err error

		typeRef, _, err = PutProto(ctx, s, typeProto)
		if err != nil {
			return Ref{}, false, errors.Wrap(err, "storing protobuf type")
		}
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, false, errors.Wrap(err, "marshaling protobuf")
	}

	return s.Put(ctx, b, &typeRef)
}

// Type computes the type protobuf of a given protobuf.
// This is the protobuf's "descriptor,"
// converted to its own protobuf.
// See https://godoc.org/google.golang.org/protobuf/reflect/protoreflect#hdr-Protocol_Buffer_Descriptors.
func Type(m proto.Message) proto.Message {
	return protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())
}

// TypeBlob computes the type blob of a given protobuf.
// This is the marshaled Type().
func TypeBlob(m proto.Message) (Blob, error) {
	t := Type(m)
	b, err := proto.Marshal(t)
	return Blob(b), err
}

// TypeRef computes the type ref of a given protobuf.
// This is the ref of the TypeBlob().
func TypeRef(m proto.Message) (Ref, error) {
	b, err := TypeBlob(m)
	if err != nil {
		return Ref{}, err
	}
	return b.Ref(), nil
}

// ProtoRef is a convenience function for computing the ref of a serialized protobuf.
func ProtoRef(m proto.Message) (Ref, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, err
	}
	return Blob(b).Ref(), nil
}

var (
	// TypeTypeRef is the metatype ref: the ref of the type blob of type blobs.
	TypeTypeRef Ref

	// TypeTypeBlob is the metatype blob: the type blob of type blobs.
	TypeTypeBlob Blob
)

func init() {
	t := Type(&descriptorpb.DescriptorProto{})

	var err error
	TypeTypeBlob, err = TypeBlob(t)
	if err != nil {
		panic(err)
	}

	TypeTypeRef = TypeTypeBlob.Ref()
}

// ErrNoType means no type information was available for the ref in a call to DynGetProto.
var ErrNoType = errors.New("no type")

// DynGetProto retrieves the blob at ref with Get,
// constructs a new *dynamicpb.Message from its type,
// and loads the blob into it.
// This serializes the same as the original protobuf
// but is not convertible (or type-assertable) to the original protobuf's Go type.
//
// This is EXPERIMENTAL and relies on a hack.
// (It's also unclear what utility it has, but at least it's interesting.)
// See: https://groups.google.com/forum/?utm_medium=email&utm_source=footer#!msg/protobuf/xRWSIyQ3Qyg/YcuGve18BAAJ.
func DynGetProto(ctx context.Context, g Getter, ref Ref) (*dynamicpb.Message, error) {
	b, types, err := g.Get(ctx, ref)
	if err != nil {
		return nil, errors.Wrapf(err, "getting %s", ref)
	}

	if len(types) == 0 {
		return nil, ErrNoType
	}

	typ := types[0]

	var dp descriptorpb.DescriptorProto
	err = GetProto(ctx, g, typ, &dp)
	if err != nil {
		return nil, errors.Wrapf(err, "getting descriptor proto at %s", typ)
	}

	md, err := descriptorProtoToMessageDescriptor(&dp)
	if err != nil {
		return nil, errors.Wrapf(err, "manifesting descriptor proto at %s", typ)
	}

	dm := dynamicpb.NewMessage(md)
	err = proto.Unmarshal(b, dm)
	return dm, errors.Wrapf(err, "unmarshaling %s into protobuf manifested from descriptor proto at %s", ref, typ)
}

func descriptorProtoToMessageDescriptor(dp *descriptorpb.DescriptorProto) (protoreflect.MessageDescriptor, error) {
	name := "x"
	f, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{Name: &name, MessageType: []*descriptorpb.DescriptorProto{dp}}}})
	if err != nil {
		return nil, errors.Wrap(err, "creating Files object")
	}
	if n := f.NumFiles(); n != 1 {
		return nil, fmt.Errorf("created Files object has %d files (want 1)", n)
	}

	var md protoreflect.MessageDescriptor
	f.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		mds := fd.Messages()
		if n := mds.Len(); n != 1 {
			err = fmt.Errorf("got %d messages in created Files object (want 1)", n)
			return false
		}
		md = mds.Get(0)
		return true
	})

	return md, err
}
