package proto

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/typed"
)

// Get reads a blob from a blob store and parses it into the given protocol buffer.
func Get(ctx context.Context, g bs.Getter, ref bs.Ref, m proto.Message) error {
	// TODO: check type info?
	b, err := g.Get(ctx, ref)
	if err != nil {
		return err
	}
	return gproto.Unmarshal(b, m)
}

// Put serializes m and stores it in s.
// If s is a typed.Store,
// Put additionally stores Type(m) as a type annotation for the blob.
func Put(ctx context.Context, s bs.Store, m proto.Message) (bs.Ref, bool, error) {
	b, err := gproto.Marshal(m)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "marshaling protobuf")
	}
	ref, added, err := s.Put(ctx, b)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "storing blob")
	}

	if ts, ok := s.(typed.Store); ok {
		typeProto := Type(m)
		var typeRef bs.Ref
		if _, ok := m.(*descriptorpb.DescriptorProto); ok {
			typeRef = TypeTypeRef
		} else {
			typeRef, _, err = Put(ctx, s, typeProto)
			if err != nil {
				return ref, added, errors.Wrap(err, "storing protobuf type")
			}
		}
		err = ts.PutType(ctx, ref, typeRef)
		if err != nil {
			return ref, added, errors.Wrapf(err, "storing type annotation for %s", ref)
		}
	}

	return ref, added, nil
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
func TypeBlob(m proto.Message) (bs.Blob, error) {
	t := Type(m)
	b, err := proto.Marshal(t)
	return bs.Blob(b), err
}

// TypeRef computes the type ref of a given protobuf.
// This is the ref of the TypeBlob().
func TypeRef(m proto.Message) (bs.Ref, error) {
	b, err := TypeBlob(m)
	if err != nil {
		return bs.Ref{}, err
	}
	return b.Ref(), nil
}

// Ref is a convenience function for computing the ref of a serialized protobuf.
func Ref(m proto.Message) (bs.Ref, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return bs.Ref{}, err
	}
	return bs.Blob(b).Ref(), nil
}

var (
	// TypeTypeRef is the metatype ref: the ref of the type blob of type blobs.
	TypeTypeRef bs.Ref

	// TypeTypeBlob is the metatype blob: the type blob of type blobs.
	TypeTypeBlob bs.Blob
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

// DynGet retrieves the typed blob at ref,
// constructs a new *dynamicpb.Message from its type,
// and loads the blob into it.
// This serializes the same as the original protobuf
// but is not convertible (or type-assertable) to the original protobuf's Go type.
//
// This is EXPERIMENTAL and relies on a hack.
// (It's also unclear what utility it has, but at least it's interesting.)
// See: https://groups.google.com/forum/?utm_medium=email&utm_source=footer#!msg/protobuf/xRWSIyQ3Qyg/YcuGve18BAAJ.
func DynGet(ctx context.Context, g typed.Getter, ref bs.Ref) (*dynamicpb.Message, error) {
	b, typeRefs, err := g.GetTyped(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "getting typed blob")
	}
	if len(typeRefs) == 0 {
		return nil, errors.New("no type for blob")
	}
	typ := typeRefs[0] // TODO: try other typeRefs until one works?

	var dp descriptorpb.DescriptorProto
	err = Get(ctx, g, typ, &dp)
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

// Init populates a bs.Store with TypeTypeBlob.
func Init(ctx context.Context, s bs.Store) error {
	_, _, err := s.Put(ctx, TypeTypeBlob)
	return err
}
