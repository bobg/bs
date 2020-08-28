package gc

import (
	"context"
	stderrs "errors"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/bobg/bs"
)

// Keep is a set of refs to protect from garbage collection.
type Keep interface {
	// Add adds a single ref to the Keep.
	// It returns true if it was newly added and false if it was already present.
	Add(context.Context, bs.Ref) (bool, error)

	// Contains tells whether a ref is in the Keep.
	Contains(context.Context, bs.Ref) (bool, error)
}

// Add adds a ref to the Keep.
// It then fetches its blob from g and,
// if it has a type
// uses the type info to locate possible refs in the blob,
// recursively adding any that it finds.
// This errs on the side of adding values to the Keep that look like refs but aren't.
//
// It is not an error for g to have no blob for ref.
func Add(ctx context.Context, k Keep, g bs.Getter, ref bs.Ref) error {
	added, err := k.Add(ctx, ref)
	if err != nil {
		return errors.Wrapf(err, "adding %s", ref)
	}
	if !added {
		return nil
	}

	p, err := bs.DynGetProto(ctx, g, ref)
	if stderrs.Is(err, bs.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "getting %s", ref)
	}
	return addProto(ctx, k, g, p)
}

func addProto(ctx context.Context, k Keep, g bs.Getter, p proto.Message) error {
	var (
		refl   = p.ProtoReflect()
		md     = refl.Descriptor()
		fields = md.Fields()
		oneofs = md.Oneofs()
	)

	for i := 0; i < fields.Len(); i++ {
		err := addField(ctx, k, g, refl, fields.Get(i))
		if err != nil {
			return err
		}
	}

	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		fd := refl.WhichOneof(oneof)
		if fd == nil {
			continue
		}
		err := addField(ctx, k, g, refl, fd)
		if err != nil {
			return err
		}
	}

	return nil
}

func addField(ctx context.Context, k Keep, g bs.Getter, refl protoreflect.Message, fd protoreflect.FieldDescriptor) error {
	switch fd.Kind() {
	case protoreflect.BytesKind:
		val := refl.Get(fd).Bytes()
		if len(val) != len(bs.Ref{}) {
			return nil
		}
		err := Add(ctx, k, g, bs.RefFromBytes(val))
		if err != nil {
			return err
		}

	case protoreflect.MessageKind:
		val := refl.Get(fd).Message()
		err := addProto(ctx, k, g, val.Interface())
		if err != nil {
			return err
		}
	}

	return nil
}
