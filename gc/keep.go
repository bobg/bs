package gc

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
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
	if errors.Is(err, bs.ErrNotFound) || errors.Is(err, bs.ErrNoType) {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "getting %s", ref)
	}
	return forProtoEdges(p, func(to bs.Ref) error {
		return Add(ctx, k, g, to)
	})
}

// forProtoEdges walks through the message p, and invokes callback
// on each Ref it finds.
func forProtoEdges(p proto.Message, callback func(bs.Ref) error) error {
	var (
		refl   = p.ProtoReflect()
		md     = refl.Descriptor()
		fields = md.Fields()
		oneofs = md.Oneofs()
	)

	for i := 0; i < fields.Len(); i++ {
		err := onFieldEdge(refl, fields.Get(i), callback)
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
		err := onFieldEdge(refl, fd, callback)
		if err != nil {
			return err
		}
	}

	return nil
}

// helper for forProtoEdges; walks through a single field.
func onFieldEdge(refl protoreflect.Message, fd protoreflect.FieldDescriptor, callback func(bs.Ref) error) error {
	switch fd.Kind() {
	case protoreflect.BytesKind:
		val := refl.Get(fd).Bytes()
		if len(val) != len(bs.Ref{}) {
			return nil
		}
		err := callback(bs.RefFromBytes(val))
		if err != nil {
			return err
		}

	case protoreflect.MessageKind:
		val := refl.Get(fd).Message()
		err := forProtoEdges(val.Interface(), callback)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddAnchors adds all anchors in g at or after time `since` to the Keep.
//
// An anchor is added if its timestamp is at or after `since`.
// An anchor is also added if it has the latest timestamp _before_ `since` for a given name
// (since that anchor was in effect as of time `since`),
// unless there is another one with the same name and a timestamp exactly equal to `since`.
//
// The anchor blob's own ref is added,
// and also indirectly the ref it contains,
// plus any transitive refs that can be found.
func AddAnchors(ctx context.Context, k Keep, g anchor.Getter, since time.Time) error {
	// Always add the ref for an anchor's type blob.
	_, err := k.Add(ctx, anchor.TypeRef())
	if err != nil {
		return err
	}

	var last *anchor.Anchor

	err = g.ListAnchors(ctx, "", func(name string, ref bs.Ref, t time.Time) error {
		// Maybe add the anchor from the previous iteration.
		if shouldAdd(last, name, t, since) {
			err := addAnchor(ctx, k, g, last)
			if err != nil {
				return err
			}
		}

		a := &anchor.Anchor{Name: name, Ref: ref[:], At: timestamppb.New(t)}
		last = a

		if t.Before(since) {
			return nil
		}

		return addAnchor(ctx, k, g, a)
	})
	if err != nil {
		return err
	}

	if last != nil && last.At.AsTime().Before(since) {
		return addAnchor(ctx, k, g, last)
	}

	return nil
}

func shouldAdd(a *anchor.Anchor, name string, t, since time.Time) bool {
	if a == nil {
		return false
	}

	atime := a.At.AsTime()

	// Add it if it was the last one with its name,
	// and it was before `since`.
	if name != a.Name && atime.Before(since) {
		return true
	}

	// Also add it if it has the same name as the current anchor,
	// and was the last one before `since`
	// (unless this one is at exactly `since`).
	return name == a.Name && t.After(since) && atime.Before(since)
}

func addAnchor(ctx context.Context, k Keep, g bs.Getter, a *anchor.Anchor) error {
	protoRef, err := bs.ProtoRef(a)
	if err != nil {
		return err
	}
	err = Add(ctx, k, g, protoRef)
	if err != nil {
		return err
	}
	typeRef, err := bs.TypeRef(a)
	if err != nil {
		return err
	}
	return Add(ctx, k, g, typeRef)
}
