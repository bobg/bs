// Package schema implements miscellaneous data structures that can be converted to and from blobs.
package schema

import (
	"bytes"
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/gc"
)

// NewSet produces a new, empty Set,
// not yet written to a blob store.
func NewSet() *Set {
	return &Set{Node: new(TreeNode)}
}

// LoadSet loads the set at the given ref.
func LoadSet(ctx context.Context, g bs.Getter, ref bs.Ref) (*Set, error) {
	var s Set
	err := bs.GetProto(ctx, g, ref, &s)
	return &s, err
}

type setItem []byte

func (it setItem) keyHash() []byte {
	return it
}

func (it setItem) mutate(t tree, pos int32, insert bool) Outcome {
	return setMutate(it)(t, pos, insert)
}

// SetFromRefs creates a Set from a slice of bs.Ref.
func SetFromRefs(ctx context.Context, s bs.Store, refs []bs.Ref) (*Set, bs.Ref, error) {
	items := make([]treeItem, 0, len(refs))
	for _, ref := range refs {
		ref := ref
		items = append(items, setItem(ref[:]))
	}
	t, ref, err := treeFromGo(ctx, s, items, setNewAt)
	return t.(*Set), ref, err
}

// Add adds a Ref to a Set.
// It returns the Set's own possibly-updated Ref,
// and a boolean indicating whether the Set was changed,
// which will be false if the input Ref was already present.
// If the Set is changed, it is written to the Store.
func (s *Set) Add(ctx context.Context, store bs.Store, ref bs.Ref) (bs.Ref, bool, error) {
	newref, outcome, err := treeSet(ctx, s, store, ref[:], setMutate(ref[:]))
	return newref, outcome == OAdded, err
}

func setMutate(member []byte) func(tree, int32, bool) Outcome {
	return func(m tree, i int32, insert bool) Outcome {
		if !insert {
			return ONone
		}
		s := m.(*Set)
		if i == int32(len(s.Members)) {
			s.Members = append(s.Members, member)
		} else {
			newMembers := make([][]byte, 1+len(s.Members))
			copy(newMembers[:i], s.Members[:i])
			newMembers[i] = member
			copy(newMembers[i+1:], s.Members[i:])
			s.Members = newMembers
		}
		return OAdded
	}
}

func (s *Set) treenode() *TreeNode    { return s.Node }
func (s *Set) numMembers() int32      { return int32(len(s.Members)) }
func (s *Set) keyHash(i int32) []byte { return s.Members[i] }
func (s *Set) zeroMembers()           { s.Members = nil }

func (*Set) newAt(depth int32) tree {
	return setNewAt(depth)
}

func setNewAt(depth int32) tree {
	return &Set{
		Node: &TreeNode{
			Depth: depth,
		},
	}
}

func (s *Set) copyMember(other tree, i int32) {
	s.Members = append(s.Members, (other.(*Set)).Members[i])
	s.Node.Size++
}

func (s *Set) removeMember(i int32) {
	n := len(s.Members)
	copy(s.Members[i:], s.Members[i+1:])
	s.Members[n-1] = nil
	s.Members = s.Members[:n-1]
}

func (s *Set) sortMembers() {
	sort.Slice(s.Members, func(i, j int) bool {
		return bytes.Compare(s.Members[i], s.Members[j]) < 0
	})
}

// Check checks whether ref is in s.
func (s *Set) Check(ctx context.Context, g bs.Getter, ref bs.Ref) (bool, error) {
	var ok bool
	err := treeLookup(ctx, s, g, ref[:], func(tree, int32) {
		ok = true
	})
	return ok, err
}

// Remove removes ref from s.
// It returns the set's new root ref,
// and a boolean indicating whether the ref was removed.
// (If false, the ref was not present in the set, and the root ref is unchanged.)
// If s is changed, it is written to the Store.
func (s *Set) Remove(ctx context.Context, store bs.Store, ref bs.Ref) (bs.Ref, bool, error) {
	return treeRemove(ctx, s, store, ref[:])
}

// Each calls a function for each member of the Set.
func (s *Set) Each(ctx context.Context, g bs.Getter, f func(bs.Ref) error) error {
	return treeEach(ctx, s, g, func(ml tree, i int32) error {
		m := ml.(*Set)
		return f(bs.RefFromBytes(m.Members[i]))
	})
}

// ProtectSet returns a gc.ProtectFunc that protects the nodes of a Set and the refs they contain.
// The parameter f is the function that protects the refs that are members of the Set.
func ProtectSet(f gc.ProtectFunc) gc.ProtectFunc {
	return func(ctx context.Context, g bs.Getter, ref bs.Ref) ([]gc.ProtectPair, error) {
		s, err := LoadSet(ctx, g, ref)
		if err != nil {
			return nil, errors.Wrap(err, "loading set")
		}

		var res []gc.ProtectPair

		if s.Node != nil && s.Node.Left != nil {
			res = append(res, gc.ProtectPair{Ref: bs.RefFromBytes(s.Node.Left.Ref), F: ProtectSet(f)})
		}
		if s.Node != nil && s.Node.Right != nil {
			res = append(res, gc.ProtectPair{Ref: bs.RefFromBytes(s.Node.Right.Ref), F: ProtectSet(f)})
		}

		for _, member := range s.Members {
			res = append(res, gc.ProtectPair{Ref: bs.RefFromBytes(member), F: f})
		}

		return res, nil
	}
}
