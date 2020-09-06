package schema

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sort"

	"github.com/bobg/bs"
)

// NewMap produces a new, blank map,
// not yet written to a blob store.
func NewMap() *Map {
	return &Map{Node: new(TreeNode)}
}

// LoadMap loads the map at the given ref.
func LoadMap(ctx context.Context, g bs.Getter, ref bs.Ref) (*Map, error) {
	var m Map
	err := bs.GetProto(ctx, g, ref, &m)
	return &m, err
}

// Set sets the payload for a given key in a Map.
// It returns the Map's possibly-updated Ref and an Outcome:
// ONone if no change was needed (the key was already present and had the same payload),
// OUpdated (the key was present with a different payload), or
// OAdded (the key was not present).
func (m *Map) Set(ctx context.Context, store bs.Store, key, payload []byte) (bs.Ref, Outcome, error) {
	return treeSet(ctx, m, store, hashKey(key), func(t tree, i int32, insert bool) Outcome {
		m := t.(*Map)
		if insert {
			newMembers := make([]*MapPair, 1+len(m.Members))
			copy(newMembers[:i], m.Members[:i])
			newMembers[i] = &MapPair{
				Key:     key,
				Payload: payload,
			}
			copy(newMembers[i+1:], m.Members[i:])
			m.Members = newMembers
			return OAdded
		}
		if bytes.Equal(payload, m.Members[i].Payload) {
			return ONone
		}
		m.Members[i].Payload = payload
		return OUpdated
	})
}

func (m *Map) treenode() *TreeNode    { return m.Node }
func (m *Map) numMembers() int32      { return int32(len(m.Members)) }
func (m *Map) keyHash(i int32) []byte { return hashKey(m.Members[i].Key) }
func (m *Map) zeroMembers()           { m.Members = nil }

func (m *Map) newAt(depth int32) tree {
	return &Map{
		Node: &TreeNode{
			Depth: depth,
		},
	}
}

func (m *Map) copyMember(other tree, i int32) {
	m.Members = append(m.Members, (other.(*Map)).Members[i])
	m.Node.Size++
}

func (m *Map) removeMember(i int32) {
	n := len(m.Members)
	copy(m.Members[i:], m.Members[i+1:])
	m.Members[n-1] = nil
	m.Members = m.Members[:n-1]
}

func (m *Map) sortMembers() {
	sort.Slice(m.Members, func(i, j int) bool {
		return bytes.Compare(hashKey(m.Members[i].Key), hashKey(m.Members[j].Key)) < 0
	})
}

func hashKey(k []byte) []byte {
	h := sha256.Sum256(k)
	return h[:]
}

// Lookup finds the given key in a Map and returns its payload if found.
// The boolean return value indicates whether the key was in the Map.
func (m *Map) Lookup(ctx context.Context, g bs.Getter, key []byte) ([]byte, bool, error) {
	var (
		ok     bool
		result []byte
	)
	err := treeLookup(ctx, m, g, hashKey(key), func(t tree, i int32) {
		m := t.(*Map)
		ok = true
		result = m.Members[i].Payload
	})
	return result, ok, err
}

// Remove removes the member pair with the given key from a Map.
// It return's the Map's possibly-updated Ref and a boolean telling whether a change was made,
// which will be false if the key was not in the Map.
func (m *Map) Remove(ctx context.Context, store bs.Store, key []byte) (bs.Ref, bool, error) {
	return treeRemove(ctx, m, store, hashKey(key))
}

// Each calls a function for each member pair of a Map,
// in an indeterminate order.
// If the callback returns an error,
// Each exits with that error.
func (m *Map) Each(ctx context.Context, g bs.Getter, f func(*MapPair) error) error {
	return treeEach(ctx, m, g, func(t tree, i int32) error {
		m := t.(*Map)
		return f(m.Members[i])
	})
}
