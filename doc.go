// Package bs is a content-addressable blob store.
//
// A blob store stores arbitrarily sized sequences of bytes,
// or _blobs_,
// and indexes them by their hash,
// which is used as a unique key.
// This key is called the blob’s reference, or _ref_.
//
// With a sufficiently good hash algorithm,
// the likelihood of any two distinct blobs “colliding” is so small
// that you’re better off worrying about spontaneously combusting
// while also facing a thundering herd of angry rhinoceroses,
// at the very moment of a solar eclipse.
//
// This module uses sha2-256,
// which is a sufficiently good hash algorithm.
//
// The fact that the lookup key is computed from a blob’s content,
// rather than by its location in memory or the order in which it was added,
// is the meaning of “content-addressable.”
//
// Content addressability has some desirable properties,
// but it does mean that if some data changes,
// so does its ref,
// which can make it tricky to keep track of a piece of data over its lifetime.
// So a blob store also stores _anchors_.
// These are mappings from an arbitrary string
// (such as a filename)
// to a ref and a timestamp.
// At a later timestamp,
// the same anchor may map to a different ref.
//
// Blob stores work best when blobs are not too big,
// so when storing potentially large bytestreams,
// use the split.Write function
// (in the split subpackage).
// This splits the input into multiple blobs organized as a tree,
// and it returns the ref of the tree’s root.
// The bytestream can be reassembled with split.Read.
//
// BS is inspired by, and a simplification of,
// [the Perkeep project](https://perkeep.org/),
// which is presently dormant.
package bs
