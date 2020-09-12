# BS, a content-addressable blob store

[![GoDoc](https://godoc.org/github.com/bobg/bs?status.svg)](https://godoc.org/github.com/bobg/bs)
[![Go Report Card](https://goreportcard.com/badge/github.com/bobg/bs)](https://goreportcard.com/report/github.com/bobg/bs)

This is BS, an implementation of a content-addressable blob store.

A blob store stores arbitrarily sized sequences of bytes,
or _blobs_,
and indexes them by their hash,
which is used as a unique key.
This key is called the blob’s reference, or _ref_.

With a sufficiently good hash algorithm,
the likelihood of any two distinct blobs “colliding” is so small
that you’re better off worrying about the much-more-likely danger of spontaneously combusting
while also facing a thundering herd of angry rhinoceroses,
at the very moment of a solar eclipse.

This module uses sha2-256,
which is a sufficiently good hash algorithm.

The fact that the lookup key is computed from a blob’s content,
rather than by its location in memory or the order in which it was added,
is the meaning of “content-addressable.”

Content addressability has some desirable properties,
but it does mean that if some data changes,
so does its ref,
which can make it tricky to keep track of a piece of data over its lifetime.
So in addition to a plain blob store,
this module provides an _anchor_ store.
An anchor is a structured blob containing a name,
a timestamp,
and a blob ref.
You can give a blob a name
(such as a filename)
by storing an anchor pointing to the blob’s ref.
As the data changes,
you can store new anchors with the same name but an updated ref and timestamp.
An anchor store lets you retrieve the latest ref for a given name as of a given timestamp.

Blob stores work best when blobs are not too big,
so when storing potentially large bytestreams,
use the split.Write function
(in the split subpackage).
This splits the input into multiple blobs organized as a tree,
and it returns the ref of the tree’s root.
The bytestream can be reassembled with split.Read.

When splitting,
blob boundaries are determined not by position or size but by content,
using the technique of _hashsplitting_.
This same technique is used by git and rsync to represent file changes very compactly:
if two versions of a file have a small difference,
only the blob containing the difference is affected.
The other blobs of the file are unchanged.
This is the same reason that the blobs are organized into a tree.
If the blobs were organized as a list,
the whole list would have to change any time a blob is added, removed, or replaced.
But as a tree, only the subtree with the affected blob has to change.

The bs package describes an abstract Store interface.
The store subpackage contains a registry for different concrete types of blob store:
a memory-based one,
a file-based one,
a SQLite-based one,
a Postgresql-based one,
and a Google Cloud Storage one.
There is also a blob store that is an LRU cache for an underlying blob store,
and a blob store that transforms blobs
(compressing or encrypting them, for instance)
on their way into and out of an underlying blob store.
And store/rpc supplies a simple grpc server wrapping a Store,
and a client for it that is a Store.

BS is inspired by,
and a simplification of,
[the Perkeep project](https://perkeep.org/),
which is presently dormant.
