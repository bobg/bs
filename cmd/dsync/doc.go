// Command dsync efficiently synchronizes a tree of files from a primary source to a replica.
//
// Both primary and replica represent their file trees as a blobstore of hashsplit files.
// (This is similar to the technique used by rsync, git, and bup.)
// On startup, the primary populates its blobstore and sends all its blobs to the replica,
// which reconstitutes files and directories from their blob trees.
// The primary then watches for filesystem change events and immediately sends any new blobs to the replica,
// which reconstitutes only the affected files and directories.
//
// Blobs are not garbage-collected;
// the blobstores grow without bound as filesystem changes accumulate.
//
// Both primary and replica can be exited by sending a keyboard interrupt,
// whereupon each will remove its blobstore.
// The replica will not, however, remove the directory where it has reconstituted files.
//
// To try dsync, first launch a replica process with:
//
//   mkdir REPLICA_ROOT
//   dsync -replica -root REPLICA_ROOT
//
// (The directory REPLICA_ROOT is where files and directories will be created.)
// The replica will report its LISTEN_ADDR (which can also be specified with -addr).
//
// Next, launch the primary with:
//
//   dsync -primary -root PRIMARY_ROOT -addr LISTEN_ADDR
//
// where LISTEN_ADDR is what the replica reported on startup,
// and PRIMARY_ROOT is the root of the file tree to be synchronized to the replica.
//
// The primary will do its initial sync to the replica.
// Thereafter, changes you make in PRIMARY_ROOT will be immediately sync'd to the replica.
package main
