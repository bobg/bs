// Package dsync contains types and functions
// for efficiently synchronizing trees of files
// between a primary and one or more replicas.
//
//go:generate protoc -I. --go_out=. dsync.proto
package dsync
