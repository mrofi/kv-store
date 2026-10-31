package store

import (
	"context"
	"errors"
	"time"
)

// MaxListLimit is the most keys one List call returns - S3's own
// ListObjectsV2 page size, so a call is always a single round trip.
const MaxListLimit = 1000

var ErrNotFound = errors.New("store: not found")

type ObjectVersion struct {
	VersionID      string
	ETag           string
	LastModified   time.Time
	IsDeleteMarker bool
}

// S3Store is the durable, versioned backing store. Every implementation
// must behave like S3 versioning: Put never overwrites in place, Delete
// leaves a marker rather than erasing history, and ErrNotFound is returned
// when the current version of a key doesn't exist (including when the
// current version is a delete marker).
type S3Store interface {
	Put(ctx context.Context, key string, value []byte) (Entry, error)
	Head(ctx context.Context, key string) (Entry, error)
	Get(ctx context.Context, key string) (Entry, error)
	GetVersion(ctx context.Context, key, versionID string) (Entry, error)
	ListVersions(ctx context.Context, key string) ([]ObjectVersion, error)
	// List returns up to limit keys that currently exist and start with
	// prefix, in lexicographic order. Keys whose current version is a
	// delete marker are omitted. limit <= 0 or > MaxListLimit means
	// MaxListLimit: List returns a single page and never paginates.
	List(ctx context.Context, prefix string, limit int) ([]string, error)
	Delete(ctx context.Context, key string) error
	PruneToMax(ctx context.Context, key string, maxVersions int) error
}
