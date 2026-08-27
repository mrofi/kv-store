package store

import (
	"context"
	"errors"
	"time"
)

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
	Delete(ctx context.Context, key string) error
	PruneToMax(ctx context.Context, key string, maxVersions int) error
}
