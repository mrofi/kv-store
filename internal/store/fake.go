package store

import (
	"context"
	"strconv"
	"sync"
	"time"
)

type versionRecord struct {
	entry          Entry
	isDeleteMarker bool
}

// FakeS3Store is an in-memory S3Store used for unit tests. It is not a
// real store; it exists so higher layers (kv.Service) can be tested
// without a network dependency, while still behaving like S3 versioning:
// every Put appends a new version, Delete appends a delete marker, and
// nothing is ever overwritten in place.
type FakeS3Store struct {
	mu       sync.Mutex
	versions map[string][]versionRecord // oldest first
	seq      int

	HeadErr error
	GetErr  error
	PutErr  error

	HeadCalls int
	GetCalls  int
	PutCalls  int
}

func NewFakeS3Store() *FakeS3Store {
	return &FakeS3Store{versions: make(map[string][]versionRecord)}
}

func (f *FakeS3Store) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PutCalls++
	if f.PutErr != nil {
		return Entry{}, f.PutErr
	}
	f.seq++
	entry := Entry{
		Value:     append([]byte(nil), value...),
		VersionID: "v" + strconv.Itoa(f.seq),
		ETag:      "etag-" + strconv.Itoa(f.seq),
		UpdatedAt: time.Now(),
	}
	f.versions[key] = append(f.versions[key], versionRecord{entry: entry})
	return entry, nil
}

func (f *FakeS3Store) Head(ctx context.Context, key string) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.HeadCalls++
	if f.HeadErr != nil {
		return Entry{}, f.HeadErr
	}
	recs := f.versions[key]
	if len(recs) == 0 || recs[len(recs)-1].isDeleteMarker {
		return Entry{}, ErrNotFound
	}
	latest := recs[len(recs)-1].entry
	return Entry{VersionID: latest.VersionID, ETag: latest.ETag, UpdatedAt: latest.UpdatedAt}, nil
}

func (f *FakeS3Store) Get(ctx context.Context, key string) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.GetCalls++
	if f.GetErr != nil {
		return Entry{}, f.GetErr
	}
	recs := f.versions[key]
	if len(recs) == 0 || recs[len(recs)-1].isDeleteMarker {
		return Entry{}, ErrNotFound
	}
	return recs[len(recs)-1].entry, nil
}

func (f *FakeS3Store) GetVersion(ctx context.Context, key, versionID string) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.versions[key] {
		if r.entry.VersionID == versionID && !r.isDeleteMarker {
			return r.entry, nil
		}
	}
	return Entry{}, ErrNotFound
}

func (f *FakeS3Store) ListVersions(ctx context.Context, key string) ([]ObjectVersion, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	recs := f.versions[key]
	out := make([]ObjectVersion, 0, len(recs))
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		out = append(out, ObjectVersion{
			VersionID:      r.entry.VersionID,
			ETag:           r.entry.ETag,
			LastModified:   r.entry.UpdatedAt,
			IsDeleteMarker: r.isDeleteMarker,
		})
	}
	return out, nil
}

func (f *FakeS3Store) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	f.versions[key] = append(f.versions[key], versionRecord{
		entry:          Entry{VersionID: "v" + strconv.Itoa(f.seq), UpdatedAt: time.Now()},
		isDeleteMarker: true,
	})
	return nil
}

func (f *FakeS3Store) PruneToMax(ctx context.Context, key string, maxVersions int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	recs := f.versions[key]
	if maxVersions <= 0 || len(recs) <= maxVersions {
		return nil
	}
	f.versions[key] = recs[len(recs)-maxVersions:]
	return nil
}
