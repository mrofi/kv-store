package httpapi

import (
	"context"

	"github.com/mrofi/kv-store/store"
)

type fakeKVService struct {
	putEntry store.Entry
	putErr   error

	getEntry store.Entry
	getStale bool
	getErr   error

	versionEntry store.Entry
	versionErr   error

	versions    []store.ObjectVersion
	versionsErr error

	deleteErr error

	appliedPushKey   string
	appliedPushEntry store.Entry

	appliedDeleteKey string

	pingErr error
}

func (f *fakeKVService) Put(ctx context.Context, key string, value []byte) (store.Entry, error) {
	return f.putEntry, f.putErr
}

func (f *fakeKVService) Get(ctx context.Context, key string) (store.Entry, bool, error) {
	return f.getEntry, f.getStale, f.getErr
}

func (f *fakeKVService) GetVersion(ctx context.Context, key, versionID string) (store.Entry, error) {
	return f.versionEntry, f.versionErr
}

func (f *fakeKVService) ListVersions(ctx context.Context, key string) ([]store.ObjectVersion, error) {
	return f.versions, f.versionsErr
}

func (f *fakeKVService) Delete(ctx context.Context, key string) error {
	return f.deleteErr
}

func (f *fakeKVService) ApplyPush(key string, entry store.Entry) {
	f.appliedPushKey = key
	f.appliedPushEntry = entry
}

func (f *fakeKVService) ApplyDelete(key string) {
	f.appliedDeleteKey = key
}

func (f *fakeKVService) Ping(ctx context.Context) error {
	return f.pingErr
}
