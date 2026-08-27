package store

import (
	"context"
	"errors"
	"testing"
)

func TestFakePutCreatesNewVersion(t *testing.T) {
	f := NewFakeS3Store()
	v1, err := f.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	v2, err := f.Put(context.Background(), "foo", []byte("v2"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if v1.VersionID == v2.VersionID {
		t.Fatal("expected distinct version ids for successive Puts")
	}
}

func TestFakeGetReturnsLatest(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := f.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	got, err := f.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(got.Value) != "v2" {
		t.Fatalf("Get = %q, want v2", got.Value)
	}
}

func TestFakeGetMissingKeyReturnsNotFound(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFakeGetVersionReturnsSpecificVersion(t *testing.T) {
	f := NewFakeS3Store()
	v1, err := f.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := f.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	got, err := f.GetVersion(context.Background(), "foo", v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersion returned error: %v", err)
	}
	if string(got.Value) != "v1" {
		t.Fatalf("GetVersion = %q, want v1", got.Value)
	}
}

func TestFakeListVersionsOrderedNewestFirst(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	v2, err := f.Put(context.Background(), "foo", []byte("v2"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	versions, err := f.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
	if versions[0].VersionID != v2.VersionID {
		t.Fatalf("versions[0] = %q, want newest %q", versions[0].VersionID, v2.VersionID)
	}
}

func TestFakeDeleteThenGetNotFound(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if err := f.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if _, err := f.Get(context.Background(), "foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if _, err := f.Head(context.Background(), "foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected Head ErrNotFound after delete, got %v", err)
	}
}

func TestFakePruneToMaxKeepsNewestVersions(t *testing.T) {
	f := NewFakeS3Store()
	for i := 0; i < 5; i++ {
		if _, err := f.Put(context.Background(), "foo", []byte("v")); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
	}
	if err := f.PruneToMax(context.Background(), "foo", 2); err != nil {
		t.Fatalf("PruneToMax returned error: %v", err)
	}
	versions, err := f.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}
