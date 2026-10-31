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

func TestFakeListFiltersByPrefixSortedAndSkipsDeleted(t *testing.T) {
	f := NewFakeS3Store()
	ctx := context.Background()
	for _, k := range []string{"u1.ns.b", "u1.ns.a", "u1.other.x", "u2.ns.a", "u1.ns.gone"} {
		if _, err := f.Put(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Put(%q) returned error: %v", k, err)
		}
	}
	if err := f.Delete(ctx, "u1.ns.gone"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	got, err := f.List(ctx, "u1.ns.", 0)
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	want := []string{"u1.ns.a", "u1.ns.b"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("List = %v, want %v", got, want)
	}

	// A key that was deleted and then written again is listed once more.
	if _, err := f.Put(ctx, "u1.ns.gone", []byte("back")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	got, _ = f.List(ctx, "u1.ns.", 0)
	if len(got) != 3 {
		t.Fatalf("List after re-Put = %v, want 3 keys", got)
	}
}

func TestFakeListHonoursLimit(t *testing.T) {
	f := NewFakeS3Store()
	ctx := context.Background()
	for _, k := range []string{"p.a", "p.b", "p.c"} {
		if _, err := f.Put(ctx, k, nil); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
	}

	got, err := f.List(ctx, "p.", 2)
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(got) != 2 || got[0] != "p.a" || got[1] != "p.b" {
		t.Fatalf("List(limit 2) = %v, want [p.a p.b]", got)
	}
}

func TestFakeListEmptyReturnsNonNilSlice(t *testing.T) {
	got, err := NewFakeS3Store().List(context.Background(), "nothing.", 10)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("List = %v, %v; want empty non-nil slice", got, err)
	}
}

func TestClampListLimit(t *testing.T) {
	for in, want := range map[int]int{-5: MaxListLimit, 0: MaxListLimit, 1: 1, 1000: 1000, 5000: MaxListLimit} {
		if got := clampListLimit(in); got != want {
			t.Errorf("clampListLimit(%d) = %d, want %d", in, got, want)
		}
	}
}
