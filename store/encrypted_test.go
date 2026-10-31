package store

import (
	"context"
	"errors"
	"testing"
)

func TestNewEncryptedS3StoreRejectsInvalidKeyLength(t *testing.T) {
	inner := NewFakeS3Store()
	if _, err := NewEncryptedS3Store(inner, []byte("too-short")); err == nil {
		t.Fatal("expected an error for a key that isn't 32 bytes")
	}
}

func TestEncryptedStorePutGetRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	entry, err := enc.Put(context.Background(), "foo", []byte("secret-value"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if entry.VersionID == "" {
		t.Fatal("expected non-empty version id")
	}
	if string(entry.Value) != "secret-value" {
		t.Fatalf("Put returned value %q, want plaintext secret-value", entry.Value)
	}

	raw, err := inner.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("inner Get returned error: %v", err)
	}
	if string(raw.Value) == "secret-value" {
		t.Fatal("expected the underlying store to hold ciphertext, not plaintext")
	}

	got, err := enc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(got.Value) != "secret-value" {
		t.Fatalf("got value %q, want secret-value", got.Value)
	}
}

func TestEncryptedStoreGetVersionDecrypts(t *testing.T) {
	key := make([]byte, 32)
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	v1, err := enc.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := enc.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	got, err := enc.GetVersion(context.Background(), "foo", v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersion returned error: %v", err)
	}
	if string(got.Value) != "v1" {
		t.Fatalf("got value %q, want v1", got.Value)
	}
}

func TestEncryptedStoreHeadPassesThrough(t *testing.T) {
	key := make([]byte, 32)
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	entry, err := enc.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	head, err := enc.Head(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Head returned error: %v", err)
	}
	if head.VersionID != entry.VersionID {
		t.Fatalf("Head version = %q, want %q", head.VersionID, entry.VersionID)
	}
}

func TestEncryptedStoreListVersionsPassesThrough(t *testing.T) {
	key := make([]byte, 32)
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	if _, err := enc.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := enc.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	versions, err := enc.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}

func TestEncryptedStoreDeletePassesThrough(t *testing.T) {
	key := make([]byte, 32)
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	if _, err := enc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if err := enc.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if _, err := enc.Get(context.Background(), "foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestEncryptedStorePruneToMaxPassesThrough(t *testing.T) {
	key := make([]byte, 32)
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := enc.Put(context.Background(), "foo", []byte("v")); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
	}
	if err := enc.PruneToMax(context.Background(), "foo", 2); err != nil {
		t.Fatalf("PruneToMax returned error: %v", err)
	}
	versions, err := enc.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}

func TestEncryptProducesDifferentCiphertextEachTime(t *testing.T) {
	key := make([]byte, 32)
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	c1, err := enc.encrypt([]byte("same-value"))
	if err != nil {
		t.Fatalf("encrypt returned error: %v", err)
	}
	c2, err := enc.encrypt([]byte("same-value"))
	if err != nil {
		t.Fatalf("encrypt returned error: %v", err)
	}
	if string(c1) == string(c2) {
		t.Fatal("expected different ciphertext for identical plaintext due to random nonces")
	}
}

func TestEncryptedStoreListPassesThrough(t *testing.T) {
	inner := NewFakeS3Store()
	enc, err := NewEncryptedS3Store(inner, make([]byte, 32))
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}
	for _, k := range []string{"p.a", "p.b", "q.a"} {
		if _, err := enc.Put(context.Background(), k, []byte("secret")); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
	}

	got, err := enc.List(context.Background(), "p.", 10)
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(got) != 2 || got[0] != "p.a" || got[1] != "p.b" {
		t.Fatalf("List = %v, want [p.a p.b]", got)
	}
}
