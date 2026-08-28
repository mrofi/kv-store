package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// EncryptedS3Store wraps another S3Store and transparently encrypts value
// bytes with AES-256-GCM before they reach it, decrypting them on the way
// back out. Only Put/Get/GetVersion touch value bytes; every other method
// passes straight through since it never sees one. Callers (kv.Service,
// the in-memory cache, HTTP responses) always see plaintext — only what
// crosses the wire to the wrapped store is encrypted.
type EncryptedS3Store struct {
	inner S3Store
	aead  cipher.AEAD
}

func NewEncryptedS3Store(inner S3Store, key []byte) (*EncryptedS3Store, error) {
	if len(key) != 32 {
		return nil, errors.New("store: encryption key must be exactly 32 bytes (AES-256)")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &EncryptedS3Store{inner: inner, aead: aead}, nil
}

func (e *EncryptedS3Store) encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return e.aead.Seal(nonce, nonce, plaintext, nil), nil
}

func (e *EncryptedS3Store) decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := e.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("store: ciphertext too short")
	}
	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return e.aead.Open(nil, nonce, ct, nil)
}

func (e *EncryptedS3Store) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	ciphertext, err := e.encrypt(value)
	if err != nil {
		return Entry{}, err
	}
	entry, err := e.inner.Put(ctx, key, ciphertext)
	if err != nil {
		return Entry{}, err
	}
	entry.Value = value
	return entry, nil
}

func (e *EncryptedS3Store) Head(ctx context.Context, key string) (Entry, error) {
	return e.inner.Head(ctx, key)
}

func (e *EncryptedS3Store) Get(ctx context.Context, key string) (Entry, error) {
	entry, err := e.inner.Get(ctx, key)
	if err != nil {
		return Entry{}, err
	}
	plaintext, err := e.decrypt(entry.Value)
	if err != nil {
		return Entry{}, err
	}
	entry.Value = plaintext
	return entry, nil
}

func (e *EncryptedS3Store) GetVersion(ctx context.Context, key, versionID string) (Entry, error) {
	entry, err := e.inner.GetVersion(ctx, key, versionID)
	if err != nil {
		return Entry{}, err
	}
	plaintext, err := e.decrypt(entry.Value)
	if err != nil {
		return Entry{}, err
	}
	entry.Value = plaintext
	return entry, nil
}

func (e *EncryptedS3Store) ListVersions(ctx context.Context, key string) ([]ObjectVersion, error) {
	return e.inner.ListVersions(ctx, key)
}

func (e *EncryptedS3Store) Delete(ctx context.Context, key string) error {
	return e.inner.Delete(ctx, key)
}

func (e *EncryptedS3Store) PruneToMax(ctx context.Context, key string, maxVersions int) error {
	return e.inner.PruneToMax(ctx, key, maxVersions)
}
