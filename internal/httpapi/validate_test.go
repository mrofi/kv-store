package httpapi

import (
	"strings"
	"testing"
)

func TestValidateKeyAcceptsOrdinaryKey(t *testing.T) {
	if err := validateKey("foo-bar_123.baz"); err != nil {
		t.Fatalf("validateKey returned error for a valid key: %v", err)
	}
}

func TestValidateKeyRejectsEmpty(t *testing.T) {
	if err := validateKey(""); err == nil {
		t.Fatal("expected an error for an empty key")
	}
}

func TestValidateKeyRejectsSlash(t *testing.T) {
	if err := validateKey("foo/bar"); err == nil {
		t.Fatal("expected an error for a key containing '/'")
	}
}

func TestValidateKeyRejectsOversized(t *testing.T) {
	if err := validateKey(strings.Repeat("a", 513)); err == nil {
		t.Fatal("expected an error for a key over 512 bytes")
	}
}

func TestValidateKeyAcceptsMaxLength(t *testing.T) {
	if err := validateKey(strings.Repeat("a", 512)); err != nil {
		t.Fatalf("validateKey returned error for a 512-byte key: %v", err)
	}
}
