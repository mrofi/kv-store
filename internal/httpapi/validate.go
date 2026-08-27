package httpapi

import (
	"errors"
	"strings"
)

// maxKeyLength is well under S3's own 1024-byte object key limit, leaving
// room for the "kv/" prefix objectKey adds in internal/store.
const maxKeyLength = 512

var (
	errEmptyKey   = errors.New("key must not be empty")
	errKeySlash   = errors.New("key must not contain '/'")
	errKeyTooLong = errors.New("key exceeds maximum length")
)

// validateKey rejects keys that can't round-trip safely through both the
// HTTP path segment they arrive as and the S3 object key they become.
func validateKey(key string) error {
	if key == "" {
		return errEmptyKey
	}
	if strings.Contains(key, "/") {
		return errKeySlash
	}
	if len(key) > maxKeyLength {
		return errKeyTooLong
	}
	return nil
}
