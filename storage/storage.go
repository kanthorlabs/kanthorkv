package storage

import "unicode/utf8"

const (
	// BLOCK_SIZE is the size of each block in bytes
	BLOCK_SIZE = 4096

	// INT64_SIZE is the number of bytes used to store an integer (int64)
	INT64_SIZE = 8
)

func MaxLength(length int) int {
	// Int64 bytes (for length) + maximum possible UTF-8 bytes.
	return INT64_SIZE + (length * utf8.UTFMax)
}
