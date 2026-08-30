package storage

import (
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

func Hash(s string) uint64 {
	slice := FromStringToBytes(s)
	return xxhash.Sum64(slice)

}

func FromStringToBytes(s string) []byte {
	if s == "" {
		return []byte{}
	}

	return unsafe.Slice(unsafe.StringData(s), len(s))
}
