package crashreplay

import "encoding/binary"

// keyPrefix namespaces the keys driven by the crash-replay gate so that the
// scenario keyspace is easy to enumerate in order.
var keyPrefix = []byte("crashreplay/")

// encodeKey returns the deterministic, lexicographically ordered key used for
// write index i. A fixed-width suffix keeps replay order stable across
// machines.
func encodeKey(i int) []byte {
	key := make([]byte, len(keyPrefix)+8)
	copy(key, keyPrefix)
	binary.BigEndian.PutUint64(key[len(keyPrefix):], uint64(i))
	return key
}

// makeValue builds the value for key index i. The value embeds the key index
// and its own length, followed by a deterministic fill, so a truncated ("torn")
// value can never compare equal to the expected one after replay.
func makeValue(i, valueSize int) []byte {
	if valueSize < 16 {
		valueSize = 16
	}
	value := make([]byte, valueSize)
	binary.BigEndian.PutUint64(value[0:8], uint64(i))
	binary.BigEndian.PutUint64(value[8:16], uint64(valueSize))
	for j := 16; j < valueSize; j++ {
		value[j] = byte(uint64(j) * 2654435761 >> 56)
	}
	return value
}

// makeKV returns the key/value pair for write index i.
func makeKV(i, valueSize int) (key, value []byte) {
	return encodeKey(i), makeValue(i, valueSize)
}

// decodeValueIndex returns the write index embedded in a value.
func decodeValueIndex(value []byte) (int, bool) {
	if len(value) < 8 {
		return 0, false
	}
	return int(binary.BigEndian.Uint64(value[0:8])), true
}
