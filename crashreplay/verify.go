package crashreplay

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/vfs"
)

// VerifyExpectations describes what replay must leave behind.
type VerifyExpectations struct {
	// DirName is the database directory on the filesystem.
	DirName string
	// ValueSize is the value size used when the keys were written.
	ValueSize int
	// ExpectedKeys is the number of writes whose Sync commit returned before
	// the kill: keys [0, ExpectedKeys) must all be present with full values.
	ExpectedKeys int
}

// VerifyReport is the outcome of replay verification. Err is non-nil if
// verification had to stop; it always carries the offending key and reason.
type VerifyReport struct {
	Stage        string
	Opened       bool
	PresentKeys  int
	MissingKeys  []int
	TornValues   []int
	FirstInOrder int
	OutOfOrder   bool
	Err          error
}

func (r *VerifyReport) fail(format string, args ...any) *VerifyReport {
	r.Err = fmt.Errorf("crashreplay: replay verification stopped: "+format, args...)
	return r
}

// VerifyReopen reopens the same database directory on the post-kill filesystem
// using pebble's ordinary recovery path, then verifies that every key that had
// to survive is present with its complete value and that the keys come back in
// stable order. A missing key, a torn value, an ordering break, or an opaque
// replay failure halts verification with a specific reason instead of
// pretending the database read cleanly.
func VerifyReopen(fs vfs.FS, makeOptions func(vfs.FS) *Options, ex VerifyExpectations) *VerifyReport {
	if ex.ValueSize < 16 {
		ex.ValueSize = 16
	}
	r := &VerifyReport{FirstInOrder: -1}

	db, err := Open(ex.DirName, makeOptions(fs))
	if err != nil {
		// Recovery failed (torn WAL/manifest debris). Require the cause to be
		// explicit; an empty error means corruption was swallowed somewhere.
		if err.Error() == "" {
			return r.fail("replay reported an empty error reopening %q", ex.DirName)
		}
		return r.fail("replay of %q failed: %w", ex.DirName, err)
	}
	r.Opened = true
	defer func() { _ = db.Close() }()

	// Point checks: each expected key with its full value.
	for i := 0; i < ex.ExpectedKeys; i++ {
		key := encodeKey(i)
		want := makeValue(i, ex.ValueSize)
		got, closer, err := db.Get(key)
		if err == ErrNotFound {
			r.MissingKeys = append(r.MissingKeys, i)
			continue
		}
		if err != nil {
			return r.fail("reading expected key index %d after replay failed: %w", i, err)
		}
		closeErr := closer.Close()
		value := got
		if closeErr != nil {
			return r.fail("closing value of key index %d after replay failed: %w", i, closeErr)
		}
		if !bytes.Equal(value, want) {
			r.TornValues = append(r.TornValues, i)
		}
	}

	// Ordered scan: the surviving scenario keys must come back in index order,
	// with no holes and no duplicates. This is the cross-machine stability
	// check.
	it, err := db.NewIter(&IterOptions{LowerBound: append(append([]byte{}, keyPrefix...), 0)})
	if err != nil {
		return r.fail("creating iterator after replay failed: %w", err)
	}
	prev := -1
	for it.First(); it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), keyPrefix) {
			continue
		}
		raw := stripKeyPrefix(it.Key())
		if len(raw) < 8 {
			return r.fail("scenario key %q has malformed fixed-width suffix", string(it.Key()))
		}
		idx := int(binary.BigEndian.Uint64(raw))
		if r.FirstInOrder < 0 {
			r.FirstInOrder = idx
		}
		if idx <= prev {
			r.OutOfOrder = true
			break
		}
		if idx >= ex.ExpectedKeys {
			// An unsynced tail write may survive depending on the crash clone;
			// it is tolerated, but ordering above still had to hold.
			prev = idx
			continue
		}
		value, vErr := it.ValueAndErr()
		if vErr != nil {
			return r.fail("reading ordered value at key index %d failed: %w", idx, vErr)
		}
		if !bytes.Equal(value, makeValue(idx, ex.ValueSize)) {
			r.TornValues = appendIfMissing(r.TornValues, idx)
		}
		r.PresentKeys++
		prev = idx
	}
	if err := it.Error(); err != nil {
		return r.fail("ordered replay scan failed: %w", err)
	}
	if err := it.Close(); err != nil {
		return r.fail("closing replay iterator failed: %w", err)
	}

	if len(r.MissingKeys) > 0 {
		return r.fail("%d expected keys missing after replay, first indices %v",
			len(r.MissingKeys), firstFew(r.MissingKeys))
	}
	if len(r.TornValues) > 0 {
		return r.fail("%d values torn or mismatched after replay, first indices %v",
			len(r.TornValues), firstFew(r.TornValues))
	}
	if r.OutOfOrder {
		return r.fail("replayed keys are out of order at key index %d", prev)
	}
	if r.PresentKeys < ex.ExpectedKeys {
		return r.fail("ordered scan found %d scenario keys, expected at least %d",
			r.PresentKeys, ex.ExpectedKeys)
	}
	return r
}

func stripKeyPrefix(key []byte) []byte {
	if len(key) >= len(keyPrefix)+8 {
		return key[len(keyPrefix):]
	}
	return nil
}

func appendIfMissing(xs []int, x int) []int {
	for _, v := range xs {
		if v == x {
			return xs
		}
	}
	return append(xs, x)
}

func firstFew(xs []int) []int {
	if len(xs) > 10 {
		return xs[:10]
	}
	return xs
}
