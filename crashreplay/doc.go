// Package crashreplay provides a crash-reproduction and replay-verification
// facility wired into pebble's existing write and restart paths.
//
// The package is split into four cooperating pieces, all of which are
// exercised by the regression gate (see gate.go):
//
//   - crash.go reproduces a process that is hard-killed mid-flight, whether
//     the kill lands in the middle of a write, while a flush is stalled, or
//     before the WAL record finished landing. It reports the exact stage at
//     which the process died, and it refuses to declare success on an empty
//     directory or a directory containing only debris.
//   - verify.go reopens the same data and verifies that every key that had to
//     survive is present with its full value, in a stable order. A missing
//     key, a torn value, or an opaque replay-corruption error halts
//     verification instead of being papered over.
//   - sync.go pins down the fsync paths: durability declarations made with
//     Sync are backed by a real filesystem sync, and a failure at that sync is
//     propagated rather than swallowed.
//   - gate.go ties the three together into a single regression gate.
//
// None of the machinery stands up a parallel key-value store: writes go
// through *pebble.DB (Batch.Commit), the hard-kill window is simulated with
// vfs.CrashableMem.CrashClone, and recovery is the ordinary pebble.Open
// replay path.
package crashreplay
