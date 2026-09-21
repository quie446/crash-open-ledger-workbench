package crashreplay

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// SyncAuditReport is the outcome of the durability-sync audit.
type SyncAuditReport struct {
	// SyncCount is the number of real fsync operations observed while the
	// audited workload ran.
	SyncCount int
	// SyncErrorPropagated records that a failing fsync was returned from the
	// file's Sync method instead of being swallowed.
	SyncErrorPropagated bool
	Err                 error
}

// countingInjector observes sync-bearing filesystem operations.
type countingInjector struct {
	count atomic.Int64
}

func (in *countingInjector) MaybeError(op errorfs.Op) error {
	switch op.Kind {
	case errorfs.OpFileSync, errorfs.OpFileSyncData, errorfs.OpFileSyncTo:
		in.count.Add(1)
	}
	return nil
}
func (in *countingInjector) String() string { return "crashreplay/sync-counting-injector" }

// AuditSyncDeclaration drives a Synced write workload through pebble and
// asserts that declaring Sync is backed by a real filesystem sync. It changes
// no defaults (in particular it does not paper over a gap by forcing extra
// syncs everywhere); it only observes. If a Synced commit lands with zero
// real syncs behind it, the declaration and the behavior disagree and the
// audit fails loudly.
func AuditSyncDeclaration(fs vfs.FS, makeOptions func(vfs.FS) *Options) *SyncAuditReport {
	r := &SyncAuditReport{}
	counter := &countingInjector{}
	obsFS := errorfs.Wrap(fs, counter)
	opts := makeOptions(obsFS)
	opts.FS = obsFS
	db, err := Open("sync-audit", opts)
	if err != nil {
		r.Err = fmt.Errorf("crashreplay: sync audit open failed: %w", err)
		return r
	}
	before := counter.count.Load()
	b := db.NewBatch()
	if err := b.Set(encodeKey(0), makeValue(0, 64), nil); err != nil {
		r.Err = fmt.Errorf("crashreplay: sync audit set failed: %w", err)
		return r
	}
	if err := b.Commit(Sync); err != nil {
		r.Err = fmt.Errorf("crashreplay: sync audit commit failed: %w", err)
		return r
	}
	r.SyncCount = int(counter.count.Load() - before)
	if r.SyncCount == 0 {
		r.Err = errors.New("crashreplay: Sync declared but no real filesystem sync observed; declaration does not match behavior")
		return r
	}
	if err := db.Close(); err != nil {
		r.Err = fmt.Errorf("crashreplay: sync audit close failed: %w", err)
		return r
	}
	return r
}

// failingSyncFile wraps a vfs.File and makes the next Sync/SyncData return an
// injected error. It proves the fsync path propagates failures rather than
// swallowing them, at the layer that owns the fsync.
type failingSyncFile struct {
	vfs.File
	failed atomic.Bool
}

func (f *failingSyncFile) Sync() error {
	f.failed.Store(true)
	return errorfs.ErrInjected
}
func (f *failingSyncFile) SyncData() error {
	f.failed.Store(true)
	return errorfs.ErrInjected
}
func (f *failingSyncFile) SyncTo(int64) (bool, error) {
	f.failed.Store(true)
	return false, errorfs.ErrInjected
}

type failingSyncFS struct {
	vfs.FS
	file *failingSyncFile
}

func (fs *failingSyncFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(name, ".log") {
		fs.file = &failingSyncFile{File: f}
		return fs.file, nil
	}
	return f, nil
}

// AuditSyncErrorPropagation creates a WAL file whose fsync fails and asserts
// the failure is returned from Sync, not swallowed. Pebble treats a failed
// durability sync as a fatal condition by design, so the check pins the
// contract at the filesystem boundary it flows through.
func AuditSyncErrorPropagation(fs vfs.FS, makeOptions func(vfs.FS) *Options) *SyncAuditReport {
	r := &SyncAuditReport{}
	wrapped := &failingSyncFS{FS: fs}
	if err := fs.MkdirAll("sync-fail-audit", 0o755); err != nil {
		r.Err = fmt.Errorf("crashreplay: mkdir failing-sync audit failed: %w", err)
		return r
	}
	f, err := wrapped.Create("sync-fail-audit/000001.log", vfs.WriteCategoryUnspecified)
	if err != nil {
		r.Err = fmt.Errorf("crashreplay: create failing-sync WAL failed: %w", err)
		return r
	}
	if _, err := f.Write([]byte("x")); err != nil {
		r.Err = fmt.Errorf("crashreplay: write to failing-sync WAL failed: %w", err)
		return r
	}
	if err := f.Sync(); err == nil {
		r.Err = errors.New("crashreplay: injected fsync failure was swallowed by File.Sync")
		return r
	}
	if !wrapped.file.failed.Load() {
		r.Err = errors.New("crashreplay: failing sync was never invoked; sync declaration skipped the real fsync")
		return r
	}
	r.SyncErrorPropagated = true
	return r
}
