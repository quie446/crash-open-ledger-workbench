package crashreplay

import (
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// Stage identifies the point in the write/restart path at which the process
// is hard-killed.
type Stage int

const (
	// StageNone does not kill the process; it exercises the ordinary clean
	// shutdown path, which must keep working after the hardening.
	StageNone Stage = iota
	// StageWrite kills while only part of the synced write prefix is down.
	StageWrite
	// StageFlush kills while a memtable flush is stalled writing an sstable.
	StageFlush
	// StageWALAppend leaves the WAL with a physically torn tail record.
	StageWALAppend
)

// String implements fmt.Stringer.
func (s Stage) String() string {
	switch s {
	case StageNone:
		return "clean-shutdown"
	case StageWrite:
		return "mid-write"
	case StageFlush:
		return "flush-stall"
	case StageWALAppend:
		return "wal-append-truncated"
	default:
		return fmt.Sprintf("unknown-stage(%d)", int(s))
	}
}

// AllStages returns the kill stages exercised by the gate.
func AllStages() []Stage {
	return []Stage{StageNone, StageWrite, StageFlush, StageWALAppend}
}

// Scenario configures one crash reproduction.
type Scenario struct {
	Name                string
	DirName             string
	Stage               Stage
	TotalWrites         int
	SyncedWrites        int
	UnsyncedDataPercent int
	Seed                uint64
	ValueSize           int
}

// CrashReport describes the outcome of a crash reproduction. A non-nil Err
// means the reproduction itself could not be driven to a state worth
// verifying; the gate treats it as a hard failure instead of passing debris
// downstream.
type CrashReport struct {
	Scenario           string
	Stage              Stage
	StageReached       bool
	SyncedKeys         int
	SurvivingCoreFiles int
	DebrisFiles        []string
	Err                error
}

func (r *CrashReport) fail(reason error) *CrashReport {
	r.Err = reason
	return r
}

// flushStallInjector blocks creation of flush-output sstables until released,
// reproducing "flush stuck on disk" without racing the flush goroutine.
type flushStallInjector struct {
	stalled atomic.Bool
	release chan struct{}
}

func newFlushStallInjector() *flushStallInjector {
	return &flushStallInjector{release: make(chan struct{})}
}

func (in *flushStallInjector) MaybeError(op errorfs.Op) error {
	if op.Kind != errorfs.OpCreate && op.Kind != errorfs.OpReuseForWrite {
		return nil
	}
	if !strings.HasSuffix(op.Path, ".sst") {
		return nil
	}
	in.stalled.Store(true)
	<-in.release
	return nil
}
func (in *flushStallInjector) String() string { return "crashreplay/flush-stall-injector" }

type errDirMissing struct{ dir string }

func (e errDirMissing) Error() string {
	return fmt.Sprintf("crashreplay: database directory %q is missing after kill", e.dir)
}

func isNotExist(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such file")
}

// classifyDir reports the core database files and debris left after a kill.
func classifyDir(fs vfs.FS, dirName string) (core []string, debris []string, err error) {
	entries, listErr := fs.List(dirName)
	if listErr != nil {
		if isNotExist(listErr) {
			return nil, nil, errDirMissing{dir: dirName}
		}
		return nil, nil, fmt.Errorf("crashreplay: list %q after kill failed: %w", dirName, listErr)
	}
	for _, name := range entries {
		switch {
		case strings.HasSuffix(name, ".log"),
			strings.HasSuffix(name, ".sst"),
			strings.HasPrefix(name, "MANIFEST-"),
			name == "CURRENT",
			strings.HasPrefix(name, "marker."):
			core = append(core, name)
		default:
			debris = append(debris, name)
		}
	}
	return core, debris, nil
}

// Reproduce opens a fresh database on a crashable filesystem, drives the
// configured write workload, hard-kills the process at the configured stage,
// and returns the crash-consistent filesystem clone that must be reopened and
// replayed. Failure to reach the stage, an empty directory, or debris-only
// leftovers are reported as errors instead of being handed downstream.
func Reproduce(sc Scenario, makeOptions func(vfs.FS) *Options) (*CrashReport, *vfs.MemFS) {
	if sc.DirName == "" {
		return (&CrashReport{Scenario: sc.Name, Stage: sc.Stage}).
			fail(fmt.Errorf("crashreplay: scenario %q has empty DirName", sc.Name)), nil
	}
	if sc.ValueSize < 16 {
		sc.ValueSize = 16
	}
	if sc.SyncedWrites > sc.TotalWrites {
		return (&CrashReport{Scenario: sc.Name, Stage: sc.Stage}).
			fail(fmt.Errorf("crashreplay: scenario %q has more synced writes than total writes", sc.Name)), nil
	}
	if sc.Stage == StageNone {
		return reproduceCleanShutdown(sc, makeOptions)
	}

	rng := rand.New(rand.NewPCG(1, sc.Seed))
	report := &CrashReport{Scenario: sc.Name, Stage: sc.Stage}

	mem := vfs.NewCrashableMem()
	openOpts := makeOptions(mem)
	var stall *flushStallInjector
	if sc.Stage == StageFlush {
		if openOpts.MemTableSize == 0 || openOpts.MemTableSize > 128<<10 {
			openOpts.MemTableSize = 128 << 10
		}
		stall = newFlushStallInjector()
		openOpts.FS = errorfs.Wrap(mem, stall)
	}

	db, err := Open(sc.DirName, openOpts)
	if err != nil {
		return report.fail(fmt.Errorf("crashreplay: open before crash failed: %w", err)), nil
	}

	var commitErr error
	switch sc.Stage {
	case StageWrite:
		for i := 0; i < sc.SyncedWrites; i++ {
			key, value := makeKV(i, sc.ValueSize)
			b := db.NewBatch()
			if err := b.Set(key, value, nil); err != nil {
				commitErr = err
				break
			}
			if err := b.Commit(Sync); err != nil {
				commitErr = fmt.Errorf("crashreplay: synced write %d failed: %w", i, err)
				break
			}
			report.SyncedKeys++
		}
		report.StageReached = commitErr == nil

	case StageWALAppend:
		// Commit the durable prefix plus an unsynced tail. After the hard
		// kill the WAL tail is physically truncated (see truncateWALTail);
		// replay must expose exactly the synced prefix and say where it
		// stopped.
		for i := 0; i < sc.TotalWrites; i++ {
			key, value := makeKV(i, sc.ValueSize)
			b := db.NewBatch()
			if err := b.Set(key, value, nil); err != nil {
				commitErr = err
				break
			}
			commit := NoSync
			if i < sc.SyncedWrites {
				commit = Sync
			}
			if err := b.Commit(commit); err != nil {
				commitErr = err
				break
			}
			if commit == Sync {
				report.SyncedKeys++
			}
		}
		report.StageReached = commitErr == nil

	case StageFlush:
		// Flood the memtable in the background, then force a flush. The
		// flush parks on the sstable-create injector while writes keep going.
		stop := make(chan struct{})
		writeDone := make(chan error, 1)
		go func() {
			i := 0
			for {
				select {
				case <-stop:
					writeDone <- nil
					return
				default:
				}
				key, value := makeKV(i, sc.ValueSize)
				b := db.NewBatch()
				if err := b.Set(key, value, nil); err != nil {
					writeDone <- err
					return
				}
				if err := b.Commit(Sync); err != nil {
					writeDone <- err
					return
				}
				i++
				report.SyncedKeys = i
			}
		}()
		// Writing past the memtable limit schedules a flush automatically;
		// wait for the flush to park on sstable creation.
		if !waitFor(&stall.stalled, 15*time.Second) {
			close(stop)
			close(stall.release)
			<-writeDone
			_ = db.Close()
			return report.fail(fmt.Errorf(
				"crashreplay: stage %s was not reached: flush never parked on sstable creation",
				sc.Stage)), nil
		}
		close(stop)
		<-writeDone
		report.StageReached = true
	}

	// Hard kill: take a crash-consistent clone. Outstanding writers never get
	// to Close, exactly like a SIGKILL.
	clone := mem.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: sc.UnsyncedDataPercent,
		RNG:                 rng,
	})
	if stall != nil {
		close(stall.release)
	}
	_ = db.Close()

	if !report.StageReached {
		return report.fail(fmt.Errorf(
			"crashreplay: stage %s was not reached (last commit error: %v)", sc.Stage, commitErr)), nil
	}
	if sc.Stage == StageWALAppend {
		if err := truncateWALTail(clone, sc.DirName, rng); err != nil {
			return report.fail(err), nil
		}
	}

	coreFiles, debris, err := classifyDir(clone, sc.DirName)
	if err != nil {
		return report.fail(err), nil
	}
	report.SurvivingCoreFiles = len(coreFiles)
	report.DebrisFiles = debris
	if report.SyncedKeys > 0 && len(coreFiles) == 0 {
		entries, _ := clone.List(sc.DirName)
		if len(entries) == 0 {
			return report.fail(fmt.Errorf(
				"crashreplay: kill at stage %s left an empty directory despite %d synced writes",
				sc.Stage, report.SyncedKeys)), nil
		}
		return report.fail(fmt.Errorf(
			"crashreplay: kill at stage %s left only debris %v despite %d synced writes",
			sc.Stage, debris, report.SyncedKeys)), nil
	}
	return report, clone
}

func reproduceCleanShutdown(sc Scenario, makeOptions func(vfs.FS) *Options) (*CrashReport, *vfs.MemFS) {
	report := &CrashReport{Scenario: sc.Name, Stage: StageNone}
	mem := vfs.NewCrashableMem()
	db, err := Open(sc.DirName, makeOptions(mem))
	if err != nil {
		return report.fail(fmt.Errorf("crashreplay: open before clean shutdown failed: %w", err)), nil
	}
	for i := 0; i < sc.TotalWrites; i++ {
		key, value := makeKV(i, sc.ValueSize)
		if err := db.Set(key, value, nil); err != nil {
			return report.fail(fmt.Errorf("crashreplay: write %d failed: %w", i, err)), nil
		}
	}
	if err := db.Close(); err != nil {
		return report.fail(fmt.Errorf("crashreplay: clean close failed: %w", err)), nil
	}
	report.StageReached = true
	report.SyncedKeys = sc.TotalWrites
	coreFiles, debris, err := classifyDir(mem, sc.DirName)
	if err != nil {
		return report.fail(err), nil
	}
	report.SurvivingCoreFiles = len(coreFiles)
	report.DebrisFiles = debris
	return report, mem
}

func waitFor(flag *atomic.Bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if flag.Load() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return flag.Load()
}

// truncateWALTail finds the largest WAL (*.log) in dirName on the post-kill
// clone and removes between 1 and 7 bytes from its end, leaving a physically
// torn tail record that replay must handle by stopping at the last complete
// record.
func truncateWALTail(fs vfs.FS, dirName string, rng *rand.Rand) error {
	entries, err := fs.List(dirName)
	if err != nil {
		return fmt.Errorf("crashreplay: list %q to truncate WAL failed: %w", dirName, err)
	}
	var walName string
	var walSize int64
	for _, name := range entries {
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		info, statErr := fs.Stat(fs.PathJoin(dirName, name))
		if statErr != nil {
			return fmt.Errorf("crashreplay: stat WAL %q failed: %w", name, statErr)
		}
		if info.Size() > walSize {
			walSize = info.Size()
			walName = name
		}
	}
	if walName == "" {
		return fmt.Errorf("crashreplay: stage %s could not be reached: no WAL file present after kill", StageWALAppend)
	}
	fullPath := fs.PathJoin(dirName, walName)
	f, err := fs.Open(fullPath)
	if err != nil {
		return fmt.Errorf("crashreplay: open WAL %q for truncation failed: %w", walName, err)
	}
	data := make([]byte, walSize)
	if _, err := io.ReadFull(f, data); err != nil {
		_ = f.Close()
		return fmt.Errorf("crashreplay: read WAL %q for truncation failed: %w", walName, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("crashreplay: close WAL %q after read failed: %w", walName, err)
	}
	out, err := fs.Create(fullPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return fmt.Errorf("crashreplay: recreate torn WAL %q failed: %w", walName, err)
	}
	if _, err := out.Write(data); err != nil {
		_ = out.Close()
		return fmt.Errorf("crashreplay: rewrite torn WAL %q failed: %w", walName, err)
	}
	// A zero record-type byte is not a valid record header; appending it
	// followed by a truncated payload leaves a fragment of a new record a hard
	// kill could have started. All synced records stay byte-for-byte intact,
	// so replay must surface exactly the synced prefix and stop at the
	// fragment with a clear recovery boundary.
	if _, err := out.Write([]byte{0}); err != nil {
		_ = out.Close()
		return fmt.Errorf("crashreplay: append torn WAL header %q failed: %w", walName, err)
	}
	fragment := make([]byte, 1+rng.IntN(4))
	for i := range fragment {
		fragment[i] = byte(i + 1)
	}
	if _, err := out.Write(fragment); err != nil {
		_ = out.Close()
		return fmt.Errorf("crashreplay: append torn WAL fragment %q failed: %w", walName, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("crashreplay: close torn WAL %q failed: %w", walName, err)
	}
	return nil
}
