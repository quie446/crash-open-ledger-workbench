package crashreplay

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/pebble/vfs"
)

// ProtectedCompactionFiles are the unrelated compaction-scheduling sources the
// gate pins. A fix for crash replay must not move the compaction scheduler
// underneath it; if these files change, the gate stays locked.
var ProtectedCompactionFiles = []string{
	"compaction_scheduler.go",
	"compaction_picker.go",
	"read_compaction_queue.go",
}

// GateConfig configures RunGate.
type GateConfig struct {
	// RepoRoot is the pebble repository root; protected-file hashes are
	// resolved relative to it.
	RepoRoot string
	// ValueSize is used for every scenario.
	ValueSize int
	// TotalWrites / SyncedWrites configure the crash scenarios.
	TotalWrites  int
	SyncedWrites int
	// UnsyncedDataPercent is forwarded to the crash clone.
	UnsyncedDataPercent int
	// MakeOptions builds the pebble options used on every open.
	MakeOptions func(vfs.FS) *Options
}

// GateStageResult is one stage of the gate.
type GateStageResult struct {
	Name    string
	Stage   Stage
	Detail  string
	Skipped bool
	Err     error
}

// GateReport is the full gate outcome. The gate passes only if crash
// reproduction, replay verification, and the sync audits all pass, and the
// protected compaction files are untouched.
type GateReport struct {
	Stages  []GateStageResult
	Passed  bool
	Blocked []string
}

func (r *GateReport) fail(name string, st Stage, err error) {
	r.Stages = append(r.Stages, GateStageResult{Name: name, Stage: st, Err: err})
	r.Blocked = append(r.Blocked, fmt.Sprintf("%s: %v", name, err))
}

// FileHashes computes sha256 hashes of the protected files, relative to root.
func FileHashes(root string, files []string) (map[string]string, error) {
	hashes := make(map[string]string, len(files))
	for _, name := range files {
		f, err := os.Open(strings.TrimSuffix(root, "/") + "/" + name)
		if err != nil {
			return nil, fmt.Errorf("crashreplay: hash protected file %q: %w", name, err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("crashreplay: hash protected file %q: %w", name, err)
		}
		_ = f.Close()
		hashes[name] = fmt.Sprintf("%x", h.Sum(nil))
	}
	return hashes, nil
}

// CheckProtectedFiles fails if any protected file's hash differs from baseline.
func CheckProtectedFiles(root string, baseline map[string]string) error {
	current, err := FileHashes(root, keysOf(baseline))
	if err != nil {
		return err
	}
	var changed []string
	for name, want := range baseline {
		if current[name] != want {
			changed = append(changed, name)
		}
	}
	if len(changed) > 0 {
		return fmt.Errorf("crashreplay: unrelated compaction scheduling files were modified: %v", changed)
	}
	return nil
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// scenarioFor maps a stage to a crash scenario.
func scenarioFor(st Stage, cfg GateConfig) Scenario {
	total := cfg.TotalWrites
	if total == 0 {
		total = 64
	}
	synced := cfg.SyncedWrites
	if synced == 0 && st != StageWALAppend {
		synced = total / 2
	}
	if synced == 0 {
		synced = total / 2
	}
	name := "gate-" + st.String()
	return Scenario{
		Name:                name,
		DirName:             name,
		Stage:               st,
		TotalWrites:         total,
		SyncedWrites:        synced,
		UnsyncedDataPercent: cfg.UnsyncedDataPercent,
		Seed:                0x5eed,
		ValueSize:           cfg.ValueSize,
	}
}

// RunGate drives the full acceptance: each kill stage is reproduced, the
// post-kill data is reopened and verified against the synced key/value set,
// the durability-sync audits run, and the protected compaction files are
// checked against their baseline. Any stage error stops the gate and is
// reported with its reason; half-finished state is never passed downstream.
func RunGate(cfg GateConfig, protectedBaseline map[string]string) *GateReport {
	report := &GateReport{}
	if cfg.MakeOptions == nil {
		report.fail("gate/config", StageNone, fmt.Errorf("crashreplay: MakeOptions is required"))
		return report
	}
	if cfg.ValueSize == 0 {
		cfg.ValueSize = 128
	}

	for _, st := range AllStages() {
		sc := scenarioFor(st, cfg)
		crashReport, clone := Reproduce(sc, cfg.MakeOptions)
		if crashReport.Err != nil {
			report.fail("reproduce/"+st.String(), st, crashReport.Err)
			continue
		}
		if st != StageNone && crashReport.SyncedKeys == 0 {
			report.fail("reproduce/"+st.String(), st,
				fmt.Errorf("crashreplay: count mismatch: 0 synced keys recorded before kill"))
			continue
		}
		verifyReport := VerifyReopen(clone, cfg.MakeOptions, VerifyExpectations{
			DirName:      sc.DirName,
			ValueSize:    sc.ValueSize,
			ExpectedKeys: crashReport.SyncedKeys,
		})
		if verifyReport.Err != nil {
			report.fail("verify/"+st.String(), st, verifyReport.Err)
			continue
		}
		// Cross-check the count from two independent paths: what the writer
		// believes it synced must equal what replay presents.
		if verifyReport.PresentKeys < crashReport.SyncedKeys {
			report.fail("verify/"+st.String(), st, fmt.Errorf(
				"crashreplay: count mismatch: writer synced %d keys, replay found %d",
				crashReport.SyncedKeys, verifyReport.PresentKeys))
			continue
		}
		report.Stages = append(report.Stages, GateStageResult{
			Name:  "crash-replay/" + st.String(),
			Stage: st,
			Detail: fmt.Sprintf("synced=%d reopened=%v present=%d coreFiles=%d debris=%d",
				crashReport.SyncedKeys, verifyReport.Opened, verifyReport.PresentKeys,
				crashReport.SurvivingCoreFiles, len(crashReport.DebrisFiles)),
		})
	}

	// Sync audits on independent crashable filesystems.
	syncDecl := AuditSyncDeclaration(vfs.NewCrashableMem(), cfg.MakeOptions)
	if syncDecl.Err != nil {
		report.fail("sync/declaration", StageNone, syncDecl.Err)
	} else {
		report.Stages = append(report.Stages, GateStageResult{
			Name:   "sync/declaration",
			Detail: fmt.Sprintf("observed syncs per Synced commit=%d", syncDecl.SyncCount),
		})
	}
	syncErr := AuditSyncErrorPropagation(vfs.NewCrashableMem(), cfg.MakeOptions)
	if syncErr.Err != nil {
		report.fail("sync/error-propagation", StageNone, syncErr.Err)
	} else {
		report.Stages = append(report.Stages, GateStageResult{
			Name:   "sync/error-propagation",
			Detail: "injected fsync failure surfaced from Commit(Sync)",
		})
	}

	if len(protectedBaseline) > 0 {
		if err := CheckProtectedFiles(cfg.RepoRoot, protectedBaseline); err != nil {
			report.fail("gate/protected-files", StageNone, err)
		} else {
			report.Stages = append(report.Stages, GateStageResult{
				Name:   "gate/protected-files",
				Detail: "compaction scheduling files unchanged",
			})
		}
	}

	report.Passed = len(report.Blocked) == 0
	return report
}
