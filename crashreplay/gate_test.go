package crashreplay_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/crashreplay"
	"github.com/cockroachdb/pebble/vfs"
)

func makeTestOptions(fs vfs.FS) *pebble.Options {
	return &pebble.Options{
		FS:                 fs,
		FormatMajorVersion: pebble.FormatNewest,
	}
}

// TestGatePasses is the regression gate: kill at every stage, reopen the same
// data, read the synced keys back, run the sync audits, and pin the compaction
// scheduler files.
func TestGatePasses(t *testing.T) {
	baseline, err := crashreplay.FileHashes("..", crashreplay.ProtectedCompactionFiles)
	if err != nil {
		t.Fatal(err)
	}
	report := crashreplay.RunGate(crashreplay.GateConfig{
		RepoRoot:            "..",
		ValueSize:           128,
		TotalWrites:         40,
		SyncedWrites:        20,
		UnsyncedDataPercent: 10,
		MakeOptions:         makeTestOptions,
	}, baseline)
	if !report.Passed {
		for _, b := range report.Blocked {
			t.Errorf("gate blocked: %s", b)
		}
	}
}

func TestReproduceCleanShutdownStillLands(t *testing.T) {
	sc := crashreplay.Scenario{
		Name: "clean", DirName: "clean", Stage: crashreplay.StageNone,
		TotalWrites: 10, SyncedWrites: 10, ValueSize: 64,
	}
	cr, fs := crashreplay.Reproduce(sc, makeTestOptions)
	if cr.Err != nil {
		t.Fatalf("clean shutdown reproduction failed: %v", cr.Err)
	}
	if cr.SyncedKeys != 10 || cr.SurvivingCoreFiles == 0 {
		t.Fatalf("clean shutdown did not land: %+v", cr)
	}
	vr := crashreplay.VerifyReopen(fs, makeTestOptions, crashreplay.VerifyExpectations{
		DirName: "clean", ValueSize: 64, ExpectedKeys: 10,
	})
	if vr.Err != nil {
		t.Fatalf("clean shutdown replay failed: %v", vr.Err)
	}
}

func TestReproduceEachCrashStageReplays(t *testing.T) {
	for _, st := range []crashreplay.Stage{
		crashreplay.StageWrite,
		crashreplay.StageFlush,
		crashreplay.StageWALAppend,
	} {
		t.Run(st.String(), func(t *testing.T) {
			sc := crashreplay.Scenario{
				Name: "stage-" + st.String(), DirName: "stage-" + st.String(),
				Stage: st, TotalWrites: 60, SyncedWrites: 20,
				UnsyncedDataPercent: 10, Seed: 99, ValueSize: 96,
			}
			cr, fs := crashreplay.Reproduce(sc, makeTestOptions)
			if cr.Err != nil {
				t.Fatalf("reproduce %s: %v", st, cr.Err)
			}
			if !cr.StageReached {
				t.Fatalf("kill did not land at stage %s", st)
			}
			if cr.SyncedKeys == 0 {
				t.Fatalf("no synced keys recorded at stage %s", st)
			}
			vr := crashreplay.VerifyReopen(fs, makeTestOptions, crashreplay.VerifyExpectations{
				DirName: sc.DirName, ValueSize: 96, ExpectedKeys: cr.SyncedKeys,
			})
			if vr.Err != nil {
				t.Fatalf("replay after %s stopped: %v", st, vr.Err)
			}
			if vr.PresentKeys < cr.SyncedKeys {
				t.Fatalf("count mismatch at %s: synced=%d replayed=%d", st, cr.SyncedKeys, vr.PresentKeys)
			}
		})
	}
}

func TestSyncAudits(t *testing.T) {
	if r := crashreplay.AuditSyncDeclaration(vfs.NewCrashableMem(), makeTestOptions); r.Err != nil {
		t.Fatalf("sync declaration audit: %v", r.Err)
	}
	if r := crashreplay.AuditSyncErrorPropagation(vfs.NewCrashableMem(), makeTestOptions); r.Err != nil {
		t.Fatalf("sync error propagation audit: %v", r.Err)
	}
}

// TestVerifyFailsWithReason ensures a missing key stops verification with a
// specific reason instead of a silent pass.
func TestVerifyFailsWithReason(t *testing.T) {
	sc := crashreplay.Scenario{
		Name: "reason", DirName: "reason", Stage: crashreplay.StageWrite,
		TotalWrites: 4, SyncedWrites: 4, ValueSize: 64, Seed: 7,
	}
	cr, fs := crashreplay.Reproduce(sc, makeTestOptions)
	if cr.Err != nil {
		t.Fatal(cr.Err)
	}
	vr := crashreplay.VerifyReopen(fs, makeTestOptions, crashreplay.VerifyExpectations{
		DirName: sc.DirName, ValueSize: 64, ExpectedKeys: cr.SyncedKeys + 5,
	})
	if vr.Err == nil {
		t.Fatal("verification should stop when expected keys are missing")
	}
	if len(vr.MissingKeys) == 0 {
		t.Fatalf("missing keys were not reported: %+v", vr)
	}
}

// TestGateBlocksProtectedFileChange ensures the gate stays locked when the
// unrelated compaction scheduler drifts.
func TestGateBlocksProtectedFileChange(t *testing.T) {
	baseline := map[string]string{
		"compaction_scheduler.go": "0000000000000000000000000000000000000000000000000000000000000000",
	}
	report := crashreplay.RunGate(crashreplay.GateConfig{
		RepoRoot:    "..",
		MakeOptions: makeTestOptions,
	}, baseline)
	if report.Passed {
		t.Fatal("gate must not pass with a drifted compaction scheduler baseline")
	}
	found := false
	for _, b := range report.Blocked {
		if strings.Contains(b, "compaction scheduling files were modified") {
			found = true
		}
	}
	if !found {
		t.Fatalf("gate did not explain the blocked compaction change: %v", report.Blocked)
	}
}

// TestReproduceDeterministic ensures the same seed reproduces the same synced
// key counts and replay order across runs (the cross-machine stability
// requirement).
func TestReproduceDeterministic(t *testing.T) {
	run := func() int {
		sc := crashreplay.Scenario{
			Name: "det", DirName: "det", Stage: crashreplay.StageWALAppend,
			TotalWrites: 50, SyncedWrites: 25, Seed: 1234, ValueSize: 64,
		}
		cr, fs := crashreplay.Reproduce(sc, makeTestOptions)
		if cr.Err != nil {
			t.Fatal(cr.Err)
		}
		vr := crashreplay.VerifyReopen(fs, makeTestOptions, crashreplay.VerifyExpectations{
			DirName: sc.DirName, ValueSize: 64, ExpectedKeys: cr.SyncedKeys,
		})
		if vr.Err != nil {
			t.Fatal(vr.Err)
		}
		return vr.PresentKeys
	}
	first := run()
	for i := 0; i < 3; i++ {
		if got := run(); got != first {
			t.Fatalf("replay count not stable: first=%d got=%d", first, got)
		}
	}
}

// TestReplayMissingDirectoryReportsReason reopens a missing database directory
// and requires an explicit failure instead of a silent pass.
func TestReplayMissingDirectoryReportsReason(t *testing.T) {
	mem := vfs.NewCrashableMem()
	vr := crashreplay.VerifyReopen(mem, makeTestOptions, crashreplay.VerifyExpectations{
		DirName: "never-existed", ValueSize: 64, ExpectedKeys: 1,
	})
	if vr.Err == nil {
		t.Fatal("reopening a missing directory with expected keys must fail verification")
	}
	if len(vr.MissingKeys) == 0 {
		t.Fatalf("failure must name the missing keys: %v", vr.Err)
	}
}

// TestReplayDebrisOnlyDirReportsReason covers the "only leftovers" case via
// the gate's crash report: a directory containing just non-core files must
// count as not reproduced.
func TestReplayDebrisOnlyScenario(t *testing.T) {
	mem := vfs.NewCrashableMem()
	if err := mem.MkdirAll("debris-only", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := mem.Create("debris-only/OPTIONS-000001", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Pebble creates an empty database when opening a directory without a
	// MANIFEST. Verification against expected keys must therefore reject the
	// debris-only directory with a specific missing-key reason.
	vr := crashreplay.VerifyReopen(mem, makeTestOptions, crashreplay.VerifyExpectations{
		DirName: "debris-only", ValueSize: 64, ExpectedKeys: 1,
	})
	if vr.Err == nil {
		t.Fatal("verification must not pass on a debris-only directory")
	}
	if len(vr.MissingKeys) == 0 {
		t.Fatalf("failure must name the missing keys: %v", vr.Err)
	}
}
