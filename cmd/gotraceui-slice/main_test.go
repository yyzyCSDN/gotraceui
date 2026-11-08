package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"honnef.co/go/gotraceui/trace/tracev2"
	"honnef.co/go/gotraceui/trace/tslice"
)

// validTrace builds a minimal trace the slicer accepts, using only the public
// tracev2 API.
func validTrace(t *testing.T, v tracev2.Version) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := tracev2.NewWriter(&out, v)
	if err != nil {
		t.Fatal(err)
	}
	if v >= tracev2.VersionGo125 {
		_ = w.WriteBatch(&tracev2.RawBatch{Gen: 1, M: tracev2.NoThread, TimeUnits: 50, Events: []tracev2.RawEvent{
			{Type: tracev2.EvSync},
			{Type: tracev2.EvFrequency, Args: []uint64{1e9}},
			{Type: tracev2.EvClockSnapshot, Args: []uint64{1, 1, 1, 1}, AbsUnits: 51},
		}})
	} else {
		_ = w.WriteBatch(&tracev2.RawBatch{
			Gen: 1, M: tracev2.NoThread, TimeUnits: 50,
			Events: []tracev2.RawEvent{{Type: tracev2.EvFrequency, Args: []uint64{1e9}}},
		})
	}
	_ = w.WriteBatch(&tracev2.RawBatch{Gen: 1, M: 0, TimeUnits: 100, Events: []tracev2.RawEvent{
		{Type: tracev2.EvProcStatus, Args: []uint64{1, 0, 1}, AbsUnits: 101},
		{Type: tracev2.EvGoStatus, Args: []uint64{2, 1, 0, 2}, AbsUnits: 102},
		{Type: tracev2.EvGoBlock, Args: []uint64{8, 0, 0}, AbsUnits: 110},
	}})
	_ = w.WriteBatch(&tracev2.RawBatch{Gen: 1, M: tracev2.NoThread, TimeUnits: 90, Events: []tracev2.RawEvent{
		{Type: tracev2.EvStrings},
	}})
	return out.Bytes()
}

func TestCLISuccess(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.trace")
	out := filepath.Join(dir, "out.trace")
	manifest := filepath.Join(dir, "out.json")
	if err := os.WriteFile(in, validTrace(t, tracev2.VersionGo123), 0644); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"-goroutines", "1",
		"-window", "100:120",
		"-o", out,
		"-manifest", manifest,
		in,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("slice missing: %v", err)
	}
	mj, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	var m tslice.Manifest
	if err := json.Unmarshal(mj, &m); err != nil {
		t.Fatalf("manifest not valid JSON: %v", err)
	}
	if m.Source.Path != in {
		t.Fatalf("source path not recorded: %q", m.Source.Path)
	}
}

func TestCLIRepeatedGoroutineFlag(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.trace")
	if err := os.WriteFile(in, validTrace(t, tracev2.VersionGo123), 0644); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"-goroutines", "1", "-goroutines", "2,3",
		"-window", "100:120",
		"-o", filepath.Join(dir, "o"),
		"-manifest", filepath.Join(dir, "m"),
		in,
	})
	// g2/g3 do not exist, so this must fail selection; the flag itself must
	// parse all three IDs without a usage error.
	if !errors.Is(err, tslice.ErrInvalidSelection) {
		t.Fatalf("got %v, want ErrInvalidSelection", err)
	}
}

func TestCLIInvalidSelectionLeavesNoArtifacts(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.trace")
	out := filepath.Join(dir, "out.trace")
	manifest := filepath.Join(dir, "out.json")
	if err := os.WriteFile(in, validTrace(t, tracev2.VersionGo123), 0644); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"-goroutines", "99",
		"-window", "100:120",
		"-o", out,
		"-manifest", manifest,
		in,
	})
	if !errors.Is(err, tslice.ErrInvalidSelection) {
		t.Fatalf("got %v, want ErrInvalidSelection", err)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("slice artifact exists after failure")
	}
	if _, err := os.Stat(manifest); !os.IsNotExist(err) {
		t.Fatalf("manifest artifact exists after failure")
	}
}

func TestCLICorruptInputLeavesNoArtifacts(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.trace")
	out := filepath.Join(dir, "out.trace")
	manifest := filepath.Join(dir, "out.json")
	if err := os.WriteFile(in, []byte("not a trace at all"), 0644); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"-goroutines", "1", "-window", "100:120",
		"-o", out, "-manifest", manifest, in,
	})
	if !errors.Is(err, tslice.ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("slice artifact exists after failure")
	}
	if _, err := os.Stat(manifest); !os.IsNotExist(err) {
		t.Fatalf("manifest artifact exists after failure")
	}
}

func TestCLIUsageErrors(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.trace")
	if err := os.WriteFile(in, validTrace(t, tracev2.VersionGo123), 0644); err != nil {
		t.Fatal(err)
	}
	base := []string{"-goroutines", "1", "-window", "100:120",
		"-o", filepath.Join(dir, "o"), "-manifest", filepath.Join(dir, "m"), in}
	if err := run(append([]string{}, base[:len(base)-1]...)); err == nil {
		t.Fatalf("missing input should fail")
	}
	if err := run(append([]string{"-goroutines", "0"}, base...)); err == nil {
		t.Fatalf("non-positive goroutine ID should fail")
	}
	if err := run(append([]string{"-window", "120:100"}, base...)); err == nil {
		t.Fatalf("inverted window should fail")
	}
	if err := run(append([]string{"-o", ""}, base...)); err == nil {
		t.Fatalf("missing -o should fail")
	}
}
