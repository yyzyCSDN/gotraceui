package tslice

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	exptrace "golang.org/x/exp/trace"

	"honnef.co/go/gotraceui/trace/ptrace"
	"honnef.co/go/gotraceui/trace/tracev2"
)

// allVersions is the supported non-experimental v2 matrix.
var allVersions = []tracev2.Version{tracev2.VersionGo122, tracev2.VersionGo123, tracev2.VersionGo125}

func statusBatch(tb *testBuilder, g *testGen, m int64, base uint64, gids []int64) *testBatch {
	// Enumerate proc and goroutine state in the working thread's own batch;
	// the reader accepts this layout for every supported version and it
	// establishes the scheduler context the later events need.
	const (
		procRunning = 1 // tracev2.ProcStatus value ProcRunning
		goRunning   = 2 // tracev2.GoStatus value GoRunning
	)
	b := g.Batch(m, base)
	t := base
	t++
	b.Event(tracev2.EvProcStatus, t, 0, procRunning)
	for _, gid := range gids {
		t++
		b.Event(tracev2.EvGoStatus, t, uint64(gid), uint64(m), goRunning)
	}
	return b
}

// exportAndParse runs the slicer and confirms both parser stacks accept the
// output.
func exportAndParse(t *testing.T, data []byte, sel Selection) (*Result, []exptrace.Event, *tracev2.RawTrace) {
	t.Helper()
	res, err := Export(bytes.NewReader(data), sel)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	// Public interpreted reader.
	pr, err := exptrace.NewReader(bytes.NewReader(res.Trace))
	if err != nil {
		t.Fatalf("output rejected by x/exp reader: %v", err)
	}
	var evs []exptrace.Event
	for {
		ev, err := pr.ReadEvent()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("output interpreted parse failed: %v", err)
		}
		evs = append(evs, ev)
	}
	// gotraceui's own enriched parser.
	pr2, err := exptrace.NewReader(bytes.NewReader(res.Trace))
	if err != nil {
		t.Fatalf("second reader failed: %v", err)
	}
	if _, err := ptrace.Parse(pr2, func(float64) {}); err != nil {
		t.Fatalf("ptrace rejected output: %v", err)
	}
	rr, err := tracev2.NewReader(bytes.NewReader(res.Trace))
	if err != nil {
		t.Fatalf("raw reader rejected output: %v", err)
	}
	raw, err := rr.ReadAll()
	if err != nil {
		t.Fatalf("raw parse of output failed: %v", err)
	}
	return res, evs, raw
}

func nonSync(evs []exptrace.Event) []exptrace.Event {
	var out []exptrace.Event
	for _, ev := range evs {
		if ev.Kind() != exptrace.EventSync {
			out = append(out, ev)
		}
	}
	return out
}

func countKind(evs []exptrace.Event, kind exptrace.EventKind) int {
	n := 0
	for _, ev := range evs {
		if ev.Kind() == kind {
			n++
		}
	}
	return n
}

// buildBasicScenario constructs a trace with:
//   - g1 (main) and g2 (worker) interacting via block/unblock,
//   - g3 emitting an independent log with dedicated strings and a stack,
//   - a CPU sample on g2 inside the interesting window.
func buildBasicScenario(v tracev2.Version) (*testBuilder, Selection) {
	tb := newTestBuilder(v)
	g := tb.Generation(1).SyncBase(50)
	statusBatch(tb, g, 0, 100, []int64{1})

	stkCreate := g.Stack(0x1000)
	stkBlock := g.Stack(0x2000)
	stkNoise := g.Stack(0x3000)
	reasonBlock := g.String("chan receive")
	noiseCat := g.String("noise-category")
	noiseMsg := g.String("noise-message")

	b := g.Batch(0, 200)
	// g1 creates g2, then blocks so g2 can be scheduled.
	b.Event(tracev2.EvGoCreate, 210, 2, stkCreate, stkBlock)
	b.Event(tracev2.EvGoBlock, 220, reasonBlock, stkBlock)
	b.Event(tracev2.EvGoStart, 230, 2, 1)
	b.Event(tracev2.EvGoBlock, 240, reasonBlock, stkBlock)
	b.Event(tracev2.EvGoUnblock, 250, 1, 1, 0)
	b.Event(tracev2.EvGoStart, 260, 1, 2)
	b.Event(tracev2.EvGoUnblock, 270, 2, 2, 0)
	b.Event(tracev2.EvGoBlock, 280, reasonBlock, stkBlock)
	b.Event(tracev2.EvGoStart, 290, 2, 3)
	b.Event(tracev2.EvGoUnblock, 300, 1, 3, 0)
	b.Event(tracev2.EvGoBlock, 310, reasonBlock, stkBlock)
	b.Event(tracev2.EvGoStart, 320, 1, 4)

	// Noisy goroutine g3, entirely outside the interesting window.
	b.Event(tracev2.EvGoCreate, 410, 3, stkCreate, stkNoise)
	b.Event(tracev2.EvGoBlock, 420, reasonBlock, stkNoise)
	b.Event(tracev2.EvGoStart, 430, 3, 1)
	b.Event(tracev2.EvUserLog, 440, 0, noiseCat, noiseMsg, stkNoise)
	b.Event(tracev2.EvGoBlock, 450, reasonBlock, stkNoise)

	// CPU samples: g2 inside the window (running 290..310), g3 outside.
	g.CPUSample(295, 0, 0, 2, stkBlock)
	g.CPUSample(445, 0, 0, 3, stkNoise)

	sel := Selection{Goroutines: []int64{2}, WindowA: 235, WindowB: 325}
	return tb, sel
}

func TestBasicSliceAllVersions(t *testing.T) {
	for _, v := range allVersions {
		t.Run(v.String(), func(t *testing.T) {
			tb, sel := buildBasicScenario(v)
			data := tb.Build()
			res, evs, raw := exportAndParse(t, data, sel)

			if got, want := raw.Version, v; got != want {
				t.Fatalf("output version = %v, want %v", got, want)
			}
			// The noisy g3 log must be gone, together with its dedicated
			// string and stack definitions.
			if n := countKind(evs, exptrace.EventLog); n != 0 {
				t.Fatalf("independent log events retained: %d", n)
			}
			outStrings := map[string]bool{}
			for _, s := range res.Manifest.Definitions.Strings {
				for _, e := range s {
					outStrings[e.Value] = true
				}
			}
			if outStrings["noise-category"] || outStrings["noise-message"] {
				t.Fatalf("noise-only strings survived: %v", outStrings)
			}
			var totalStacks int
			for _, ss := range res.Manifest.Definitions.Stacks {
				totalStacks += len(ss)
			}
			// Only stacks referenced by retained events (create/block/sample).
			if totalStacks == 0 {
				t.Fatalf("expected retained stacks")
			}
			for _, ss := range res.Manifest.Definitions.Stacks {
				for _, s := range ss {
					for _, f := range s.Frames {
						if f.PC == 0x3000 {
							t.Fatalf("noise-only stack 0x3000 survived")
						}
					}
				}
			}
			// The in-window g2 CPU sample survives.
			if n := countKind(evs, exptrace.EventStackSample); n != 1 {
				t.Fatalf("expected 1 retained stack sample, got %d", n)
			}
			// g3 itself must not appear in the output at all.
			for _, ev := range nonSync(evs) {
				if ev.Kind() == exptrace.EventStateTransition {
					st := ev.StateTransition()
					if st.Resource.Kind == exptrace.ResourceGoroutine && st.Resource.Goroutine() == 3 {
						t.Fatalf("goroutine 3 events leaked into slice: %s", ev)
					}
				}
			}
			// Roles and chains are populated.
			var seeds, supports int
			for _, rec := range res.Manifest.Events {
				switch rec.Role {
				case "selected":
					seeds++
				case "support":
					supports++
					if len(rec.DependencyChain) == 0 {
						t.Fatalf("support event without dependency chain: %+v", rec)
					}
				}
			}
			if seeds == 0 || supports == 0 {
				t.Fatalf("expected both seeds and support events, got %d/%d", seeds, supports)
			}
		})
	}
}

func TestClockCoordinatesPreserved(t *testing.T) {
	tb, sel := buildBasicScenario(tracev2.VersionGo123)
	res, _, _ := exportAndParse(t, tb.Build(), sel)
	// With a 1e9 freq, one timer unit is exactly one nanosecond: every event
	// must keep identical native units between source and output.
	for _, rec := range res.Manifest.Events {
		if rec.Source.TimerUnits != rec.Output.TimerUnits {
			t.Fatalf("native timer units drifted for %+v", rec)
		}
	}
}

// buildCrossGenScenario spans two generations with a task beginning in gen 1
// and ending in gen 2.
func buildCrossGenScenario(v tracev2.Version) (*testBuilder, Selection) {
	tb := newTestBuilder(v)
	g1 := tb.Generation(1).SyncBase(50)
	statusBatch(tb, g1, 0, 100, []int64{1})
	stk := g1.Stack(0xa1)
	taskName := g1.String("cross-gen-task")
	b1 := g1.Batch(0, 200)
	b1.Event(tracev2.EvUserTaskBegin, 210, 7, 0, taskName, stk)
	// g1 remains running at the generation boundary so the gen-2 status
	// enumeration is consistent.

	g2 := tb.Generation(2).SyncBase(5000)
	statusBatch(tb, g2, 0, 5100, []int64{1})
	// Stack and string IDs are per-generation tables.
	stk2 := g2.Stack(0xa2)
	b2 := g2.Batch(0, 5200)
	b2.Event(tracev2.EvUserTaskEnd, 5250, 7, stk2)
	b2.Event(tracev2.EvGoBlock, 5300, g2.String("sleep"), stk2)

	sel := Selection{Goroutines: []int64{1}, WindowA: 5200, WindowB: 5400}
	return tb, sel
}

func TestCrossGeneration(t *testing.T) {
	for _, v := range allVersions {
		t.Run(v.String(), func(t *testing.T) {
			tb, sel := buildCrossGenScenario(v)
			res, evs, raw := exportAndParse(t, tb.Build(), sel)
			if len(raw.Generations) != 2 {
				t.Fatalf("expected 2 output generations, got %d", len(raw.Generations))
			}
			if raw.Generations[0].Gen != 1 || raw.Generations[1].Gen != 2 {
				t.Fatalf("generation renumbering wrong: %d, %d", raw.Generations[0].Gen, raw.Generations[1].Gen)
			}
			// Task begin from gen 1 is retained as support for the gen-2 end.
			if n := countKind(evs, exptrace.EventTaskBegin); n != 1 {
				t.Fatalf("expected 1 task begin, got %d", n)
			}
			if n := countKind(evs, exptrace.EventTaskEnd); n != 1 {
				t.Fatalf("expected 1 task end, got %d", n)
			}
			var sawGen1Support bool
			for _, rec := range res.Manifest.Events {
				if rec.Source.Generation == 1 && rec.Role == "support" {
					sawGen1Support = true
				}
			}
			if !sawGen1Support {
				t.Fatalf("no gen-1 support events retained")
			}
		})
	}
}

// buildMigrationScenario moves a proc between threads while a goroutine sits
// in a blocking syscall, then reacquires it on the original thread.
func buildMigrationScenario(v tracev2.Version) (*testBuilder, Selection) {
	tb := newTestBuilder(v)
	g := tb.Generation(1).SyncBase(50)
	statusBatch(tb, g, 0, 100, []int64{1})
	stk := g.Stack(0xb1)
	reason := g.String("syscall")

	// M0: g1 running on p0 enters a syscall; p0 becomes syscall state.
	m0 := g.Batch(0, 200)
	m0.Event(tracev2.EvGoSyscallBegin, 210, 1, stk)
	// M1: sysmon steals p0 while g1 remains blocked in the syscall on M0.
	m1 := g.Batch(1, 300)
	m1.Event(tracev2.EvProcSteal, 310, 0, 2, 0)
	// M0 reacquires p0 and finishes the blocked syscall.
	m0b := g.Batch(0, 400)
	m0b.Event(tracev2.EvProcStart, 410, 0, 3)
	m0b.Event(tracev2.EvGoSyscallEndBlocked, 420)
	m0b.Event(tracev2.EvGoStart, 430, 1, 1)
	m0b.Event(tracev2.EvGoBlock, 440, reason, stk)

	sel := Selection{Goroutines: []int64{1}, WindowA: 415, WindowB: 450}
	return tb, sel
}

func TestProcMigration(t *testing.T) {
	for _, v := range allVersions {
		t.Run(v.String(), func(t *testing.T) {
			tb, sel := buildMigrationScenario(v)
			res, evs, _ := exportAndParse(t, tb.Build(), sel)
			// The steal must survive as support, and g1's full transition
			// sequence must replay.
			var sawSteal bool
			for _, ev := range nonSync(evs) {
				if ev.Kind() == exptrace.EventStateTransition {
					st := ev.StateTransition()
					if st.Resource.Kind == exptrace.ResourceProc {
						from, to := st.Proc()
						if from == exptrace.ProcRunning && to == exptrace.ProcIdle {
							sawSteal = true
						}
					}
				}
			}
			if !sawSteal {
				t.Fatalf("proc steal not retained")
			}
			if res.Manifest.Boundaries == nil {
				t.Fatalf("expected boundary records")
			}
		})
	}
}

// buildCrossWindowAnnotationScenario starts a region before the window and
// ends it inside the window.
func buildCrossWindowAnnotationScenario(v tracev2.Version) (*testBuilder, Selection) {
	tb := newTestBuilder(v)
	g := tb.Generation(1).SyncBase(50)
	statusBatch(tb, g, 0, 100, []int64{1})
	stk := g.Stack(0xc1)
	regionName := g.String("interesting-region")
	b := g.Batch(0, 200)
	b.Event(tracev2.EvUserRegionBegin, 250, 0, regionName, stk) // outside window
	b.Event(tracev2.EvGoBlock, 300, g.String("wait"), stk)
	b.Event(tracev2.EvGoUnblock, 330, 1, 1, 0)
	b.Event(tracev2.EvGoStart, 350, 1, 2)
	b.Event(tracev2.EvUserRegionEnd, 400, 0, regionName, stk) // inside window
	sel := Selection{Goroutines: []int64{1}, WindowA: 360, WindowB: 450}
	return tb, sel
}

func TestCrossWindowAnnotation(t *testing.T) {
	for _, v := range allVersions {
		t.Run(v.String(), func(t *testing.T) {
			tb, sel := buildCrossWindowAnnotationScenario(v)
			res, evs, _ := exportAndParse(t, tb.Build(), sel)
			if n := countKind(evs, exptrace.EventRegionBegin); n != 1 {
				t.Fatalf("region begin outside window not retained")
			}
			if n := countKind(evs, exptrace.EventRegionEnd); n != 1 {
				t.Fatalf("region end missing")
			}
			// The begin must be documented as support.
			var beginSupport bool
			for _, rec := range res.Manifest.Events {
				if rec.Kind == "RegionBegin" && rec.Role == "support" {
					beginSupport = true
				}
			}
			if !beginSupport {
				t.Fatalf("cross-window region begin not marked support")
			}
		})
	}
}

func TestInvalidSelectionsAndBadInput(t *testing.T) {
	tb, _ := buildBasicScenario(tracev2.VersionGo123)
	data := tb.Build()

	cases := []struct {
		name string
		sel  Selection
		want error
	}{
		{"empty goroutines", Selection{Goroutines: nil, WindowA: 1, WindowB: 2}, ErrInvalidSelection},
		{"inverted window", Selection{Goroutines: []int64{1}, WindowA: 100, WindowB: 50}, ErrInvalidSelection},
		{"unknown goroutine", Selection{Goroutines: []int64{99}, WindowA: 200, WindowB: 300}, ErrInvalidSelection},
		{"window outside trace", Selection{Goroutines: []int64{1}, WindowA: 900000, WindowB: 999999}, ErrInvalidSelection},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Export(bytes.NewReader(data), tc.sel)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("corrupt input", func(t *testing.T) {
		// Truncation reliably breaks batch framing.
		bad := data[:len(data)/2]
		if _, err := Export(bytes.NewReader(bad), Selection{Goroutines: []int64{1}, WindowA: 200, WindowB: 300}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("got %v, want ErrCorrupt", err)
		}
	})
	t.Run("garbage input", func(t *testing.T) {
		bad := bytes.Repeat([]byte{0x7f, 0x80, 0xff}, 64)
		if _, err := Export(bytes.NewReader(bad), Selection{Goroutines: []int64{1}, WindowA: 200, WindowB: 300}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("got %v, want ErrCorrupt", err)
		}
	})
	t.Run("v1 unsupported", func(t *testing.T) {
		v1 := append([]byte("go 1.21 trace\x00\x00\x00"), data[16:]...)
		if _, err := Export(bytes.NewReader(v1), Selection{Goroutines: []int64{1}, WindowA: 200, WindowB: 300}); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("got %v, want ErrUnsupported", err)
		}
	})
	t.Run("experimental unsupported", func(t *testing.T) {
		// An EvExperimentalBatch (type 49) header must be rejected on v23.
		var buf bytes.Buffer
		w, _ := tracev2.NewWriter(&buf, tracev2.VersionGo123)
		_ = w
		exp := bytes.Clone(data)
		// Insert an experimental batch right after the 16-byte header.
		batch := []byte{
			49,   // EvExperimentalBatch
			1, 0, // exp=1, gen=1
			0, 0, // m=0, time=0
			1, 0, // size=1
			0, // one payload byte
		}
		exp = append(exp[:16], append(batch, exp[16:]...)...)
		if _, err := Export(bytes.NewReader(exp), Selection{Goroutines: []int64{1}, WindowA: 200, WindowB: 300}); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("got %v, want ErrUnsupported", err)
		}
	})
}

// TestGoToolTraceReadBack runs the actual go tool trace parser over slice
// outputs for every supported version, mirroring what end users will do.
func TestGoToolTraceReadBack(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain unavailable")
	}
	// The go tool trace reader only accepts v25 traces starting with the
	// Go 1.25 toolchain; older toolchains reject the header. The x/exp reader
	// (exercised by exportAndParse) covers v25 byte validity regardless.
	toolVersion := strings.TrimPrefix(string(mustOutput(t, exec.Command(goBin, "env", "GOVERSION"))), "go")
	toolSupportsV25 := false
	if major, minor, ok := parseGoVersion(toolVersion); ok {
		toolSupportsV25 = major > 1 || (major == 1 && minor >= 25)
	}
	scenarios := map[string]func(tracev2.Version) (*testBuilder, Selection){
		"basic":      buildBasicScenario,
		"crossgen":   buildCrossGenScenario,
		"migration":  buildMigrationScenario,
		"annotation": buildCrossWindowAnnotationScenario,
	}
	for name, build := range scenarios {
		for _, v := range allVersions {
			if v == tracev2.VersionGo125 && !toolSupportsV25 {
				continue
			}
			t.Run(name+"/"+v.String(), func(t *testing.T) {
				tb, sel := build(v)
				res, err := Export(bytes.NewReader(tb.Build()), sel)
				if err != nil {
					t.Fatalf("Export: %v", err)
				}
				dir := t.TempDir()
				p := filepath.Join(dir, "slice.trace")
				if err := os.WriteFile(p, res.Trace, 0644); err != nil {
					t.Fatal(err)
				}
				out, err := exec.Command(goBin, "tool", "trace", "-d=parsed", p).CombinedOutput()
				if err != nil {
					t.Fatalf("go tool trace rejected slice:\n%s", out)
				}
			})
		}
	}
}

func mustOutput(t *testing.T, c *exec.Cmd) []byte {
	t.Helper()
	out, err := c.Output()
	if err != nil {
		t.Fatalf("command failed: %v: %s", err, out)
	}
	return bytes.TrimSpace(out)
}

func parseGoVersion(v string) (major, minor int, ok bool) {
	// Accept forms like "1.24", "1.24.1", "1.25-rc.1".
	parts := strings.Split(v, ".")
	if len(parts) < 2 {
		return 0, 0, false
	}
	m, err1 := strconv.Atoi(parts[0])
	n, err2 := strconv.Atoi(strings.SplitN(parts[1], "-", 2)[0])
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return m, n, true
}

// TestNoSuccessArtifactsOnError is enforced at the CLI layer; here we just
// confirm Export returns errors without producing any result bytes.
func TestNoResultOnError(t *testing.T) {
	tb, _ := buildBasicScenario(tracev2.VersionGo122)
	res, err := Export(bytes.NewReader(tb.Build()), Selection{Goroutines: []int64{1}, WindowA: 1, WindowB: 2})
	if err == nil || res != nil {
		t.Fatalf("expected error and nil result, got %v / %v", err, res)
	}
}

func TestManifestJSONShape(t *testing.T) {
	tb, sel := buildBasicScenario(tracev2.VersionGo125)
	res, _, _ := exportAndParse(t, tb.Build(), sel)
	if res.Manifest.SchemaVersion != 1 {
		t.Fatalf("schema version = %d", res.Manifest.SchemaVersion)
	}
	if !strings.HasPrefix(res.Manifest.Source.TraceVersion, "go1.") {
		t.Fatalf("bad source version %q", res.Manifest.Source.TraceVersion)
	}
	if len(res.Manifest.Source.Generations) != 1 || res.Manifest.Source.Generations[0].FrequencyUnitsPerSec != 1e9 {
		t.Fatalf("bad generation info: %+v", res.Manifest.Source.Generations)
	}
	if len(res.Manifest.Source.ClockSnapshots) != 1 {
		t.Fatalf("v25 trace must retain a clock snapshot record")
	}
	if res.Manifest.Dropped.IndependentLogEvents != 1 {
		t.Fatalf("dropped log count = %d, want 1", res.Manifest.Dropped.IndependentLogEvents)
	}
}

func TestWindowHalfOpen(t *testing.T) {
	tb, _ := buildBasicScenario(tracev2.VersionGo123)
	// Window ending exactly at the g2 unblock t=270 excludes it.
	res, _, _ := exportAndParse(t, tb.Build(), Selection{Goroutines: []int64{2}, WindowA: 235, WindowB: 270})
	for _, rec := range res.Manifest.Events {
		if rec.Role == "selected" && rec.Source.TimerUnits == 270 {
			t.Fatalf("event at exclusive window boundary was selected")
		}
	}
	// Window starting exactly at t=270 includes the unblock.
	res2, _, _ := exportAndParse(t, tb.Build(), Selection{Goroutines: []int64{2}, WindowA: 270, WindowB: 280})
	found := false
	for _, rec := range res2.Manifest.Events {
		if rec.Role == "selected" && rec.Source.TimerUnits == 270 {
			found = true
		}
	}
	if !found {
		t.Fatalf("event at inclusive window start not selected")
	}
}

func TestMonotonizationDocumented(t *testing.T) {
	tb := newTestBuilder(tracev2.VersionGo123)
	g := tb.Generation(1).SyncBase(50)
	statusBatch(tb, g, 0, 100, []int64{1})
	stk := g.Stack(0xd1)
	reason := g.String("chan receive")
	b := g.Batch(0, 200)
	// Two events share a raw timestamp; the interpreted reader bumps the
	// second one to preserve strict ordering.
	b.Event(tracev2.EvGoCreate, 210, 2, stk, stk)
	b.Event(tracev2.EvGoBlock, 210, reason, stk)
	b.Event(tracev2.EvGoStart, 220, 2, 1)
	b.Event(tracev2.EvGoBlock, 230, reason, stk)

	res, _, _ := exportAndParse(t, tb.Build(), Selection{Goroutines: []int64{2}, WindowA: 205, WindowB: 240})
	var bumps, identical int
	for _, rec := range res.Manifest.Events {
		switch rec.Output.TimeMapping {
		case "identical-native-units":
			identical++
		default:
			bumps++
			// Native units remain identical even when parsed ns moved.
			if rec.Source.TimerUnits != rec.Output.TimerUnits {
				t.Fatalf("native units changed under monotonization")
			}
			if rec.Output.ParsedNS <= rec.Source.ParsedNS {
				t.Fatalf("monotonized event not shifted forward")
			}
		}
	}
	if bumps == 0 {
		t.Fatalf("expected at least one documented monotonization")
	}
	if identical == 0 {
		t.Fatalf("expected some unchanged events")
	}
}

func TestMainRuntimeTrace(t *testing.T) {
	// End-to-end against a trace produced by the real runtime/trace
	// implementation, if the helper generator is available.
	helper := filepath.Join("testdata", "runtimegen", "main.go")
	if _, err := os.Stat(helper); err != nil {
		t.Skip("runtime generator not present")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "runtimegen.exe")
	if out, err := exec.Command("go", "build", "-o", bin, helper).CombinedOutput(); err != nil {
		t.Skipf("helper build failed: %v\n%s", err, out)
	}
	tracePath := filepath.Join(dir, "real.trace")
	if out, err := exec.Command(bin, tracePath).CombinedOutput(); err != nil {
		t.Skipf("helper run failed: %v\n%s", err, out)
	}
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	// Pick goroutine 1 (runtime.main) and a window covering the whole trace
	// after its first event.
	rr, err := tracev2.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := rr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	minNS := unitsToNS(raw.Generations[0].MinUnits, raw.Generations[0].Frequency)
	sel := Selection{Goroutines: []int64{1}, WindowA: minNS, WindowB: minNS + 10_000_000}
	if _, err := Export(bytes.NewReader(data), sel); err != nil {
		t.Fatalf("real runtime trace slice failed: %v", err)
	}
}
