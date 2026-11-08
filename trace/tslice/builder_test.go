package tslice

import (
	"bytes"
	"time"

	"honnef.co/go/gotraceui/trace/tracev2"
)

// testBuilder produces valid version-2 trace binaries from hand-written
// event sequences, giving tests precise control over generations, threads,
// scheduler transitions and annotations.
type testBuilder struct {
	v    tracev2.Version
	gens []*testGen
}

type testGen struct {
	b          *testBuilder
	gen        uint64
	freq       uint64
	batches    []*testBatch
	strings    map[string]uint64
	stacks     map[uint64][]tracev2.StackFrame
	nextString uint64
	nextStack  uint64
	// syncBase is the base timestamp of the sync/frequency batch.
	syncBase uint64
	snapMono uint64
	snapWall time.Time
	samples  []tracev2.RawEvent
}

type testBatch struct {
	g    *testGen
	m    int64
	base uint64
	evs  []tracev2.RawEvent
}

func newTestBuilder(v tracev2.Version) *testBuilder {
	return &testBuilder{v: v}
}

// Generation adds a generation. gen numbers start at 1 and must be contiguous.
func (tb *testBuilder) Generation(gen uint64) *testGen {
	g := &testGen{
		b:       tb,
		gen:     gen,
		freq:    1_000_000_000, // one timer unit per nanosecond for simplicity
		strings: map[string]uint64{},
		stacks:  map[uint64][]tracev2.StackFrame{},
	}
	tb.gens = append(tb.gens, g)
	return g
}

// Frequency overrides the default frequency (units per second).
func (g *testGen) Frequency(f uint64) *testGen { g.freq = f; return g }

// SyncBase sets the structural batch base timestamp.
func (g *testGen) SyncBase(units uint64) *testGen { g.syncBase = units; return g }

// ClockSnapshot attaches a Go 1.25 clock snapshot to the generation's sync.
func (g *testGen) ClockSnapshot(mono uint64, wall time.Time) *testGen {
	g.snapMono = mono
	g.snapWall = wall
	return g
}

// String interns a string and returns its table ID.
func (g *testGen) String(s string) uint64 {
	if s == "" {
		return 0
	}
	if id, ok := g.strings[s]; ok {
		return id
	}
	g.nextString++
	g.strings[s] = g.nextString
	return g.nextString
}

// Stack interns a stack identified by its PCs and returns its table ID.
func (g *testGen) Stack(pcs ...uint64) uint64 {
	if len(pcs) == 0 {
		return 0
	}
	id := g.nextStack + 1
	for sid, frames := range g.stacks {
		if equalPCs(frames, pcs) {
			return sid
		}
	}
	g.nextStack++
	frames := make([]tracev2.StackFrame, len(pcs))
	for i, pc := range pcs {
		frames[i] = tracev2.StackFrame{
			PC:     pc,
			FuncID: g.String("func@" + itoa(pc)),
			FileID: g.String("file@" + itoa(pc)),
			Line:   uint64(i + 1),
		}
	}
	g.stacks[id] = frames
	return id
}

func equalPCs(frames []tracev2.StackFrame, pcs []uint64) bool {
	if len(frames) != len(pcs) {
		return false
	}
	for i := range frames {
		if frames[i].PC != pcs[i] {
			return false
		}
	}
	return true
}

// Batch opens a timed event batch on thread m with the given base timer units.
func (g *testGen) Batch(m int64, baseUnits uint64) *testBatch {
	b := &testBatch{g: g, m: m, base: baseUnits}
	g.batches = append(g.batches, b)
	return b
}

// Event appends a timed event; args must omit the timestamp delta, which is
// computed from the absolute timer unit timestamp.
func (b *testBatch) Event(typ tracev2.EventType, absUnits uint64, args ...uint64) {
	dt := absUnits - b.base
	if len(b.evs) > 0 {
		prev := b.evs[len(b.evs)-1].AbsUnits
		dt = absUnits - prev
	}
	all := append([]uint64{dt}, args...)
	b.evs = append(b.evs, tracev2.RawEvent{Type: typ, Args: all, AbsUnits: absUnits})
}

// Build encodes the trace.
func (tb *testBuilder) Build() []byte {
	var out bytes.Buffer
	w, err := tracev2.NewWriter(&out, tb.v)
	if err != nil {
		panic(err)
	}
	for _, g := range tb.gens {
		// Go 1.25+ requires a clock snapshot in every generation's sync batch.
		if tb.v >= tracev2.VersionGo125 && g.snapWall.IsZero() && g.snapMono == 0 {
			g.snapMono = 777
			g.snapWall = time.Unix(1700000000, 123)
		}
		g.writeSync(w)
		for _, b := range g.batches {
			w.WriteBatch(&tracev2.RawBatch{Gen: g.gen, M: b.m, TimeUnits: b.base, Events: b.evs})
		}
		g.writeCPUSamples(w)
		g.writeStacks(w)
		g.writeStrings(w)
	}
	return out.Bytes()
}

func (g *testGen) writeSync(w *tracev2.RawWriter) {
	if g.b.v >= tracev2.VersionGo125 {
		evs := []tracev2.RawEvent{{Type: tracev2.EvSync}}
		evs = append(evs, tracev2.RawEvent{Type: tracev2.EvFrequency, Args: []uint64{g.freq}})
		if !g.snapWall.IsZero() || g.snapMono != 0 {
			evs = append(evs, tracev2.RawEvent{
				Type:     tracev2.EvClockSnapshot,
				Args:     []uint64{1, g.snapMono, uint64(g.snapWall.Unix()), uint64(g.snapWall.Nanosecond())},
				AbsUnits: g.syncBase + 1,
			})
		}
		w.WriteBatch(&tracev2.RawBatch{Gen: g.gen, M: tracev2.NoThread, TimeUnits: g.syncBase, Events: evs})
		return
	}
	w.WriteBatch(&tracev2.RawBatch{
		Gen:       g.gen,
		M:         tracev2.NoThread,
		TimeUnits: g.syncBase,
		Events:    []tracev2.RawEvent{{Type: tracev2.EvFrequency, Args: []uint64{g.freq}}},
	})
}

func (g *testGen) writeCPUSamples(w *tracev2.RawWriter) {
	// Samples are added directly via CPUSample; collected per gen.
	if len(g.samples) == 0 {
		return
	}
	evs := []tracev2.RawEvent{{Type: tracev2.EvCPUSamples}}
	evs = append(evs, g.samples...)
	w.WriteBatch(&tracev2.RawBatch{Gen: g.gen, M: tracev2.NoThread, TimeUnits: g.syncBase, Events: evs})
}

func (g *testGen) writeStacks(w *tracev2.RawWriter) {
	if len(g.stacks) == 0 {
		return
	}
	evs := []tracev2.RawEvent{{Type: tracev2.EvStacks}}
	ids := make([]uint64, 0, len(g.stacks))
	for id := range g.stacks {
		ids = append(ids, id)
	}
	sortU(ids)
	for _, id := range ids {
		frames := g.stacks[id]
		args := []uint64{id, uint64(len(frames))}
		for _, f := range frames {
			args = append(args, f.PC, f.FuncID, f.FileID, f.Line)
		}
		evs = append(evs, tracev2.RawEvent{Type: tracev2.EvStack, Args: args})
	}
	w.WriteBatch(&tracev2.RawBatch{Gen: g.gen, M: tracev2.NoThread, TimeUnits: g.syncBase, Events: evs})
}

func (g *testGen) writeStrings(w *tracev2.RawWriter) {
	if len(g.strings) == 0 {
		return
	}
	evs := []tracev2.RawEvent{{Type: tracev2.EvStrings}}
	type strEntry struct {
		id  uint64
		val string
	}
	entries := make([]strEntry, 0, len(g.strings))
	for v, id := range g.strings {
		entries = append(entries, strEntry{id: id, val: v})
	}
	sort := func(s []strEntry) {
		for i := 1; i < len(s); i++ {
			for j := i; j > 0 && s[j-1].id > s[j].id; j-- {
				s[j-1], s[j] = s[j], s[j-1]
			}
		}
	}
	sort(entries)
	for _, e := range entries {
		evs = append(evs, tracev2.RawEvent{Type: tracev2.EvString, Args: []uint64{e.id}, Data: []byte(e.val)})
	}
	w.WriteBatch(&tracev2.RawBatch{Gen: g.gen, M: tracev2.NoThread, TimeUnits: g.syncBase, Events: evs})
}

// CPUSample records a CPU sample with an absolute timer-unit timestamp.
func (g *testGen) CPUSample(absUnits, m, p, gid, stackID uint64) {
	g.samples = append(g.samples, tracev2.RawEvent{
		Type:     tracev2.EvCPUSample,
		Args:     []uint64{absUnits, m, p, gid, stackID},
		AbsUnits: absUnits,
	})
}

func sortU(s []uint64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func itoa(x uint64) string {
	if x == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for x > 0 {
		i--
		buf[i] = byte('0' + x%10)
		x /= 10
	}
	return string(buf[i:])
}
