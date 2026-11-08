package tslice

import (
	"fmt"

	exptrace "golang.org/x/exp/trace"

	"honnef.co/go/gotraceui/trace/tracev2"
)

// Manifest is the machine-readable description of a causal slice.
type Manifest struct {
	SchemaVersion int           `json:"schema_version"`
	Source        SourceInfo    `json:"source"`
	Output        OutputInfo    `json:"output"`
	Selection     SelectionInfo `json:"selection"`
	Events        []EventRecord `json:"events"`
	Definitions   Definitions   `json:"definitions"`
	Boundaries    []Boundary    `json:"boundaries"`
	Dropped       DroppedInfo   `json:"dropped"`
}

// SourceInfo describes the input trace.
type SourceInfo struct {
	Path           string              `json:"path"`
	TraceVersion   string              `json:"trace_version"`
	Generations    []GenerationInfo    `json:"generations"`
	ClockSnapshots []ClockSnapshotInfo `json:"clock_snapshots"`
}

// GenerationInfo carries the clock basis of a source generation.
type GenerationInfo struct {
	Generation           int    `json:"generation"`
	FrequencyUnitsPerSec uint64 `json:"frequency_units_per_sec"`
	MinTimerUnits        uint64 `json:"min_timer_units"`
}

// ClockSnapshotInfo records a Go 1.25 clock snapshot.
type ClockSnapshotInfo struct {
	Generation int    `json:"generation"`
	TraceNS    int64  `json:"trace_ns"`
	Mono       uint64 `json:"mono"`
	WallSec    int64  `json:"wall_sec"`
	WallNSec   int64  `json:"wall_nsec"`
}

// OutputInfo describes the encoded trace.
type OutputInfo struct {
	TraceVersion string            `json:"trace_version"`
	Generations  []GenerationRemap `json:"generations"`
}

// GenerationRemap links source and output generations.
type GenerationRemap struct {
	SourceGeneration int `json:"source_generation"`
	OutputGeneration int `json:"output_generation"`
}

// SelectionInfo is the resolved selection.
type SelectionInfo struct {
	Goroutines []int64 `json:"goroutines"`
	WindowA    int64   `json:"window_a_ns"`
	WindowB    int64   `json:"window_b_ns"`
}

// EventRecord describes one interpreted event in the slice.
type EventRecord struct {
	Role            string        `json:"role"`
	Kind            string        `json:"kind"`
	Detail          string        `json:"detail"`
	Source          EventLocation `json:"source"`
	Output          EventTiming   `json:"output"`
	DependencyChain []ChainLink   `json:"dependency_chain,omitempty"`
}

// EventLocation locates an event in the source trace.
type EventLocation struct {
	Generation int    `json:"generation"`
	Thread     int64  `json:"thread"`
	Ordinal    int    `json:"ordinal"`
	SampleOrd  int    `json:"sample_ordinal"`
	TimerUnits uint64 `json:"timer_units"`
	ParsedNS   int64  `json:"parsed_time_ns"`
}

// EventTiming locates and timestamps the same event in the slice.
type EventTiming struct {
	Generation  int    `json:"generation"`
	Thread      int64  `json:"thread"`
	Ordinal     int    `json:"ordinal"`
	SampleOrd   int    `json:"sample_ordinal"`
	TimerUnits  uint64 `json:"timer_units"`
	ParsedNS    int64  `json:"parsed_time_ns"`
	TimeMapping string `json:"time_mapping"`
}

// ChainLink is one hop of a support-to-seed dependency path.
type ChainLink struct {
	Reason string        `json:"reason"`
	Event  EventLocation `json:"event"`
	Kind   string        `json:"kind"`
}

// Definitions lists the retained per-generation string and stack tables.
type Definitions struct {
	Strings [][]StringEntry `json:"strings"`
	Stacks  [][]StackEntry  `json:"stacks"`
}

// DroppedInfo counts what was deliberately excluded.
type DroppedInfo struct {
	// IndependentLogEvents counts log events that neither are seeds nor are
	// referenced by any retained event.
	IndependentLogEvents int `json:"independent_log_events"`
	// UnreferencedStrings is the total number of dropped string definitions.
	UnreferencedStrings int `json:"unreferenced_strings"`
	// UnreferencedStacks is the total number of dropped stack definitions.
	UnreferencedStacks int `json:"unreferenced_stacks"`
	// EventsByKind counts interpreted events excluded from the slice.
	EventsByKind map[string]int `json:"events_by_kind"`
}

func (e *engine) buildManifest(out []byte, sel Selection) Manifest {
	m := Manifest{
		SchemaVersion: 1,
		Selection: SelectionInfo{
			Goroutines: dedupe(sel.Goroutines),
			WindowA:    sel.WindowA,
			WindowB:    sel.WindowB,
		},
		Dropped: DroppedInfo{EventsByKind: map[string]int{}},
	}

	// Source and output clock metadata.
	m.Source.TraceVersion = e.v.String()
	for gi, g := range e.raw.Generations {
		m.Source.Generations = append(m.Source.Generations, GenerationInfo{
			Generation:           gi + 1,
			FrequencyUnitsPerSec: g.Frequency,
			MinTimerUnits:        g.MinUnits,
		})
		if g.HasClockSnapshot {
			snap := g.ClockSnapshot
			m.Source.ClockSnapshots = append(m.Source.ClockSnapshots, ClockSnapshotInfo{
				Generation: gi + 1,
				TraceNS:    unitsToNS(snap.AbsUnits, g.Frequency),
				Mono:       snap.Args[1],
				WallSec:    int64(snap.Args[2]),
				WallNSec:   int64(snap.Args[3]),
			})
		}
	}
	m.Output.TraceVersion = e.v.String()
	for _, ge := range e.enc {
		m.Output.Generations = append(m.Output.Generations, GenerationRemap{
			SourceGeneration: genOrdinal(e.raw, ge.srcGen) + 1,
			OutputGeneration: int(ge.outGen),
		})
		m.Definitions.Strings = append(m.Definitions.Strings, ge.keptStrings)
		m.Definitions.Stacks = append(m.Definitions.Stacks, ge.keptStacks)
		m.Dropped.UnreferencedStrings += ge.dropStrings
		m.Dropped.UnreferencedStacks += ge.dropStacks
	}
	m.Boundaries = e.bounds

	// Event records, one per retained interpreted event, in source order.
	for i, ev := range e.ievs {
		if e.igen[i] < 0 {
			continue
		}
		ref := e.refs[i]
		role, ok := e.role[ref]
		if !ok {
			continue
		}
		it := e.items[ref]
		src := EventLocation{
			Generation: ref.gen + 1,
			Thread:     ref.m,
			Ordinal:    ref.ord,
			SampleOrd:  ref.sampleOrd,
			TimerUnits: it.units,
			ParsedNS:   it.ns,
		}
		t := e.timing[ref]
		oref := e.outRefOf[ref]
		mapping := "identical-native-units"
		if t.monotonized {
			mapping = "reader-monotonization: parsed time adjusted to preserve strict event ordering"
		}
		out := EventTiming{
			Generation:  oref.gen + 1,
			Thread:      oref.m,
			Ordinal:     oref.ord,
			SampleOrd:   oref.sampleOrd,
			TimerUnits:  t.units,
			ParsedNS:    t.parsedNS,
			TimeMapping: mapping,
		}
		rec := EventRecord{
			Role:   roleName(role),
			Kind:   ev.Kind().String(),
			Detail: e.eventDetail(ev),
			Source: src,
			Output: out,
		}
		if role == roleSupport {
			rec.DependencyChain = e.chainFor(i)
		}
		m.Events = append(m.Events, rec)
	}

	// Dropped event statistics.
	for i, ev := range e.ievs {
		if e.igen[i] < 0 {
			continue
		}
		if _, kept := e.role[e.refs[i]]; kept {
			continue
		}
		m.Dropped.EventsByKind[ev.Kind().String()]++
		if ev.Kind() == exptrace.EventLog {
			m.Dropped.IndependentLogEvents++
		}
	}
	return m
}

func (e *engine) chainFor(i int) []ChainLink {
	var chain []ChainLink
	seen := map[int]bool{}
	cur := i
	for {
		edge, ok := e.parent[cur]
		if !ok {
			break
		}
		if seen[cur] {
			break
		}
		seen[cur] = true
		prereq := edge.prereq
		ref := e.refs[prereq]
		it := e.items[ref]
		chain = append(chain, ChainLink{
			Reason: edge.reason,
			Kind:   e.ievs[prereq].Kind().String(),
			Event: EventLocation{
				Generation: ref.gen + 1,
				Thread:     ref.m,
				Ordinal:    ref.ord,
				SampleOrd:  ref.sampleOrd,
				TimerUnits: it.units,
				ParsedNS:   it.ns,
			},
		})
		cur = prereq
	}
	return chain
}

func (e *engine) eventDetail(ev exptrace.Event) string {
	switch ev.Kind() {
	case exptrace.EventStateTransition:
		st := ev.StateTransition()
		switch st.Resource.Kind {
		case exptrace.ResourceGoroutine:
			from, to := st.Goroutine()
			return fmt.Sprintf("goroutine %d: %s -> %s reason=%q", st.Resource.Goroutine(), from, to, st.Reason)
		case exptrace.ResourceProc:
			from, to := st.Proc()
			return fmt.Sprintf("proc %d: %s -> %s", st.Resource.Proc(), from, to)
		}
	case exptrace.EventStackSample:
		return fmt.Sprintf("sample of M=%d P=%d G=%d", ev.Thread(), ev.Proc(), ev.Goroutine())
	case exptrace.EventLog:
		l := ev.Log()
		return fmt.Sprintf("task=%d category=%q message=%q", l.Task, l.Category, l.Message)
	case exptrace.EventTaskBegin, exptrace.EventTaskEnd:
		t := ev.Task()
		return fmt.Sprintf("task=%d parent=%d type=%q", t.ID, t.Parent, t.Type)
	case exptrace.EventRegionBegin, exptrace.EventRegionEnd:
		r := ev.Region()
		return fmt.Sprintf("task=%d region=%q", r.Task, r.Type)
	case exptrace.EventRangeBegin, exptrace.EventRangeActive, exptrace.EventRangeEnd:
		r := ev.Range()
		return fmt.Sprintf("range=%q scope=%s", r.Name, r.Scope)
	case exptrace.EventLabel:
		return fmt.Sprintf("label=%q", ev.Label().Label)
	case exptrace.EventMetric:
		m := ev.Metric()
		return fmt.Sprintf("metric=%q value=%s", m.Name, m.Value.String())
	}
	return ""
}

func roleName(r eventRole) string {
	switch r {
	case roleSeed:
		return "selected"
	case roleSupport:
		return "support"
	default:
		return "none"
	}
}

func genOrdinal(raw *tracev2.RawTrace, gen uint64) int {
	for i, g := range raw.Generations {
		if g.Gen == gen {
			return i
		}
	}
	return -1
}
