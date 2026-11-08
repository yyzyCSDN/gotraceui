package tslice

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	exptrace "golang.org/x/exp/trace"

	"honnef.co/go/gotraceui/trace/ptrace"
	"honnef.co/go/gotraceui/trace/tracev2"
)

// outTiming records how a source event reappears in the encoded trace.
type outTiming struct {
	units       uint64
	parsedNS    int64
	monotonized bool
}

// verify re-parses the encoded trace with both the gotraceui parser stack and
// the golang.org/x/exp reader that powers go tool trace, then asserts that the
// selected events survive with identical native semantics and clock
// coordinates.
func (e *engine) verify(out []byte, sel Selection) error {
	rr, err := tracev2.NewReader(bytes.NewReader(out))
	if err != nil {
		return fmt.Errorf("slice output failed raw read-back: %w", err)
	}
	outRaw, err := rr.ReadAll()
	if err != nil {
		return fmt.Errorf("slice output failed raw read-back: %w", err)
	}
	if len(outRaw.Generations) != len(e.enc) {
		return fmt.Errorf("slice output generation count mismatch: got %d want %d",
			len(outRaw.Generations), len(e.enc))
	}

	outIEvs, outRefs, outIGen, err := readAligned(bytes.NewReader(out), outRaw)
	if err != nil {
		return fmt.Errorf("slice output failed interpreted read-back: %w", err)
	}

	// The gotraceui-enriched parser must accept the slice as well.
	ptReader, err := exptrace.NewReader(bytes.NewReader(out))
	if err != nil {
		return fmt.Errorf("slice output failed gotraceui read-back: %w", err)
	}
	if _, err := ptrace.Parse(ptReader, func(float64) {}); err != nil {
		return fmt.Errorf("slice output failed gotraceui parse: %w", err)
	}

	outCatalog := catalog(outRaw)
	srcToOut := map[rawRef]rawRef{}
	for src, or := range e.outRefOf {
		srcToOut[src] = or
		if _, ok := outCatalog[or]; !ok {
			return fmt.Errorf("internal error: encoded event %v missing from output catalog", or)
		}
	}

	// Resolve output timings per source ref and assert clock preservation.
	timing := map[rawRef]outTiming{}
	for i, ev := range outIEvs {
		if outIGen[i] < 0 {
			continue
		}
		or := outRefs[i]
		src, ok := refByValue(srcToOut, or)
		if !ok {
			return fmt.Errorf("internal error: output event %v has no source counterpart", or)
		}
		srcUnits := e.items[src].units
		outUnits := outCatalog[or].units
		if outUnits != srcUnits {
			return fmt.Errorf("clock drift for %v: source units %d output units %d",
				src, srcUnits, outUnits)
		}
		// Multiple interpreted events may share a ref (synthetic expansions);
		// they carry the same timestamp by construction.
		if t, seen := timing[src]; seen && t.parsedNS != eventTime(ev) {
			return fmt.Errorf("internal error: inconsistent parsed times for ref %v", src)
		}
		timing[src] = outTiming{
			units:       outUnits,
			parsedNS:    eventTime(ev),
			monotonized: eventTime(ev) != e.items[src].ns,
		}
	}
	for ref := range e.role {
		if _, ok := timing[ref]; !ok {
			return fmt.Errorf("internal error: retained event %v absent from slice output", ref)
		}
	}
	e.timing = timing

	if err := e.verifyWindowSemantics(sel, outIEvs, outIGen, outRefs); err != nil {
		return err
	}
	if err := e.verifyContentAudit(outIEvs, outIGen); err != nil {
		return err
	}
	return nil
}

// verifyWindowSemantics compares the in-window state sequences of the selected
// goroutines and their creator/waker facts between source and slice.
func (e *engine) verifyWindowSemantics(sel Selection, outIEvs []exptrace.Event, outIGen []int, outRefs []rawRef) error {
	for _, gid := range dedupe(sel.Goroutines) {
		src := e.windowGTransitions(gid, sel, e.ievs, e.igen, e.refs, true)
		dst := e.windowGTransitions(gid, sel, outIEvs, outIGen, outRefs, false)
		if len(src) != len(dst) {
			return fmt.Errorf("read-back mismatch for goroutine %d: %d in-window transitions in source, %d in slice",
				gid, len(src), len(dst))
		}
		for k := range src {
			a, b := src[k], dst[k]
			if transitionSig(a) != transitionSig(b) {
				return fmt.Errorf("read-back mismatch for goroutine %d transition %d:\n  source: %s\n  slice:  %s",
					gid, k, transitionSig(a), transitionSig(b))
			}
		}
	}
	return nil
}

// windowGTransitions collects the state transitions of gid inside the window.
// Both source and slice are compared on their parsed nanosecond timelines;
// raw-unit clock correlation is audited separately in the manifest.
func (e *engine) windowGTransitions(gid int64, sel Selection,
	ievs []exptrace.Event, igen []int, refs []rawRef, source bool) []exptrace.Event {
	var out []exptrace.Event
	for i, ev := range ievs {
		if igen[i] < 0 || ev.Kind() != exptrace.EventStateTransition {
			continue
		}
		st := ev.StateTransition()
		if st.Resource.Kind != exptrace.ResourceGoroutine || int64(st.Resource.Goroutine()) != gid {
			continue
		}
		ts := eventTime(ev)
		if ts < sel.WindowA || ts >= sel.WindowB {
			continue
		}
		out = append(out, ev)
	}
	return out
}

// transitionSig renders the comparable identity of a goroutine state
// transition, including the executor (creator/waker) fact.
func transitionSig(ev exptrace.Event) string {
	st := ev.StateTransition()
	from, to := st.Goroutine()
	return fmt.Sprintf("g=%d %s->%s reason=%q M=%d P=%d actorG=%d",
		st.Resource.Goroutine(), from, to, st.Reason, ev.Thread(), ev.Proc(), ev.Goroutine())
}

// verifyContentAudit asserts that the slice contains exactly the retained
// interpreted events with unchanged content (modulo timestamps, which are
// audited separately).
func (e *engine) verifyContentAudit(outIEvs []exptrace.Event, outIGen []int) error {
	want := map[string]int{}
	for i, ev := range e.ievs {
		if e.igen[i] < 0 {
			continue
		}
		if _, ok := e.role[e.refs[i]]; !ok {
			continue
		}
		want[contentSig(ev)]++
	}
	got := map[string]int{}
	for i, ev := range outIEvs {
		if outIGen[i] < 0 {
			continue
		}
		got[contentSig(ev)]++
	}
	for sig, n := range want {
		if got[sig] != n {
			return fmt.Errorf("content audit failed for %q: source count %d, slice count %d", sig, n, got[sig])
		}
	}
	for sig, n := range got {
		if want[sig] != n {
			return fmt.Errorf("content audit found unexpected event %q (count %d)", sig, n)
		}
	}
	return nil
}

// contentSig renders a time-independent identity of an interpreted event.
func contentSig(ev exptrace.Event) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s M=%d P=%d G=%d", ev.Kind(), ev.Thread(), ev.Proc(), ev.Goroutine())
	switch ev.Kind() {
	case exptrace.EventStateTransition:
		st := ev.StateTransition()
		switch st.Resource.Kind {
		case exptrace.ResourceGoroutine:
			from, to := st.Goroutine()
			fmt.Fprintf(&b, " G=%d %s->%s reason=%q", st.Resource.Goroutine(), from, to, st.Reason)
		case exptrace.ResourceProc:
			from, to := st.Proc()
			fmt.Fprintf(&b, " P=%d %s->%s", st.Resource.Proc(), from, to)
		}
		if st.Stack != exptrace.NoStack {
			writeStackSig(&b, st.Stack)
		}
	case exptrace.EventStackSample:
		writeStackSig(&b, ev.Stack())
	case exptrace.EventLog:
		l := ev.Log()
		fmt.Fprintf(&b, " task=%d category=%q message=%q", l.Task, l.Category, l.Message)
		writeStackSig(&b, ev.Stack())
	case exptrace.EventTaskBegin, exptrace.EventTaskEnd:
		t := ev.Task()
		fmt.Fprintf(&b, " task=%d parent=%d type=%q", t.ID, t.Parent, t.Type)
		writeStackSig(&b, ev.Stack())
	case exptrace.EventRegionBegin, exptrace.EventRegionEnd:
		r := ev.Region()
		fmt.Fprintf(&b, " task=%d type=%q", r.Task, r.Type)
		writeStackSig(&b, ev.Stack())
	case exptrace.EventRangeBegin, exptrace.EventRangeActive, exptrace.EventRangeEnd:
		r := ev.Range()
		fmt.Fprintf(&b, " name=%q scope=%s", r.Name, r.Scope)
	case exptrace.EventLabel:
		fmt.Fprintf(&b, " label=%q resource=%s", ev.Label().Label, ev.Label().Resource)
	case exptrace.EventMetric:
		m := ev.Metric()
		fmt.Fprintf(&b, " name=%q value=%s", m.Name, m.Value.String())
	}
	return b.String()
}

func writeStackSig(b *strings.Builder, st exptrace.Stack) {
	var pcs []uint64
	for f := range st.Frames() {
		pcs = append(pcs, f.PC)
	}
	slices.Sort(pcs)
	fmt.Fprintf(b, " stack=%v", pcs)
}

func refByValue(m map[rawRef]rawRef, want rawRef) (rawRef, bool) {
	for k, v := range m {
		if v == want {
			return k, true
		}
	}
	return rawRef{}, false
}
