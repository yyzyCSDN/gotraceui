package tslice

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	exptrace "golang.org/x/exp/trace"

	"honnef.co/go/gotraceui/trace/tracev2"
)

// rawRef identifies a single raw event inside the source trace.
//
// Timed events are identified by generation, thread and their ordinal among
// the thread's timed events. CPU samples are identified by their generation and
// sample ordinal.
type rawRef struct {
	gen       int
	m         int64
	ord       int
	sampleOrd int
}

func timedRef(gen int, m int64, ord int) rawRef {
	return rawRef{gen: gen, m: m, ord: ord, sampleOrd: -1}
}

func sampleRef(gen, sampleOrd int) rawRef {
	return rawRef{gen: gen, m: tracev2.NoThread, ord: -1, sampleOrd: sampleOrd}
}

func (r rawRef) isSample() bool { return r.sampleOrd >= 0 }

// rawItem is a catalogued raw event with resolved absolute timing.
type rawItem struct {
	ref   rawRef
	ev    tracev2.RawEvent
	units uint64
	ns    int64
}

// engine holds the parsed source and all slicing state.
type engine struct {
	data []byte
	raw  *tracev2.RawTrace
	v    tracev2.Version
	spec []tracev2.EventSpec

	// Interpreted event stream and the raw ref each event corresponds to.
	// Sync events have ref.gen == -1.
	ievs []exptrace.Event
	refs []rawRef
	igen []int

	// Catalog of all selectable raw events.
	items map[rawRef]*rawItem

	// Closure state.
	role   map[rawRef]eventRole
	parent map[int]depEdge
	bounds []Boundary

	// enc records per-generation remapping produced during encoding.
	enc []*genEncoding

	// outRefOf maps source refs to the refs they occupy in the encoded trace.
	outRefOf map[rawRef]rawRef

	// timing records the read-back timing per source ref after verification.
	timing map[rawRef]outTiming

	// Re-encode bookkeeping.
	keptOrder []rawRef
}

type eventRole uint8

const (
	roleNone eventRole = iota
	roleSeed
	roleSupport
)

type depEdge struct {
	prereq int
	reason string
}

func newEngine(data []byte, raw *tracev2.RawTrace) (*engine, error) {
	e := &engine{
		data:     data,
		raw:      raw,
		v:        raw.Version,
		spec:     raw.Version.Specs(),
		items:    map[rawRef]*rawItem{},
		role:     map[rawRef]eventRole{},
		parent:   map[int]depEdge{},
		outRefOf: map[rawRef]rawRef{},
		timing:   map[rawRef]outTiming{},
	}
	if err := e.buildCatalog(); err != nil {
		return nil, err
	}
	if err := e.parseAndAlign(); err != nil {
		return nil, err
	}
	return e, nil
}

// buildCatalog indexes every timed event and CPU sample.
func (e *engine) buildCatalog() error {
	e.items = catalog(e.raw)
	return nil
}

// parseAndAlign drains the public reader and binds every interpreted event to
// its raw ref.
func (e *engine) parseAndAlign() error {
	ievs, refs, igen, err := readAligned(bytes.NewReader(e.data), e.raw)
	if err != nil {
		return err
	}
	e.ievs = ievs
	e.refs = refs
	e.igen = igen
	return nil
}

// readAligned parses data with the public interpreted reader and aligns every
// event to raw refs in raw. It is used both for source traces and for
// read-back validation of encoded slices.
func readAligned(r io.Reader, raw *tracev2.RawTrace) ([]exptrace.Event, []rawRef, []int, error) {
	pr, err := exptrace.NewReader(r)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: interpreted parse failed: %v", tracev2.ErrCorrupt, err)
	}
	var ievs []exptrace.Event
	var refs []rawRef
	var igen []int
	gen := -1
	syncs := 0
	for {
		ev, err := pr.ReadEvent()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, nil, nil, fmt.Errorf("%w: interpreted parse failed: %v", tracev2.ErrCorrupt, err)
		}
		if ev.Kind() == exptrace.EventSync {
			syncs++
			gen++
			ievs = append(ievs, ev)
			refs = append(refs, rawRef{gen: -1})
			igen = append(igen, -1)
			continue
		}
		if gen < 0 || gen >= len(raw.Generations) {
			return nil, nil, nil, fmt.Errorf("%w: interpreted event outside any generation", tracev2.ErrCorrupt)
		}
		ievs = append(ievs, ev)
		igen = append(igen, gen)
		refs = append(refs, rawRef{})
	}
	if syncs != len(raw.Generations)+1 {
		return nil, nil, nil, fmt.Errorf("%w: sync count %d does not match %d generations",
			tracev2.ErrCorrupt, syncs, len(raw.Generations))
	}

	items := catalog(raw)
	for gi := range raw.Generations {
		if err := alignGeneration(ievs, refs, igen, gi, raw, items); err != nil {
			return nil, nil, nil, err
		}
	}
	return ievs, refs, igen, nil
}

// catalog builds the raw event index for a parsed trace.
func catalog(raw *tracev2.RawTrace) map[rawRef]*rawItem {
	items := map[rawRef]*rawItem{}
	for gi, g := range raw.Generations {
		freq := g.Frequency
		for _, m := range g.SortedThreadIDs() {
			for ord, ev := range g.TimedEvents(m) {
				ref := timedRef(gi, m, ord)
				items[ref] = &rawItem{ref: ref, ev: ev, units: ev.AbsUnits, ns: unitsToNS(ev.AbsUnits, freq)}
			}
		}
		for so, ev := range g.CPUSamples {
			ref := sampleRef(gi, so)
			items[ref] = &rawItem{ref: ref, ev: ev, units: ev.AbsUnits, ns: unitsToNS(ev.AbsUnits, freq)}
		}
	}
	return items
}

// alignGeneration matches all interpreted events of one generation against the
// raw catalog.
//
// Matching is signature-driven rather than timestamp-driven because the
// interpreted reader may monotonize timestamps to enforce strict ordering, so
// parsed nanoseconds need not equal the raw frequency conversion. Signatures
// pin event types, resources and states; equal-signature events are paired
// FIFO by raw timestamp. Events the reader synthesizes from a single raw event
// have no raw counterpart and are validated and bound separately.
func alignGeneration(ievs []exptrace.Event, refs []rawRef, igen []int, gi int,
	raw *tracev2.RawTrace, items map[rawRef]*rawItem) error {
	queues := map[string][]*rawItem{}
	var order []string
	for _, it := range items {
		if it.ref.gen != gi {
			continue
		}
		sig := rawSignature(it)
		if _, ok := queues[sig]; !ok {
			order = append(order, sig)
		}
		queues[sig] = append(queues[sig], it)
	}
	for _, sig := range order {
		slices.SortFunc(queues[sig], func(a, b *rawItem) int { return compareRef(a.ref, b.ref) })
	}

	var leftovers []int
	for i := range ievs {
		if igen[i] != gi {
			continue
		}
		sig := interpSignature(ievs[i])
		q := queues[sig]
		if len(q) == 0 {
			leftovers = append(leftovers, i)
			continue
		}
		refs[i] = q[0].ref
		queues[sig] = q[1:]
	}

	if err := bindSynthetic(ievs, refs, igen, gi, leftovers, items); err != nil {
		return err
	}
	for sig, q := range queues {
		if len(q) != 0 {
			return fmt.Errorf("%w: internal alignment error: %d unmatched raw events with signature %q in gen %d",
				tracev2.ErrCorrupt, len(q), sig, raw.Generations[gi].Gen)
		}
	}
	for i := range ievs {
		if igen[i] == gi && refs[i].gen < 0 {
			return fmt.Errorf("%w: internal alignment error: event %d unmatched", tracev2.ErrCorrupt, i)
		}
	}
	return nil
}

// bindSynthetic validates interpreted events without a raw counterpart and
// binds them to the raw event they were synthesized from.
func bindSynthetic(ievs []exptrace.Event, refs []rawRef, igen []int, gi int,
	leftovers []int, items map[rawRef]*rawItem) error {
	for _, idx := range leftovers {
		ev := ievs[idx]
		if ev.Kind() != exptrace.EventStateTransition {
			return fmt.Errorf("%w: internal alignment error: unmatched non-synthetic event %s",
				tracev2.ErrCorrupt, ev.Kind())
		}
		st := ev.StateTransition()
		var parent *rawItem
		if st.Resource.Kind == exptrace.ResourceProc {
			from, to := st.Proc()
			if from != exptrace.ProcRunning || to != exptrace.ProcIdle {
				return fmt.Errorf("%w: internal alignment error: unmatched proc transition %s->%s",
					tracev2.ErrCorrupt, from, to)
			}
			parent = findItem(items, gi, tracev2.EvGoDestroySyscall, -1)
		} else {
			from, to := st.Goroutine()
			gid := int64(st.Resource.Goroutine())
			switch {
			case from == exptrace.GoRunning && to == exptrace.GoWaiting:
				parent = findItem(items, gi, tracev2.EvGoSwitch, -1)
			case from == exptrace.GoRunning && to == exptrace.GoNotExist:
				parent = findItem(items, gi, tracev2.EvGoSwitchDestroy, -1)
			case from == exptrace.GoRunnable && to == exptrace.GoRunning:
				if p := findItem(items, gi, tracev2.EvGoSwitch, gid); p != nil {
					parent = p
				} else {
					parent = findItem(items, gi, tracev2.EvGoSwitchDestroy, gid)
				}
			default:
				return fmt.Errorf("%w: internal alignment error: unmatched goroutine transition %s->%s",
					tracev2.ErrCorrupt, from, to)
			}
		}
		if parent == nil {
			return fmt.Errorf("%w: internal alignment error: synthetic event without parent raw event",
				tracev2.ErrCorrupt)
		}
		refs[idx] = parent.ref
	}
	return nil
}

func findItem(items map[rawRef]*rawItem, gi int, typ tracev2.EventType, gid int64) *rawItem {
	var best *rawItem
	for _, it := range items {
		if it.ref.gen != gi || it.ev.Type != typ {
			continue
		}
		if gid >= 0 && goroutineArg(it.ev) != gid {
			continue
		}
		if best == nil || compareRef(it.ref, best.ref) < 0 {
			best = it
		}
	}
	return best
}

// rawSignature derives the alignment signature of a raw event. The thread ID
// is always part of the signature so context-only events (blocks, syscalls,
// annotations) stay partitioned per M stream and FIFO pairing follows the
// actual per-thread wire ordering.
func rawSignature(it *rawItem) string {
	ev := it.ev
	m := it.ref.m
	switch ev.Type {
	case tracev2.EvCPUSample:
		// Samples live in NoThread structural batches; their thread identity
		// comes from the event arguments.
		return fmt.Sprintf("sample:%d:%d:%d", ev.Args[1], ev.Args[2], ev.Args[3])
	case tracev2.EvProcStart:
		return fmt.Sprintf("m=%d|p:idle->running:%d", m, ev.Args[1])
	case tracev2.EvProcStop, tracev2.EvProcSteal:
		// Both present as Proc Running->Idle; disambiguation is unnecessary
		// because they never coexist for the same transition.
		return fmt.Sprintf("m=%d|p:running->idle:%d", m, -1)
	case tracev2.EvProcStatus:
		return fmt.Sprintf("m=%d|p:status:%d", m, ev.Args[1])
	case tracev2.EvGoCreate:
		return fmt.Sprintf("m=%d|g:notexist->runnable:%d", m, ev.Args[1])
	case tracev2.EvGoCreateBlocked:
		return fmt.Sprintf("m=%d|g:notexist->waiting:%d", m, ev.Args[1])
	case tracev2.EvGoCreateSyscall:
		return fmt.Sprintf("m=%d|g:notexist->syscall:%d", m, ev.Args[1])
	case tracev2.EvGoStart:
		return fmt.Sprintf("m=%d|g:runnable->running:%d", m, ev.Args[1])
	case tracev2.EvGoDestroy:
		return fmt.Sprintf("m=%d|g:running->notexist:ctx", m)
	case tracev2.EvGoDestroySyscall:
		return fmt.Sprintf("m=%d|g:syscall->notexist:ctx", m)
	case tracev2.EvGoStop:
		return fmt.Sprintf("m=%d|g:running->runnable:ctx", m)
	case tracev2.EvGoBlock:
		return fmt.Sprintf("m=%d|g:running->waiting:ctx", m)
	case tracev2.EvGoUnblock:
		return fmt.Sprintf("m=%d|g:waiting->runnable:%d", m, ev.Args[1])
	case tracev2.EvGoSyscallBegin:
		return fmt.Sprintf("m=%d|g:running->syscall:ctx", m)
	case tracev2.EvGoSyscallEnd:
		return fmt.Sprintf("m=%d|g:syscall->running:ctx", m)
	case tracev2.EvGoSyscallEndBlocked:
		return fmt.Sprintf("m=%d|g:syscall->runnable:ctx", m)
	case tracev2.EvGoStatus, tracev2.EvGoStatusStack:
		return fmt.Sprintf("m=%d|g:status:%d", m, ev.Args[1])
	case tracev2.EvGoSwitch, tracev2.EvGoSwitchDestroy:
		return fmt.Sprintf("m=%d|g:waiting->runnable:%d", m, ev.Args[1])
	case tracev2.EvUserLog:
		return fmt.Sprintf("m=%d|log:ctx", m)
	case tracev2.EvUserTaskBegin:
		return fmt.Sprintf("m=%d|taskbegin:%d", m, ev.Args[1])
	case tracev2.EvUserTaskEnd:
		return fmt.Sprintf("m=%d|taskend:%d", m, ev.Args[1])
	case tracev2.EvUserRegionBegin:
		return fmt.Sprintf("m=%d|regionbegin:ctx", m)
	case tracev2.EvUserRegionEnd:
		return fmt.Sprintf("m=%d|regionend:ctx", m)
	case tracev2.EvGoLabel:
		return fmt.Sprintf("m=%d|label:ctx", m)
	case tracev2.EvSTWBegin:
		return fmt.Sprintf("m=%d|rangebegin:stw", m)
	case tracev2.EvSTWEnd:
		return fmt.Sprintf("m=%d|rangeend:stw", m)
	case tracev2.EvGCActive:
		return fmt.Sprintf("m=%d|rangeactive:gc", m)
	case tracev2.EvGCBegin:
		return fmt.Sprintf("m=%d|rangebegin:gc", m)
	case tracev2.EvGCEnd:
		return fmt.Sprintf("m=%d|rangeend:gc", m)
	case tracev2.EvGCSweepActive:
		return fmt.Sprintf("m=%d|rangeactive:sweep", m)
	case tracev2.EvGCSweepBegin:
		return fmt.Sprintf("m=%d|rangebegin:sweep", m)
	case tracev2.EvGCSweepEnd:
		return fmt.Sprintf("m=%d|rangeend:sweep", m)
	case tracev2.EvGCMarkAssistActive:
		return fmt.Sprintf("m=%d|rangeactive:assist", m)
	case tracev2.EvGCMarkAssistBegin:
		return fmt.Sprintf("m=%d|rangebegin:assist", m)
	case tracev2.EvGCMarkAssistEnd:
		return fmt.Sprintf("m=%d|rangeend:assist", m)
	case tracev2.EvProcsChange, tracev2.EvHeapAlloc, tracev2.EvHeapGoal:
		return fmt.Sprintf("m=%d|metric:%d", m, ev.Type)
	default:
		return fmt.Sprintf("m=%d|raw:%d", m, ev.Type)
	}
}

// interpSignature mirrors rawSignature for the interpreted event stream.
func interpSignature(ev exptrace.Event) string {
	m := int64(ev.Thread())
	switch ev.Kind() {
	case exptrace.EventStackSample:
		// Thread comes from the sample arguments, not the structural batch.
		return fmt.Sprintf("sample:%d:%d:%d", ev.Thread(), ev.Proc(), ev.Goroutine())
	case exptrace.EventStateTransition:
		st := ev.StateTransition()
		switch st.Resource.Kind {
		case exptrace.ResourceProc:
			from, to := st.Proc()
			switch {
			case from == exptrace.ProcUndetermined || from == exptrace.ProcNotExist || from == to:
				// Initial enumeration and same-state re-enumeration at
				// generation starts both originate from ProcStatus.
				return fmt.Sprintf("m=%d|p:status:%d", m, st.Resource.Proc())
			case to == exptrace.ProcRunning:
				return fmt.Sprintf("m=%d|p:idle->running:%d", m, st.Resource.Proc())
			default:
				return fmt.Sprintf("m=%d|p:running->idle:%d", m, -1)
			}
		case exptrace.ResourceGoroutine:
			gid := int64(st.Resource.Goroutine())
			from, to := st.Goroutine()
			return gInterpSig(m, from, to, gid)
		}
		return "trans:unknown"
	case exptrace.EventLog:
		return fmt.Sprintf("m=%d|log:ctx", m)
	case exptrace.EventTaskBegin:
		return fmt.Sprintf("m=%d|taskbegin:%d", m, ev.Task().ID)
	case exptrace.EventTaskEnd:
		return fmt.Sprintf("m=%d|taskend:%d", m, ev.Task().ID)
	case exptrace.EventRegionBegin:
		return fmt.Sprintf("m=%d|regionbegin:ctx", m)
	case exptrace.EventRegionEnd:
		return fmt.Sprintf("m=%d|regionend:ctx", m)
	case exptrace.EventLabel:
		return fmt.Sprintf("m=%d|label:ctx", m)
	case exptrace.EventRangeBegin:
		return fmt.Sprintf("m=%d|rangebegin:%s", m, rangeTag(ev.Range().Name))
	case exptrace.EventRangeActive:
		return fmt.Sprintf("m=%d|rangeactive:%s", m, rangeTag(ev.Range().Name))
	case exptrace.EventRangeEnd:
		return fmt.Sprintf("m=%d|rangeend:%s", m, rangeTag(ev.Range().Name))
	case exptrace.EventMetric:
		switch ev.Metric().Name {
		case "/sched/gomaxprocs:threads":
			return fmt.Sprintf("m=%d|metric:%d", m, tracev2.EvProcsChange)
		case "/memory/classes/heap/objects:bytes":
			return fmt.Sprintf("m=%d|metric:%d", m, tracev2.EvHeapAlloc)
		case "/gc/heap/goal:bytes":
			return fmt.Sprintf("m=%d|metric:%d", m, tracev2.EvHeapGoal)
		}
		return "metric:unknown"
	default:
		return "interp:" + ev.Kind().String()
	}
}

func gInterpSig(m int64, from, to exptrace.GoState, gid int64) string {
	ctx := func(s string) string { return fmt.Sprintf("m=%d|%s:ctx", m, s) }
	switch {
	case from == exptrace.GoUndetermined:
		return fmt.Sprintf("m=%d|g:status:%d", m, gid)
	case from == to:
		// Same-state re-enumeration at a later generation start.
		return fmt.Sprintf("m=%d|g:status:%d", m, gid)
	case from == exptrace.GoNotExist && to == exptrace.GoRunnable:
		return fmt.Sprintf("m=%d|g:notexist->runnable:%d", m, gid)
	case from == exptrace.GoNotExist && to == exptrace.GoWaiting:
		return fmt.Sprintf("m=%d|g:notexist->waiting:%d", m, gid)
	case from == exptrace.GoNotExist && to == exptrace.GoSyscall:
		return fmt.Sprintf("m=%d|g:notexist->syscall:%d", m, gid)
	case to == exptrace.GoRunning && from == exptrace.GoRunnable:
		return fmt.Sprintf("m=%d|g:runnable->running:%d", m, gid)
	case from == exptrace.GoRunning && to == exptrace.GoNotExist:
		return ctx("g:running->notexist")
	case from == exptrace.GoSyscall && to == exptrace.GoNotExist:
		return ctx("g:syscall->notexist")
	case from == exptrace.GoRunning && to == exptrace.GoRunnable:
		return ctx("g:running->runnable")
	case from == exptrace.GoRunning && to == exptrace.GoWaiting:
		return ctx("g:running->waiting")
	case from == exptrace.GoWaiting && to == exptrace.GoRunnable:
		return fmt.Sprintf("m=%d|g:waiting->runnable:%d", m, gid)
	case from == exptrace.GoRunning && to == exptrace.GoSyscall:
		return ctx("g:running->syscall")
	case from == exptrace.GoSyscall && to == exptrace.GoRunning:
		return ctx("g:syscall->running")
	case from == exptrace.GoSyscall && to == exptrace.GoRunnable:
		return ctx("g:syscall->runnable")
	default:
		return fmt.Sprintf("m=%d|g:unknown:%d", m, gid)
	}
}

func rangeTag(name string) string {
	switch {
	case strings.HasPrefix(name, "stop-the-world"):
		return "stw"
	case name == "GC concurrent mark phase":
		return "gc"
	case name == "GC incremental sweep":
		return "sweep"
	case name == "GC mark assist":
		return "assist"
	default:
		return "unknown"
	}
}

func uitoa(v int64) string {
	return strconv.FormatInt(v, 10)
}

// goroutineArg returns the explicit goroutine ID carried by events that name a
// goroutine in their arguments.
func goroutineArg(ev tracev2.RawEvent) int64 {
	switch ev.Type {
	case tracev2.EvGoStart, tracev2.EvGoUnblock, tracev2.EvGoSwitch, tracev2.EvGoSwitchDestroy:
		return int64(ev.Args[1])
	case tracev2.EvGoCreate, tracev2.EvGoCreateBlocked, tracev2.EvGoCreateSyscall:
		return int64(ev.Args[1])
	case tracev2.EvGoStatus, tracev2.EvGoStatusStack:
		return int64(ev.Args[1])
	default:
		return -1
	}
}

func compareRef(a, b rawRef) int {
	if a.gen != b.gen {
		return a.gen - b.gen
	}
	if a.isSample() || b.isSample() {
		if a.isSample() != b.isSample() {
			if a.isSample() {
				return -1
			}
			return 1
		}
		return a.sampleOrd - b.sampleOrd
	}
	if a.m != b.m {
		if a.m < b.m {
			return -1
		}
		return 1
	}
	return a.ord - b.ord
}
