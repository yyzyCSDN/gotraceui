package tslice

import (
	"fmt"
	"sort"

	exptrace "golang.org/x/exp/trace"

	"honnef.co/go/gotraceui/trace/tracev2"
)

// Boundary records a prerequisite that the source trace does not contain.
// Boundaries are never repaired by fabricating events; the slice simply notes
// that a start predates the recorded trace.
type Boundary struct {
	Resource   string `json:"resource"`
	ID         int64  `json:"id"`
	Generation int    `json:"source_generation"`
	Reason     string `json:"reason"`
}

type taskInfo struct {
	begin  int
	parent exptrace.TaskID
	end    []int
}

// index holds derived cross-reference structures over the interpreted event
// stream.
type index struct {
	gTrans map[int64][]int
	pTrans map[int64][]int
	mTrans map[int64][]int
	prev   map[int]int
	// mPrev links a state transition to the previous state transition on the
	// same thread, within one generation.
	mPrev map[int]int

	tasks map[exptrace.TaskID]*taskInfo

	// regionPair maps a RegionEnd interpreted index to its RegionBegin.
	regionPair map[int]int
	// rangePair maps range end indices to their begin indices.
	rangePair map[int]int
	// rangeActive marks RangeActive events (generation-start enumeration).
	rangeActive map[int]bool
}

func (e *engine) buildIndex() *index {
	idx := &index{
		gTrans:      map[int64][]int{},
		pTrans:      map[int64][]int{},
		mTrans:      map[int64][]int{},
		prev:        map[int]int{},
		mPrev:       map[int]int{},
		tasks:       map[exptrace.TaskID]*taskInfo{},
		regionPair:  map[int]int{},
		rangePair:   map[int]int{},
		rangeActive: map[int]bool{},
	}
	type regionKey struct {
		task exptrace.TaskID
		name string
	}
	regionStacks := map[int64][]regionKey{}
	regionBegins := map[int64][]int{}
	var stwStack []int
	gcBegin := map[uint64]int{}
	sweepStack := map[int64][]int{}
	assistStack := map[int64][]int{}

	for i, ev := range e.ievs {
		if e.igen[i] < 0 {
			continue
		}
		switch ev.Kind() {
		case exptrace.EventStateTransition:
			st := ev.StateTransition()
			switch st.Resource.Kind {
			case exptrace.ResourceGoroutine:
				gid := int64(st.Resource.Goroutine())
				if l := idx.gTrans[gid]; len(l) > 0 {
					idx.prev[i] = l[len(l)-1]
				}
				idx.gTrans[gid] = append(idx.gTrans[gid], i)
			case exptrace.ResourceProc:
				pid := int64(st.Resource.Proc())
				if l := idx.pTrans[pid]; len(l) > 0 {
					idx.prev[i] = l[len(l)-1]
				}
				idx.pTrans[pid] = append(idx.pTrans[pid], i)
			}
			// Thread streams reset at generation boundaries; only link
			// transitions within the same generation.
			if m := int64(ev.Thread()); m != int64(exptrace.NoThread) {
				if l := idx.mTrans[m]; len(l) > 0 && e.igen[l[len(l)-1]] == e.igen[i] {
					idx.mPrev[i] = l[len(l)-1]
				}
				idx.mTrans[m] = append(idx.mTrans[m], i)
			}
		case exptrace.EventTaskBegin:
			t := ev.Task()
			idx.tasks[t.ID] = &taskInfo{begin: i, parent: t.Parent}
		case exptrace.EventTaskEnd:
			t := ev.Task()
			ti, ok := idx.tasks[t.ID]
			if !ok {
				ti = &taskInfo{begin: -1, parent: t.Parent}
				idx.tasks[t.ID] = ti
			}
			ti.end = append(ti.end, i)
		case exptrace.EventRegionBegin:
			gid := int64(ev.Goroutine())
			r := ev.Region()
			k := regionKey{task: r.Task, name: r.Type}
			regionStacks[gid] = append(regionStacks[gid], k)
			regionBegins[gid] = append(regionBegins[gid], i)
		case exptrace.EventRegionEnd:
			gid := int64(ev.Goroutine())
			r := ev.Region()
			k := regionKey{task: r.Task, name: r.Type}
			st := regionStacks[gid]
			matched := -1
			for p := len(st) - 1; p >= 0; p-- {
				if st[p] == k {
					matched = p
					break
				}
			}
			if matched >= 0 {
				// Pair with the begin event at the same stack position.
				idx.regionPair[i] = regionBegins[gid][matched]
				regionStacks[gid] = append(st[:matched], st[matched+1:]...)
				regionBegins[gid] = append(regionBegins[gid][:matched], regionBegins[gid][matched+1:]...)
			}
		case exptrace.EventRangeBegin:
			switch e.rawType(i) {
			case tracev2.EvSTWBegin:
				stwStack = append(stwStack, i)
			case tracev2.EvGCBegin:
				gcBegin[e.rawArgs(i)[1]] = i
			case tracev2.EvGCSweepBegin:
				pid := int64(ev.Proc())
				sweepStack[pid] = append(sweepStack[pid], i)
			case tracev2.EvGCMarkAssistBegin:
				gid := int64(ev.Goroutine())
				assistStack[gid] = append(assistStack[gid], i)
			}
		case exptrace.EventRangeActive:
			idx.rangeActive[i] = true
		case exptrace.EventRangeEnd:
			switch e.rawType(i) {
			case tracev2.EvSTWEnd:
				if n := len(stwStack); n > 0 {
					idx.rangePair[i] = stwStack[n-1]
					stwStack = stwStack[:n-1]
				}
			case tracev2.EvGCEnd:
				if b, ok := gcBegin[e.rawArgs(i)[1]]; ok {
					idx.rangePair[i] = b
				}
			case tracev2.EvGCSweepEnd:
				pid := int64(ev.Proc())
				if l := sweepStack[pid]; len(l) > 0 {
					idx.rangePair[i] = l[len(l)-1]
					sweepStack[pid] = l[:len(l)-1]
				}
			case tracev2.EvGCMarkAssistEnd:
				gid := int64(ev.Goroutine())
				if l := assistStack[gid]; len(l) > 0 {
					idx.rangePair[i] = l[len(l)-1]
					assistStack[gid] = l[:len(l)-1]
				}
			}
		}
	}
	return idx
}

// validateSelection checks the selection against the parsed trace.
func (e *engine) validateSelection(sel Selection) error {
	known := map[int64]bool{}
	for i := range e.ievs {
		if e.igen[i] < 0 || e.ievs[i].Kind() != exptrace.EventStateTransition {
			continue
		}
		st := e.ievs[i].StateTransition()
		if st.Resource.Kind == exptrace.ResourceGoroutine {
			known[int64(st.Resource.Goroutine())] = true
		}
	}
	for _, g := range dedupe(sel.Goroutines) {
		if !known[g] {
			return fmt.Errorf("%w: goroutine %d does not appear in the trace", ErrInvalidSelection, g)
		}
	}
	traceStart := int64(e.ievs[0].Time())
	traceEnd := int64(e.ievs[len(e.ievs)-1].Time())
	if sel.WindowB <= traceStart || sel.WindowA >= traceEnd {
		return fmt.Errorf("%w: window [%d,%d) does not intersect trace timeline [%d,%d]",
			ErrInvalidSelection, sel.WindowA, sel.WindowB, traceStart, traceEnd)
	}
	return nil
}

// computeClosure selects seeds and expands dependencies until a fixed point.
func (e *engine) computeClosure(sel Selection) error {
	idx := e.buildIndex()
	gset := map[int64]bool{}
	for _, g := range dedupe(sel.Goroutines) {
		gset[g] = true
	}

	var queue []int
	enqueue := func(i int, role eventRole) {
		ref := e.refs[i]
		if cur, ok := e.role[ref]; !ok || role == roleSeed && cur != roleSeed {
			e.role[ref] = role
			e.keptOrder = append(e.keptOrder, ref)
		}
		queue = append(queue, i)
	}

	// Phase 1: identify seeds.
	seedCount := 0
	for i, ev := range e.ievs {
		if e.igen[i] < 0 {
			continue
		}
		if e.isSeed(ev, i, gset, sel) {
			enqueue(i, roleSeed)
			seedCount++
		}
	}
	if seedCount == 0 {
		return fmt.Errorf("%w: no events of goroutines %v in window [%d,%d)",
			ErrInvalidSelection, sel.Goroutines, sel.WindowA, sel.WindowB)
	}

	// Phase 2: fixed-point dependency expansion.
	queued := map[int]bool{}
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		if queued[i] {
			continue
		}
		queued[i] = true
		e.expand(i, idx, enqueue)
	}
	return nil
}

// isSeed reports whether interpreted event i is a user-selected seed.
func (e *engine) isSeed(ev exptrace.Event, i int, gset map[int64]bool, sel Selection) bool {
	ts := eventTime(ev)
	if ts < sel.WindowA || ts >= sel.WindowB {
		return false
	}
	related := gset[int64(ev.Goroutine())]
	if ev.Kind() == exptrace.EventStateTransition {
		st := ev.StateTransition()
		if st.Resource.Kind == exptrace.ResourceGoroutine && gset[int64(st.Resource.Goroutine())] {
			related = true
		}
	}
	if ev.Kind() == exptrace.EventStackSample && gset[int64(ev.Goroutine())] {
		related = true
	}
	if !related {
		return false
	}
	switch ev.Kind() {
	case exptrace.EventStateTransition,
		exptrace.EventStackSample,
		exptrace.EventLog,
		exptrace.EventTaskBegin,
		exptrace.EventTaskEnd,
		exptrace.EventRegionBegin,
		exptrace.EventRegionEnd,
		// Runtime range annotations on the selected goroutines (STW, GC mark
		// assist) carry native GC state required to interpret their spans.
		exptrace.EventRangeBegin,
		exptrace.EventRangeActive,
		exptrace.EventRangeEnd:
		return true
	default:
		// Labels and pure metrics are not part of the enumerated seed classes
		// and carry no prerequisite semantics, so they never enter a slice.
		return false
	}
}

// expand pulls every recorded prerequisite of event i.
func (e *engine) expand(i int, idx *index, enqueue func(int, eventRole)) {
	ev := e.ievs[i]
	require := func(prereq int, reason string) {
		if prereq < 0 {
			return
		}
		if _, ok := e.parent[prereq]; !ok {
			e.parent[prereq] = depEdge{prereq: i, reason: reason}
		}
		enqueue(prereq, roleSupport)
	}
	boundary := func(resource string, id int64, reason string) {
		e.bounds = append(e.bounds, Boundary{
			Resource:   resource,
			ID:         id,
			Generation: e.igen[i] + 1,
			Reason:     reason,
		})
	}

	// Uniform context rule: any retained event executed by a goroutine or on a
	// proc requires that resource's recorded state predecessor. These are real
	// scheduler state prerequisites, never reader conveniences.
	if ev.Goroutine() != exptrace.NoGoroutine {
		if head, ok := e.headTransition(idx.gTrans[int64(ev.Goroutine())], i); ok {
			require(head, fmt.Sprintf("executor goroutine %d state predecessor", ev.Goroutine()))
		} else {
			boundary("goroutine", int64(ev.Goroutine()), "goroutine state predates recorded trace")
		}
	}
	if ev.Proc() != exptrace.NoProc {
		if head, ok := e.headTransition(idx.pTrans[int64(ev.Proc())], i); ok {
			require(head, fmt.Sprintf("executor proc %d state predecessor", ev.Proc()))
		} else {
			boundary("proc", int64(ev.Proc()), "proc state predates recorded trace")
		}
	}
	// Thread prefix rule: on a real M the reader replays the thread's event
	// stream in order, so every retained event needs the complete preceding
	// transition history of that thread within the generation. This is what
	// binds and releases goroutine/proc context (for example the destroy or
	// block that frees the M immediately before a retained GoStart).
	if m := int64(ev.Thread()); m != int64(exptrace.NoThread) {
		if head, ok := e.headTransition(idx.mTrans[m], i); ok {
			require(head, fmt.Sprintf("thread %d scheduler context predecessor", m))
		}
	}

	switch ev.Kind() {
	case exptrace.EventStateTransition:
		e.expandTransition(i, idx, require, boundary)
	case exptrace.EventTaskBegin:
		t := ev.Task()
		if t.Parent != exptrace.NoTask {
			if pi, ok := idx.tasks[t.Parent]; ok && pi.begin >= 0 {
				require(pi.begin, fmt.Sprintf("parent task %d begin", t.Parent))
			} else {
				boundary("task", int64(t.Parent), "parent task begin is not recorded")
			}
		}
	case exptrace.EventTaskEnd:
		t := ev.Task()
		if ti, ok := idx.tasks[t.ID]; ok && ti.begin >= 0 {
			require(ti.begin, fmt.Sprintf("task %d begin", t.ID))
		} else {
			boundary("task", int64(t.ID), "task begin is not recorded")
		}
	case exptrace.EventRegionBegin:
		r := ev.Region()
		if r.Task != exptrace.NoTask && r.Task != exptrace.BackgroundTask {
			if ti, ok := idx.tasks[r.Task]; ok && ti.begin >= 0 {
				require(ti.begin, fmt.Sprintf("region task %d begin", r.Task))
			} else {
				boundary("task", int64(r.Task), "region task begin is not recorded")
			}
		}
	case exptrace.EventRegionEnd:
		if begin, ok := idx.regionPair[i]; ok {
			require(begin, "nested region begin")
		} else {
			boundary("region", int64(ev.Goroutine()), "region start is not recorded")
		}
	case exptrace.EventLog:
		l := ev.Log()
		if l.Task != exptrace.NoTask {
			if ti, ok := idx.tasks[l.Task]; ok && ti.begin >= 0 {
				require(ti.begin, fmt.Sprintf("log task %d begin", l.Task))
			} else {
				boundary("task", int64(l.Task), "log task begin is not recorded")
			}
		}
	case exptrace.EventRangeActive:
		boundary(e.rangeResource(i), e.rangeResourceID(i), "range active at generation start; begin is not recorded")
	case exptrace.EventRangeEnd:
		if begin, ok := idx.rangePair[i]; ok {
			require(begin, "paired range begin")
		} else {
			boundary(e.rangeResource(i), e.rangeResourceID(i), "range begin is not recorded")
		}
	}
}

func (e *engine) expandTransition(i int, idx *index, require func(int, string), boundary func(string, int64, string)) {
	ev := e.ievs[i]
	st := ev.StateTransition()
	// Require the previous transition on the same thread, making the whole
	// per-thread prefix transitive retention.
	if m := int64(ev.Thread()); m != int64(exptrace.NoThread) {
		if prev, ok := idx.mPrev[i]; ok {
			require(prev, fmt.Sprintf("thread %d previous transition", m))
		}
	}
	if st.Resource.Kind == exptrace.ResourceProc {
		// ProcStart reports NoProc as its executing context, so the uniform
		// context rule cannot pull its predecessor. Require the previous P
		// transition explicitly; a missing predecessor means the proc was
		// enumerated at a generation start.
		if prev, ok := idx.prev[i]; ok {
			require(prev, fmt.Sprintf("proc %d sequence predecessor", st.Resource.Proc()))
		} else {
			boundary("proc", int64(st.Resource.Proc()), "proc enumerated at generation start")
		}
		return
	}
	if st.Resource.Kind != exptrace.ResourceGoroutine {
		return
	}
	gid := int64(st.Resource.Goroutine())
	from, _ := st.Goroutine()
	if prev, ok := idx.prev[i]; ok {
		require(prev, fmt.Sprintf("goroutine %d sequence predecessor", gid))
	} else {
		// No predecessor: either a recorded creation or a generation-start
		// enumeration whose origin is unrecorded.
		switch e.rawType(i) {
		case tracev2.EvGoCreate, tracev2.EvGoCreateBlocked, tracev2.EvGoCreateSyscall:
			// Recorded creation start; the uniform rule already pulled the
			// creator and proc context.
		default:
			boundary("goroutine", gid, "goroutine enumerated at generation start; creation is not recorded")
		}
	}
	// Explicit creator/waker facts need the acting goroutine's history. The
	// uniform executor rule already covers it when ev.Goroutine() is set; this
	// makes the intent explicit for unblocks without a current goroutine.
	switch e.rawType(i) {
	case tracev2.EvGoUnblock, tracev2.EvGoSwitch, tracev2.EvGoSwitchDestroy:
		if from == exptrace.GoWaiting && ev.Goroutine() != exptrace.NoGoroutine {
			if head, ok := e.headTransition(idx.gTrans[int64(ev.Goroutine())], i); ok {
				require(head, fmt.Sprintf("waker goroutine %d state predecessor", ev.Goroutine()))
			}
		}
	}
}

func (e *engine) headTransition(list []int, before int) (int, bool) {
	n := sort.Search(len(list), func(k int) bool { return list[k] >= before })
	if n == 0 {
		return 0, false
	}
	return list[n-1], true
}

func (e *engine) rawType(i int) tracev2.EventType {
	return e.items[e.refs[i]].ev.Type
}

func (e *engine) rawArgs(i int) []uint64 {
	return e.items[e.refs[i]].ev.Args
}

func (e *engine) rangeResource(i int) string {
	switch e.rawType(i) {
	case tracev2.EvGCSweepBegin, tracev2.EvGCSweepEnd, tracev2.EvGCSweepActive:
		return "proc"
	default:
		return "goroutine"
	}
}

func (e *engine) rangeResourceID(i int) int64 {
	if e.rangeResource(i) == "proc" {
		return int64(e.ievs[i].Proc())
	}
	return int64(e.ievs[i].Goroutine())
}

func dedupe(in []int64) []int64 {
	seen := map[int64]struct{}{}
	var out []int64
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
