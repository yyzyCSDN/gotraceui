package tslice

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"honnef.co/go/gotraceui/trace/tracev2"
)

// genEncoding records the per-generation remapping performed while encoding.
type genEncoding struct {
	srcGen      uint64
	outGen      uint64
	stringMap   map[uint64]uint64
	stackMap    map[uint64]uint64
	keptStrings []StringEntry
	keptStacks  []StackEntry
	dropStrings int
	dropStacks  int
	keptSamples int
	keptTimed   int
	threadBatch int
}

// StringEntry is a retained string definition.
type StringEntry struct {
	SourceID uint64 `json:"source_id"`
	OutputID uint64 `json:"output_id"`
	Value    string `json:"value"`
}

// StackEntry is a retained stack definition.
type StackEntry struct {
	SourceID uint64               `json:"source_id"`
	OutputID uint64               `json:"output_id"`
	Frames   []tracev2.StackFrame `json:"-"`
}

func (e *engine) encode() ([]byte, error) {
	// Retained generations in source order.
	var retained []int
	for gi := range e.raw.Generations {
		used := false
		for ref := range e.role {
			if ref.gen == gi {
				used = true
				break
			}
		}
		if used {
			retained = append(retained, gi)
		}
	}
	if len(retained) == 0 {
		return nil, fmt.Errorf("%w: selection produced no events", ErrInvalidSelection)
	}

	var out bytes.Buffer
	w, err := tracev2.NewWriter(&out, e.v)
	if err != nil {
		return nil, err
	}
	e.enc = make([]*genEncoding, 0, len(retained))

	for outGen, gi := range retained {
		ge, err := e.encodeGeneration(w, gi, uint64(outGen)+1)
		if err != nil {
			return nil, err
		}
		e.enc = append(e.enc, ge)
	}
	return out.Bytes(), nil
}

func (e *engine) encodeGeneration(w *tracev2.RawWriter, gi int, outGen uint64) (*genEncoding, error) {
	g := e.raw.Generations[gi]
	ge := &genEncoding{
		srcGen:    g.Gen,
		outGen:    outGen,
		stringMap: map[uint64]uint64{0: 0},
		stackMap:  map[uint64]uint64{0: 0},
	}

	// Collect kept refs in this generation, separated by kind, in deterministic
	// source order.
	var keptTimed []rawRef
	var keptSamples []rawRef
	for ref, role := range e.role {
		if role == roleNone || ref.gen != gi {
			continue
		}
		if ref.isSample() {
			keptSamples = append(keptSamples, ref)
		} else {
			keptTimed = append(keptTimed, ref)
		}
	}
	slices.SortFunc(keptTimed, func(a, b rawRef) int { return compareRef(a, b) })
	slices.SortFunc(keptSamples, func(a, b rawRef) int { return compareRef(a, b) })

	// Build the string and stack closure.
	usedStrings := map[uint64]bool{}
	usedStacks := map[uint64]bool{}
	noteString := func(id uint64) {
		if id != 0 {
			usedStrings[id] = true
		}
	}
	noteStack := func(id uint64) {
		if id != 0 {
			usedStacks[id] = true
		}
	}
	for _, ref := range keptTimed {
		it := e.items[ref]
		spec := e.spec[it.ev.Type]
		for _, argIdx := range spec.StringIDs {
			noteString(it.ev.Args[argIdx])
		}
		for _, argIdx := range spec.StackIDs {
			noteStack(it.ev.Args[argIdx])
		}
	}
	for _, ref := range keptSamples {
		noteStack(e.items[ref].ev.Args[4])
	}
	// Stacks reference function and file strings.
	for sid := range usedStacks {
		frames, ok := g.Stacks[sid]
		if !ok {
			return nil, fmt.Errorf("%w: stack %d referenced but not defined in gen %d",
				tracev2.ErrCorrupt, sid, g.Gen)
		}
		for _, f := range frames {
			noteString(f.FuncID)
			noteString(f.FileID)
		}
	}

	// Assign dense output IDs in ascending source-ID order. ID 0 is the
	// implicit empty string/stack and is never stored on the wire.
	ge.stringMap = denseMap(usedStrings)
	ge.stackMap = denseMap(usedStacks)
	for oldID, newID := range ge.stringMap {
		if oldID == 0 {
			continue
		}
		val, ok := g.Strings[oldID]
		if !ok {
			return nil, fmt.Errorf("%w: string %d referenced but not defined in gen %d",
				tracev2.ErrCorrupt, oldID, g.Gen)
		}
		ge.keptStrings = append(ge.keptStrings, StringEntry{SourceID: oldID, OutputID: newID, Value: val})
	}
	slices.SortFunc(ge.keptStrings, func(a, b StringEntry) int { return cmpUint64(a.OutputID, b.OutputID) })
	for oldID, newID := range ge.stackMap {
		if oldID == 0 {
			continue
		}
		frames, ok := g.Stacks[oldID]
		if !ok {
			return nil, fmt.Errorf("%w: stack %d referenced but not defined in gen %d",
				tracev2.ErrCorrupt, oldID, g.Gen)
		}
		ge.keptStacks = append(ge.keptStacks, StackEntry{SourceID: oldID, OutputID: newID, Frames: frames})
	}
	slices.SortFunc(ge.keptStacks, func(a, b StackEntry) int { return cmpUint64(a.OutputID, b.OutputID) })
	ge.dropStrings = len(g.Strings) - (len(ge.stringMap) - 1)
	ge.dropStacks = len(g.Stacks) - (len(ge.stackMap) - 1)
	ge.keptSamples = len(keptSamples)
	ge.keptTimed = len(keptTimed)

	// Sync / frequency batch first, preserving the source base time so clock
	// snapshots keep their native coordinates.
	if e.v >= tracev2.VersionGo125 {
		batch := &tracev2.RawBatch{Gen: outGen, M: tracev2.NoThread, TimeUnits: g.SyncBaseUnits}
		batch.Events = append(batch.Events, tracev2.RawEvent{Type: tracev2.EvSync})
		batch.Events = append(batch.Events, tracev2.RawEvent{Type: tracev2.EvFrequency, Args: []uint64{g.Frequency}})
		if g.HasClockSnapshot {
			snap := g.ClockSnapshot
			batch.Events = append(batch.Events, snap)
		}
		if err := w.WriteBatch(batch); err != nil {
			return nil, err
		}
	} else {
		batch := &tracev2.RawBatch{
			Gen:       outGen,
			M:         tracev2.NoThread,
			TimeUnits: g.FrequencyBaseUnits,
			Events:    []tracev2.RawEvent{{Type: tracev2.EvFrequency, Args: []uint64{g.Frequency}}},
		}
		if err := w.WriteBatch(batch); err != nil {
			return nil, err
		}
	}

	// Regular event batches, grouped by source thread, with recomputed deltas
	// and absolute timer coordinates preserved.
	threadIDs := map[int64]bool{}
	for _, ref := range keptTimed {
		threadIDs[ref.m] = true
	}
	var ms []int64
	for m := range threadIDs {
		ms = append(ms, m)
	}
	slices.Sort(ms)
	for _, m := range ms {
		n, err := e.writeThreadBatches(w, gi, outGen, m, ge)
		if err != nil {
			return nil, err
		}
		ge.threadBatch += n
	}

	// CPU samples, with absolute timer units and remapped stacks.
	if len(keptSamples) > 0 {
		batch := &tracev2.RawBatch{Gen: outGen, M: tracev2.NoThread, TimeUnits: g.MinUnits}
		batch.Events = append(batch.Events, tracev2.RawEvent{Type: tracev2.EvCPUSamples})
		for so, ref := range keptSamples {
			ev := e.items[ref].ev
			args := slices.Clone(ev.Args)
			args[4] = ge.stackMap[args[4]]
			batch.Events = append(batch.Events, tracev2.RawEvent{
				Type:     tracev2.EvCPUSample,
				Args:     args,
				AbsUnits: ev.AbsUnits,
			})
			e.outRefOf[ref] = sampleRef(int(outGen)-1, so)
		}
		if err := e.writeOversizedStructural(w, batch); err != nil {
			return nil, err
		}
	}

	// Stack definitions with remapped string references.
	if len(ge.keptStacks) > 0 {
		batch := &tracev2.RawBatch{Gen: outGen, M: tracev2.NoThread, TimeUnits: g.MinUnits}
		batch.Events = append(batch.Events, tracev2.RawEvent{Type: tracev2.EvStacks})
		for _, st := range ge.keptStacks {
			args := []uint64{st.OutputID, uint64(len(st.Frames))}
			for _, f := range st.Frames {
				args = append(args, f.PC, ge.stringMap[f.FuncID], ge.stringMap[f.FileID], f.Line)
			}
			batch.Events = append(batch.Events, tracev2.RawEvent{Type: tracev2.EvStack, Args: args})
		}
		if err := e.writeOversizedStructural(w, batch); err != nil {
			return nil, err
		}
	}

	// String definitions.
	if len(ge.keptStrings) > 0 {
		batch := &tracev2.RawBatch{Gen: outGen, M: tracev2.NoThread, TimeUnits: g.MinUnits}
		batch.Events = append(batch.Events, tracev2.RawEvent{Type: tracev2.EvStrings})
		for _, s := range ge.keptStrings {
			batch.Events = append(batch.Events, tracev2.RawEvent{
				Type: tracev2.EvString,
				Args: []uint64{s.OutputID},
				Data: []byte(s.Value),
			})
		}
		if err := e.writeOversizedStructural(w, batch); err != nil {
			return nil, err
		}
	}

	return ge, nil
}

// writeThreadBatches emits the retained timed events of one thread, splitting
// batches at the wire-format size limit and recomputing timestamp deltas.
func (e *engine) writeThreadBatches(w *tracev2.RawWriter, gi int, outGen uint64, m int64, ge *genEncoding) (int, error) {
	refs := make([]rawRef, 0)
	for ref := range e.role {
		if ref.gen == gi && !ref.isSample() && ref.m == m {
			refs = append(refs, ref)
		}
	}
	slices.SortFunc(refs, func(a, b rawRef) int { return a.ord - b.ord })
	if len(refs) == 0 {
		return 0, nil
	}

	nbatches := 0
	var batch *tracev2.RawBatch
	var lastUnits uint64
	outOrd := 0
	flush := func() error {
		if batch == nil || len(batch.Events) == 0 {
			return nil
		}
		if err := w.WriteBatch(batch); err != nil {
			return err
		}
		nbatches++
		batch = nil
		return nil
	}
	for _, ref := range refs {
		src := e.items[ref].ev
		ev := tracev2.RawEvent{Type: src.Type, Args: slices.Clone(src.Args), AbsUnits: src.AbsUnits}
		spec := e.spec[ev.Type]
		for _, argIdx := range spec.StringIDs {
			ev.Args[argIdx] = ge.stringMap[ev.Args[argIdx]]
		}
		for _, argIdx := range spec.StackIDs {
			ev.Args[argIdx] = ge.stackMap[ev.Args[argIdx]]
		}
		if batch == nil {
			batch = &tracev2.RawBatch{Gen: outGen, M: m, TimeUnits: ev.AbsUnits}
			lastUnits = ev.AbsUnits
			ev.Args[0] = 0
		} else {
			if ev.AbsUnits < lastUnits {
				return 0, fmt.Errorf("%w: event timestamp regression on M %d", tracev2.ErrCorrupt, m)
			}
			ev.Args[0] = ev.AbsUnits - lastUnits
			lastUnits = ev.AbsUnits
			if e.wouldOversize(batch, ev) {
				if err := flush(); err != nil {
					return 0, err
				}
				batch = &tracev2.RawBatch{Gen: outGen, M: m, TimeUnits: ev.AbsUnits}
				lastUnits = ev.AbsUnits
				ev.Args[0] = 0
			}
		}
		batch.Events = append(batch.Events, ev)
		e.outRefOf[ref] = timedRef(int(outGen)-1, m, outOrd)
		outOrd++
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return nbatches, nil
}

// wouldOversize reports whether adding ev would grow the batch beyond the
// wire-format limit. Header size is 1 byte plus four uvarints (at most 41
// bytes); the runtime accounts only batch data, so we compare event payloads.
func (e *engine) wouldOversize(batch *tracev2.RawBatch, ev tracev2.RawEvent) bool {
	const framing = 1 + binary.MaxVarintLen64*4
	size := framing
	for _, x := range batch.Events {
		size += e.eventWireSize(x)
	}
	size += e.eventWireSize(ev)
	return size > tracev2.MaxBatchSize
}

func (e *engine) eventWireSize(ev tracev2.RawEvent) int {
	size := 1
	for _, a := range ev.Args {
		size += uvarintLen(a)
	}
	if e.spec[ev.Type].HasData {
		size += uvarintLen(uint64(len(ev.Data))) + len(ev.Data)
	}
	return size
}

// writeOversizedStructural splits structural batches (strings, stacks,
// samples) if they exceed the batch limit, repeating the section marker.
func (e *engine) writeOversizedStructural(w *tracev2.RawWriter, full *tracev2.RawBatch) error {
	marker := full.Events[0]
	rest := full.Events[1:]
	cur := &tracev2.RawBatch{Gen: full.Gen, M: full.M, TimeUnits: full.TimeUnits}
	cur.Events = []tracev2.RawEvent{marker}
	curSize := e.eventWireSize(marker)
	flush := func() error {
		if len(cur.Events) <= 1 {
			return nil
		}
		if err := w.WriteBatch(cur); err != nil {
			return err
		}
		cur.Events = []tracev2.RawEvent{marker}
		curSize = e.eventWireSize(marker)
		return nil
	}
	for _, ev := range rest {
		n := e.eventWireSize(ev)
		if curSize+n > tracev2.MaxBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		cur.Events = append(cur.Events, ev)
		curSize += n
	}
	return flush()
}

func denseMap(used map[uint64]bool) map[uint64]uint64 {
	ids := make([]uint64, 0, len(used))
	for id := range used {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	m := map[uint64]uint64{0: 0}
	for n, id := range ids {
		m[id] = uint64(n + 1)
	}
	return m
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
