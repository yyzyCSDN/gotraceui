package tracev2

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
)

// Errors returned by the raw reader. Callers should use errors.Is to detect
// the error class.
var (
	// ErrCorrupt indicates malformed trace bytes.
	ErrCorrupt = errors.New("corrupt trace")
	// ErrUnsupported indicates a trace format that this package cannot handle,
	// such as a v1 trace, a newer than supported version, or experimental
	// batches.
	ErrUnsupported = errors.New("unsupported trace format")
)

// NoThread is the sentinel M ID used by structural batches (strings, stacks,
// CPU samples, sync). On the wire it is encoded as uint64(-1).
const NoThread int64 = -1

// RawEvent is one decoded wire event.
type RawEvent struct {
	Type EventType
	// Args contains the wire arguments in their original order. For timed
	// events Args[0] is the timestamp delta relative to the batch base (or to
	// the previous event within the batch). Structural events keep their
	// documented argument layout.
	Args []uint64
	// Data is the byte trailer of HasData events (currently EvString).
	Data []byte
	// AbsUnits is the event's absolute timestamp in trace timer units. It is
	// populated for timed events in M batches (batch base plus accumulated
	// deltas) and for CPU samples, whose time argument is already absolute.
	// It is zero for all other structural events.
	AbsUnits uint64
}

// BatchKind classifies a wire batch by the marker event in its data.
type BatchKind uint8

const (
	BatchUnknown BatchKind = iota
	// BatchEvents is a regular per-M batch of timed events.
	BatchEvents
	// BatchStrings is the string dictionary batch.
	BatchStrings
	// BatchStacks is the stack table batch.
	BatchStacks
	// BatchCPUSamples is the CPU profiling sample batch.
	BatchCPUSamples
	// BatchFrequency is a pre-Go-1.25 sync batch holding a lone Frequency.
	BatchFrequency
	// BatchSync is a Go 1.25+ sync batch holding Frequency and optionally a
	// clock snapshot.
	BatchSync
)

// RawBatch is one decoded trace batch.
type RawBatch struct {
	// Gen is the generation number carried in the batch header.
	Gen uint64
	// M is the thread ID; NoThread (-1) for structural batches.
	M int64
	// TimeUnits is the base timestamp in trace timer units.
	TimeUnits uint64
	// Kind classifies the batch.
	Kind BatchKind
	// Events are the decoded events contained in the batch, including marker
	// events such as EvStrings, EvStacks, EvCPUSamples, EvFrequency, EvSync.
	Events []RawEvent
}

// StackFrame is one frame of a stack table entry.
type StackFrame struct {
	PC     uint64
	FuncID uint64
	FileID uint64
	Line   uint64
}

// RawGeneration holds all decoded data of one trace generation.
type RawGeneration struct {
	// Gen is the generation number as recorded on the wire.
	Gen uint64
	// Batches preserves wire order, including structural batches.
	Batches []*RawBatch
	// Strings maps string table ID to its value.
	Strings map[uint64]string
	// Stacks maps stack table ID to its frames.
	Stacks map[uint64][]StackFrame
	// CPUSamples are the generation's CPU samples in wire order; their
	// AbsUnits carry absolute timer units.
	CPUSamples []RawEvent
	// Frequency is the timer frequency in timestamp units per second.
	Frequency uint64
	// FrequencyBaseUnits is the base timestamp of the batch containing the
	// Frequency event.
	FrequencyBaseUnits uint64
	// SyncBaseUnits is the base timestamp of the Go 1.25 sync batch.
	SyncBaseUnits uint64
	// HasClockSnapshot reports whether a clock snapshot is present.
	HasClockSnapshot bool
	// ClockSnapshot is the EvClockSnapshot event, with AbsUnits resolved to
	// absolute timer units. Only valid when HasClockSnapshot is true.
	ClockSnapshot RawEvent
	// MinUnits is the smallest batch base timestamp in the generation.
	MinUnits uint64
}

// RawTrace is a fully decoded, non-experimental v2 trace.
type RawTrace struct {
	Version     Version
	Generations []*RawGeneration
}

// RawReader decodes trace v2 wire bytes.
type RawReader struct {
	r     *bufio.Reader
	v     Version
	specs []EventSpec
}

// NewReader validates the header and returns a raw reader.
func NewReader(r io.Reader) (*RawReader, error) {
	br := bufio.NewReader(r)
	var hdr [16]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, fmt.Errorf("%w: reading header: %v", ErrCorrupt, err)
	}
	v, err := parseHeader(hdr[:])
	if err != nil {
		return nil, err
	}
	return &RawReader{r: br, v: v, specs: v.Specs()}, nil
}

// Version returns the trace version.
func (r *RawReader) Version() Version { return r.v }

func parseHeader(hdr []byte) (Version, error) {
	prefix := []byte("go 1.")
	if !bytes.HasPrefix(hdr, prefix) {
		return 0, fmt.Errorf("%w: bad file format: not a Go execution trace", ErrCorrupt)
	}
	if hdr[7] != ' ' || !bytes.Equal(hdr[8:13], []byte("trace")) || !bytes.Equal(hdr[13:16], []byte{0, 0, 0}) {
		return 0, fmt.Errorf("%w: malformed trace header %q", ErrCorrupt, hdr)
	}
	if hdr[5] < '0' || hdr[5] > '9' || hdr[6] < '0' || hdr[6] > '9' {
		return 0, fmt.Errorf("%w: malformed trace version in header", ErrCorrupt)
	}
	v := Version(uint32(hdr[5]-'0')*10 + uint32(hdr[6]-'0'))
	switch v {
	case 11, 19, 21:
		return 0, fmt.Errorf("%w: v1 trace (go 1.%d) is not supported, only non-experimental v2 traces", ErrUnsupported, v)
	case 26:
		return 0, fmt.Errorf("%w: go 1.26 traces are not supported by this version of the slicer", ErrUnsupported)
	}
	if !v.Supported() {
		return 0, fmt.Errorf("%w: unknown or unsupported trace version go 1.%d", ErrUnsupported, v)
	}
	return v, nil
}

// ReadAll decodes every batch in the stream.
func (r *RawReader) ReadAll() (*RawTrace, error) {
	out := &RawTrace{Version: r.v}
	gens := map[uint64]*RawGeneration{}
	var lastGen uint64
	minUnitsSet := map[uint64]bool{}

	for {
		b, err := r.readBatch()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		g, ok := gens[b.Gen]
		if !ok {
			if b.Gen == 0 {
				return nil, fmt.Errorf("%w: invalid generation number 0", ErrCorrupt)
			}
			if lastGen != 0 && b.Gen <= lastGen {
				return nil, fmt.Errorf("%w: generations out of order: gen %d after %d", ErrCorrupt, b.Gen, lastGen)
			}
			g = &RawGeneration{
				Gen:     b.Gen,
				Strings: map[uint64]string{},
				Stacks:  map[uint64][]StackFrame{},
			}
			gens[b.Gen] = g
			out.Generations = append(out.Generations, g)
			lastGen = b.Gen
		}
		if !minUnitsSet[b.Gen] || b.TimeUnits < g.MinUnits {
			g.MinUnits = b.TimeUnits
			minUnitsSet[b.Gen] = true
		}
		g.Batches = append(g.Batches, b)
		if err := r.ingest(g, b); err != nil {
			return nil, err
		}
	}
	if len(out.Generations) == 0 {
		return nil, fmt.Errorf("%w: trace contains no batches", ErrCorrupt)
	}
	for _, g := range out.Generations {
		if g.Frequency == 0 {
			return nil, fmt.Errorf("%w: no frequency event found in generation %d", ErrCorrupt, g.Gen)
		}
		if r.v >= VersionGo125 && !g.HasClockSnapshot {
			return nil, fmt.Errorf("%w: no clock snapshot found in generation %d", ErrCorrupt, g.Gen)
		}
	}
	return out, nil
}

func (r *RawReader) readBatch() (*RawBatch, error) {
	typ, err := r.r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: reading batch header: %v", ErrCorrupt, err)
	}
	et := EventType(typ)
	if int(et) >= len(r.specs) || et == EvNone {
		return nil, fmt.Errorf("%w: invalid event type %d", ErrCorrupt, typ)
	}
	switch et {
	case EvExperimentalBatch:
		return nil, fmt.Errorf("%w: experimental batches are not supported", ErrUnsupported)
	case EvEventBatch:
		// Handled below.
	default:
		return nil, fmt.Errorf("%w: expected batch event, got %s", ErrCorrupt, r.specs[et].Name)
	}

	gen, err := r.uvarint()
	if err != nil {
		return nil, fmt.Errorf("%w: reading batch gen: %v", ErrCorrupt, err)
	}
	m, err := r.uvarint()
	if err != nil {
		return nil, fmt.Errorf("%w: reading batch M: %v", ErrCorrupt, err)
	}
	ts, err := r.uvarint()
	if err != nil {
		return nil, fmt.Errorf("%w: reading batch timestamp: %v", ErrCorrupt, err)
	}
	size, err := r.uvarint()
	if err != nil {
		return nil, fmt.Errorf("%w: reading batch size: %v", ErrCorrupt, err)
	}
	if size > MaxBatchSize {
		return nil, fmt.Errorf("%w: batch size %d exceeds maximum %d", ErrCorrupt, size, MaxBatchSize)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r.r, data); err != nil {
		return nil, fmt.Errorf("%w: reading batch data: %v", ErrCorrupt, err)
	}
	b := &RawBatch{Gen: gen, M: int64(m), TimeUnits: ts}
	if b.M < 0 && uint64(b.M) != ^uint64(0) {
		// Only the NoThread sentinel is expected as a negative-when-cast ID.
		return nil, fmt.Errorf("%w: invalid thread ID %d", ErrCorrupt, m)
	}
	if err := r.parseBatchData(b, data); err != nil {
		return nil, err
	}
	return b, nil
}

func (r *RawReader) parseBatchData(b *RawBatch, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: empty batch in generation %d", ErrCorrupt, b.Gen)
	}
	br := bytes.NewReader(data)
	marker, err := br.ReadByte()
	if err != nil {
		return fmt.Errorf("%w: missing batch marker", ErrCorrupt)
	}
	switch EventType(marker) {
	case EvStrings:
		b.Kind = BatchStrings
		return r.parseStrings(b, br)
	case EvStacks:
		b.Kind = BatchStacks
		return r.parseStacks(b, br)
	case EvCPUSamples:
		b.Kind = BatchCPUSamples
		return r.parseCPUSamples(b, br)
	case EvFrequency:
		if r.v >= VersionGo125 {
			return fmt.Errorf("%w: lone frequency batch is invalid for %s", ErrCorrupt, r.v)
		}
		b.Kind = BatchFrequency
		// Unlike the other structural batches, a frequency batch consists of
		// the bare Frequency event; there is no section marker. Rewind and
		// parse it as a regular event.
		br.Reset(data)
		typ, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: reading frequency event: %v", ErrCorrupt, err)
		}
		if EventType(typ) != EvFrequency {
			return fmt.Errorf("%w: expected Frequency event, got %d", ErrCorrupt, typ)
		}
		ev, err := r.readEventArgs(br, EvFrequency)
		if err != nil {
			return err
		}
		if br.Len() != 0 {
			return fmt.Errorf("%w: trailing data in frequency batch", ErrCorrupt)
		}
		b.Events = append(b.Events, ev)
		return nil
	case EvSync:
		if r.v < VersionGo125 {
			return fmt.Errorf("%w: sync batch is invalid for %s", ErrCorrupt, r.v)
		}
		b.Kind = BatchSync
		return r.parseSync(b, br)
	default:
		// Regular event batch: rewind and parse timed events.
		//
		// N.B. Go 1.23+ emits goroutine and proc status enumeration events
		// (GoStatus/GoStatusStack/ProcStatus) in batches whose thread is
		// NoThread. Such batches still contain ordinary timed events.
		br.Reset(data)
		b.Kind = BatchEvents
		return r.parseTimedEvents(b, br)
	}
}

func (r *RawReader) parseStrings(b *RawBatch, br *bytes.Reader) error {
	b.Events = append(b.Events, RawEvent{Type: EvStrings})
	for br.Len() != 0 {
		typ, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: reading string marker: %v", ErrCorrupt, err)
		}
		if EventType(typ) != EvString {
			return fmt.Errorf("%w: expected String event, got %d", ErrCorrupt, typ)
		}
		id, err := readUvarint(br)
		if err != nil {
			return fmt.Errorf("%w: reading string ID: %v", ErrCorrupt, err)
		}
		n, err := readUvarint(br)
		if err != nil {
			return fmt.Errorf("%w: reading string length: %v", ErrCorrupt, err)
		}
		if n > MaxEventTrailerDataSize {
			return fmt.Errorf("%w: string of %d bytes exceeds maximum %d", ErrCorrupt, n, MaxEventTrailerDataSize)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(br, buf); err != nil {
			return fmt.Errorf("%w: reading string data: %v", ErrCorrupt, err)
		}
		b.Events = append(b.Events, RawEvent{Type: EvString, Args: []uint64{id}, Data: buf})
	}
	return nil
}

func (r *RawReader) parseStacks(b *RawBatch, br *bytes.Reader) error {
	b.Events = append(b.Events, RawEvent{Type: EvStacks})
	for br.Len() != 0 {
		typ, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: reading stack marker: %v", ErrCorrupt, err)
		}
		if EventType(typ) != EvStack {
			return fmt.Errorf("%w: expected Stack event, got %d", ErrCorrupt, typ)
		}
		id, err := readUvarint(br)
		if err != nil {
			return fmt.Errorf("%w: reading stack ID: %v", ErrCorrupt, err)
		}
		nframes, err := readUvarint(br)
		if err != nil {
			return fmt.Errorf("%w: reading stack frame count: %v", ErrCorrupt, err)
		}
		if nframes > MaxFramesPerStack {
			return fmt.Errorf("%w: stack with %d frames exceeds maximum %d", ErrCorrupt, nframes, MaxFramesPerStack)
		}
		ev := RawEvent{Type: EvStack, Args: []uint64{id, nframes}}
		for i := uint64(0); i < nframes; i++ {
			pc, err := readUvarint(br)
			if err != nil {
				return fmt.Errorf("%w: reading frame PC: %v", ErrCorrupt, err)
			}
			funcID, err := readUvarint(br)
			if err != nil {
				return fmt.Errorf("%w: reading frame func ID: %v", ErrCorrupt, err)
			}
			fileID, err := readUvarint(br)
			if err != nil {
				return fmt.Errorf("%w: reading frame file ID: %v", ErrCorrupt, err)
			}
			line, err := readUvarint(br)
			if err != nil {
				return fmt.Errorf("%w: reading frame line: %v", ErrCorrupt, err)
			}
			ev.Args = append(ev.Args, pc, funcID, fileID, line)
		}
		b.Events = append(b.Events, ev)
	}
	return nil
}

func (r *RawReader) parseCPUSamples(b *RawBatch, br *bytes.Reader) error {
	b.Events = append(b.Events, RawEvent{Type: EvCPUSamples})
	for br.Len() != 0 {
		typ, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: reading CPU sample marker: %v", ErrCorrupt, err)
		}
		if EventType(typ) != EvCPUSample {
			return fmt.Errorf("%w: expected CPUSample event, got %d", ErrCorrupt, typ)
		}
		ev, err := r.readEventArgs(br, EventType(typ))
		if err != nil {
			return err
		}
		// CPU sample timestamps are absolute timer units.
		ev.AbsUnits = ev.Args[0]
		b.Events = append(b.Events, ev)
	}
	return nil
}

func (r *RawReader) parseSync(b *RawBatch, br *bytes.Reader) error {
	b.Events = append(b.Events, RawEvent{Type: EvSync})
	sawFreq := false
	for br.Len() != 0 {
		typ, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: reading sync event marker: %v", ErrCorrupt, err)
		}
		et := EventType(typ)
		ev, err := r.readEventArgs(br, et)
		if err != nil {
			return err
		}
		switch et {
		case EvFrequency:
			if sawFreq {
				return fmt.Errorf("%w: multiple frequency events in sync batch", ErrCorrupt)
			}
			sawFreq = true
		case EvClockSnapshot:
			// The snapshot's first argument is a delta from the batch base.
			ev.AbsUnits = b.TimeUnits + ev.Args[0]
		default:
			return fmt.Errorf("%w: unexpected event %s in sync batch", ErrCorrupt, r.specs[et].Name)
		}
		b.Events = append(b.Events, ev)
	}
	if !sawFreq {
		return fmt.Errorf("%w: sync batch without frequency event", ErrCorrupt)
	}
	return nil
}

func (r *RawReader) parseTimedEvents(b *RawBatch, br *bytes.Reader) error {
	cursor := b.TimeUnits
	for br.Len() != 0 {
		typ, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: reading timed event marker: %v", ErrCorrupt, err)
		}
		et := EventType(typ)
		spec, ok := r.specFor(et)
		if !ok {
			return fmt.Errorf("%w: event %d is not valid for %s", ErrCorrupt, et, r.v)
		}
		if !spec.IsTimedEvent {
			return fmt.Errorf("%w: event %s is not valid inside an event batch", ErrCorrupt, spec.Name)
		}
		ev, err := r.readEventArgs(br, et)
		if err != nil {
			return err
		}
		cursor += ev.Args[0]
		ev.AbsUnits = cursor
		b.Events = append(b.Events, ev)
	}
	if len(b.Events) == 0 {
		return fmt.Errorf("%w: empty event batch for M %d in gen %d", ErrCorrupt, b.M, b.Gen)
	}
	return nil
}

func (r *RawReader) specFor(et EventType) (EventSpec, bool) {
	if int(et) >= len(r.specs) {
		return EventSpec{}, false
	}
	s := r.specs[et]
	return s, s.Name != ""
}

func (r *RawReader) readEventArgs(br *bytes.Reader, et EventType) (RawEvent, error) {
	spec, ok := r.specFor(et)
	if !ok {
		return RawEvent{}, fmt.Errorf("%w: event %d is not valid for %s", ErrCorrupt, et, r.v)
	}
	ev := RawEvent{Type: et, Args: make([]uint64, 0, len(spec.Args))}
	for i := 0; i < len(spec.Args); i++ {
		v, err := readUvarint(br)
		if err != nil {
			return RawEvent{}, fmt.Errorf("%w: reading %s arg %d: %v", ErrCorrupt, spec.Name, i, err)
		}
		ev.Args = append(ev.Args, v)
	}
	if spec.IsStack {
		nframes := ev.Args[1]
		for i := uint64(0); i < nframes; i++ {
			for j := 0; j < 4; j++ {
				v, err := readUvarint(br)
				if err != nil {
					return RawEvent{}, fmt.Errorf("%w: reading stack frame data: %v", ErrCorrupt, err)
				}
				ev.Args = append(ev.Args, v)
			}
		}
	}
	if spec.HasData {
		n, err := readUvarint(br)
		if err != nil {
			return RawEvent{}, fmt.Errorf("%w: reading data length for %s: %v", ErrCorrupt, spec.Name, err)
		}
		if n > MaxEventTrailerDataSize {
			return RawEvent{}, fmt.Errorf("%w: data of %d bytes exceeds maximum for %s", ErrCorrupt, n, spec.Name)
		}
		ev.Data = make([]byte, n)
		if _, err := io.ReadFull(br, ev.Data); err != nil {
			return RawEvent{}, fmt.Errorf("%w: reading data for %s: %v", ErrCorrupt, spec.Name, err)
		}
	}
	return ev, nil
}

func (r *RawReader) ingest(g *RawGeneration, b *RawBatch) error {
	switch b.Kind {
	case BatchStrings:
		for _, ev := range b.Events[1:] {
			id := ev.Args[0]
			if _, exists := g.Strings[id]; exists {
				return fmt.Errorf("%w: duplicate string ID %d in gen %d", ErrCorrupt, id, g.Gen)
			}
			g.Strings[id] = string(ev.Data)
		}
	case BatchStacks:
		for _, ev := range b.Events[1:] {
			id := ev.Args[0]
			if _, exists := g.Stacks[id]; exists {
				return fmt.Errorf("%w: duplicate stack ID %d in gen %d", ErrCorrupt, id, g.Gen)
			}
			nframes := ev.Args[1]
			frames := make([]StackFrame, 0, nframes)
			for i := uint64(0); i < nframes; i++ {
				off := 2 + i*4
				frames = append(frames, StackFrame{
					PC:     ev.Args[off],
					FuncID: ev.Args[off+1],
					FileID: ev.Args[off+2],
					Line:   ev.Args[off+3],
				})
			}
			g.Stacks[id] = frames
		}
	case BatchCPUSamples:
		g.CPUSamples = append(g.CPUSamples, b.Events[1:]...)
	case BatchFrequency:
		ev := b.Events[0]
		g.Frequency = ev.Args[0]
		g.FrequencyBaseUnits = b.TimeUnits
	case BatchSync:
		g.SyncBaseUnits = b.TimeUnits
		for _, ev := range b.Events[1:] {
			switch ev.Type {
			case EvFrequency:
				g.Frequency = ev.Args[0]
				g.FrequencyBaseUnits = b.TimeUnits
			case EvClockSnapshot:
				g.HasClockSnapshot = true
				g.ClockSnapshot = ev
			}
		}
	}
	return nil
}

func (r *RawReader) uvarint() (uint64, error) {
	return binary.ReadUvarint(r.r)
}

func readUvarint(br *bytes.Reader) (uint64, error) {
	return binary.ReadUvarint(br)
}

// RawWriter emits trace v2 wire bytes.
//
// The writer does not reproduce the runtime's LEB128 padding; canonical
// uvarint encoding is accepted by all trace readers.
type RawWriter struct {
	w     io.Writer
	v     Version
	specs []EventSpec
	buf   []byte
}

// NewWriter writes the header and returns a raw writer for the version.
func NewWriter(w io.Writer, v Version) (*RawWriter, error) {
	if !v.Supported() {
		return nil, fmt.Errorf("%w: cannot write %s", ErrUnsupported, v)
	}
	rw := &RawWriter{w: w, v: v, specs: v.Specs()}
	if _, err := w.Write(v.Header()); err != nil {
		return nil, err
	}
	return rw, nil
}

// Version returns the writer's version.
func (w *RawWriter) Version() Version { return w.v }

// WriteBatch emits one batch including its framing.
func (w *RawWriter) WriteBatch(b *RawBatch) error {
	size := w.batchDataSize(b)
	if size > MaxBatchSize {
		return fmt.Errorf("batch of %d bytes exceeds maximum %d; split it before writing", size, MaxBatchSize)
	}
	w.buf = appendUvarint(w.buf[:0], uint64(EvEventBatch))
	w.buf = appendUvarint(w.buf, b.Gen)
	w.buf = appendUvarint(w.buf, uint64(b.M))
	w.buf = appendUvarint(w.buf, b.TimeUnits)
	w.buf = appendUvarint(w.buf, uint64(size))
	if _, err := w.w.Write(w.buf); err != nil {
		return err
	}
	for i := range b.Events {
		if err := w.writeEvent(&b.Events[i]); err != nil {
			return err
		}
	}
	return nil
}

func (w *RawWriter) batchDataSize(b *RawBatch) int {
	n := 0
	for i := range b.Events {
		n += w.eventSize(&b.Events[i])
	}
	return n
}

func (w *RawWriter) eventSize(ev *RawEvent) int {
	spec := w.specs[ev.Type]
	n := 1
	var buf [binary.MaxVarintLen64]byte
	for _, a := range ev.Args {
		n += binary.PutUvarint(buf[:], a)
	}
	if spec.HasData {
		n += binary.PutUvarint(buf[:], uint64(len(ev.Data)))
		n += len(ev.Data)
	}
	return n
}

func (w *RawWriter) writeEvent(ev *RawEvent) error {
	if int(ev.Type) >= len(w.specs) || w.specs[ev.Type].Name == "" {
		return fmt.Errorf("%w: event %d is not valid for %s", ErrUnsupported, ev.Type, w.v)
	}
	spec := w.specs[ev.Type]
	w.buf = append(w.buf[:0], byte(ev.Type))
	for _, a := range ev.Args {
		w.buf = appendUvarint(w.buf, a)
	}
	if spec.HasData {
		w.buf = appendUvarint(w.buf, uint64(len(ev.Data)))
	}
	if _, err := w.w.Write(w.buf); err != nil {
		return err
	}
	if spec.HasData {
		if _, err := w.w.Write(ev.Data); err != nil {
			return err
		}
	}
	return nil
}

func appendUvarint(buf []byte, x uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

// SortedThreadIDs returns the distinct M IDs of regular event batches in the
// generation in deterministic order.
func (g *RawGeneration) SortedThreadIDs() []int64 {
	seen := map[int64]struct{}{}
	var ids []int64
	for _, b := range g.Batches {
		if b.Kind != BatchEvents {
			continue
		}
		if _, ok := seen[b.M]; !ok {
			seen[b.M] = struct{}{}
			ids = append(ids, b.M)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TimedEvents returns the regular timed events of one M in wire order across
// all of the generation's batches.
func (g *RawGeneration) TimedEvents(m int64) []RawEvent {
	var out []RawEvent
	for _, b := range g.Batches {
		if b.Kind == BatchEvents && b.M == m {
			out = append(out, b.Events...)
		}
	}
	return out
}
