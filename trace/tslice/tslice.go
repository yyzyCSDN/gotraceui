// Package tslice implements causal slicing of Go execution traces.
//
// A causal slice is a self-contained, valid trace binary that retains a user
// selected set of goroutines and a half-open time window, plus every event
// required to reconstruct the same native scheduler semantics for the
// selected events: goroutine creation and wake-up, G/P/M state and sequence
// predecessors, syscalls, GC state, recorded tasks and nested region starts,
// and the string and stack definitions they reference. Independent events and
// unreferenced definitions are omitted.
//
// Only non-experimental version 2 traces produced by Go 1.22 through Go 1.25
// are accepted.
package tslice

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	exptrace "golang.org/x/exp/trace"

	"honnef.co/go/gotraceui/trace/tracev2"
)

// Selection describes the requested slice.
type Selection struct {
	// Goroutines is the non-empty set of goroutine IDs the user is interested
	// in. Duplicates are ignored.
	Goroutines []int64
	// WindowA is the inclusive start of the half-open window in the parsed
	// trace's nanosecond timeline.
	WindowA int64
	// WindowB is the exclusive end of the half-open window in the parsed
	// trace's nanosecond timeline.
	WindowB int64
}

// Error classes returned by Export. Callers may use errors.Is to distinguish
// usage failures from malformed or unsupported input.
var (
	// ErrInvalidSelection means the selection itself is illegal, independent of
	// trace contents (empty goroutine set, inverted window, unknown goroutine,
	// empty result, and so on).
	ErrInvalidSelection = errors.New("invalid slice selection")
	// ErrUnsupported mirrors tracev2.ErrUnsupported.
	ErrUnsupported = tracev2.ErrUnsupported
	// ErrCorrupt mirrors tracev2.ErrCorrupt.
	ErrCorrupt = tracev2.ErrCorrupt
)

// Result is the produced slice.
type Result struct {
	// Trace is the re-encoded, valid Go trace binary.
	Trace []byte
	// Manifest is the machine-readable description of the slice.
	Manifest Manifest
}

// Export computes the causal slice of the trace read from r according to sel.
//
// On failure no result is returned; callers writing artifacts to disk must do
// so atomically themselves (the CLI does) so that errors never leave
// successful-looking output behind.
func Export(r io.Reader, sel Selection) (*Result, error) {
	if err := validateSelectionShape(sel); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading trace: %w", err)
	}

	raw, err := parseRaw(data)
	if err != nil {
		return nil, err
	}

	eng, err := newEngine(data, raw)
	if err != nil {
		return nil, err
	}
	if err := eng.validateSelection(sel); err != nil {
		return nil, err
	}
	if err := eng.computeClosure(sel); err != nil {
		return nil, err
	}
	out, err := eng.encode()
	if err != nil {
		return nil, err
	}
	if err := eng.verify(out, sel); err != nil {
		return nil, err
	}
	manifest := eng.buildManifest(out, sel)
	return &Result{Trace: out, Manifest: manifest}, nil
}

func validateSelectionShape(sel Selection) error {
	if len(sel.Goroutines) == 0 {
		return fmt.Errorf("%w: at least one goroutine ID is required", ErrInvalidSelection)
	}
	seen := map[int64]struct{}{}
	for _, g := range sel.Goroutines {
		if g <= 0 {
			return fmt.Errorf("%w: goroutine IDs must be positive, got %d", ErrInvalidSelection, g)
		}
		seen[g] = struct{}{}
	}
	if sel.WindowA < 0 || sel.WindowB < 0 {
		return fmt.Errorf("%w: window timestamps must be non-negative", ErrInvalidSelection)
	}
	if sel.WindowA >= sel.WindowB {
		return fmt.Errorf("%w: window [%d,%d) is empty or inverted", ErrInvalidSelection, sel.WindowA, sel.WindowB)
	}
	return nil
}

func parseRaw(data []byte) (*tracev2.RawTrace, error) {
	rr, err := tracev2.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	raw, err := rr.ReadAll()
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// ns converts absolute trace timer units to nanoseconds using the generation
// frequency. It replicates the upstream reader's floating-point conversion so
// that its output matches exptrace.Event.Time exactly.
func unitsToNS(units uint64, freq uint64) int64 {
	return int64(float64(units) * (1e9 / float64(freq)))
}

// eventTime is a small helper for nil-safe time access.
func eventTime(ev exptrace.Event) int64 {
	return int64(ev.Time())
}
