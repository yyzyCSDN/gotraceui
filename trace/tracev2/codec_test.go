package tracev2

import (
	"bytes"
	"errors"
	"io"
	"testing"

	exptrace "golang.org/x/exp/trace"
)

func TestHeaderValidation(t *testing.T) {
	cases := []struct {
		name string
		hdr  []byte
		want error
	}{
		{"v22", VersionGo122.Header(), nil},
		{"v23", VersionGo123.Header(), nil},
		{"v25", VersionGo125.Header(), nil},
		{"v1", []byte("go 1.21 trace\x00\x00\x00"), ErrUnsupported},
		{"v26", []byte("go 1.26 trace\x00\x00\x00"), ErrUnsupported},
		{"garbage", bytes.Repeat([]byte{0xff}, 16), ErrCorrupt},
		{"short", []byte("go 1.23"), ErrCorrupt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(tc.hdr))
			if tc.want == nil {
				if err != nil || r == nil {
					t.Fatalf("got %v, want success", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestExperimentalBatchRejected(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, VersionGo123)
	if err != nil {
		t.Fatal(err)
	}
	// EvExperimentalBatch header: type, exp, gen, m, time, size=1, one payload.
	payload := []byte{
		byte(EvExperimentalBatch),
		1, // experiment id
		1, 0, 0, 0, // gen, m, time
		1, // size
		0, // payload
	}
	if _, err := buf.Write(payload); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAll(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
}

// TestMinimalRoundTrip builds the smallest useful trace with the writer and
// confirms the x/exp parser accepts it.
func TestMinimalRoundTrip(t *testing.T) {
	for _, v := range []Version{VersionGo122, VersionGo123, VersionGo125} {
		t.Run(v.String(), func(t *testing.T) {
			var out bytes.Buffer
			w, err := NewWriter(&out, v)
			if err != nil {
				t.Fatal(err)
			}
			// Sync / frequency.
			if v >= VersionGo125 {
				sync := []RawEvent{
					{Type: EvSync},
					{Type: EvFrequency, Args: []uint64{1e9}},
					{Type: EvClockSnapshot, Args: []uint64{1, 42, 1700000000, 1}, AbsUnits: 51},
				}
				if err := w.WriteBatch(&RawBatch{Gen: 1, M: NoThread, TimeUnits: 50, Events: sync}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := w.WriteBatch(&RawBatch{
					Gen: 1, M: NoThread, TimeUnits: 50,
					Events: []RawEvent{{Type: EvFrequency, Args: []uint64{1e9}}},
				}); err != nil {
					t.Fatal(err)
				}
			}
			// Enumerate p0 and g1 as running, then emit a block.
			evs := []RawEvent{
				{Type: EvProcStatus, Args: []uint64{1, 0, 1}, AbsUnits: 101},
				{Type: EvGoStatus, Args: []uint64{2, 1, 0, 2}, AbsUnits: 102},
				{Type: EvGoBlock, Args: []uint64{8, 0, 0}, AbsUnits: 110},
			}
			if err := w.WriteBatch(&RawBatch{Gen: 1, M: 0, TimeUnits: 100, Events: evs}); err != nil {
				t.Fatal(err)
			}
			// Empty definitions sections are legal; emit a string and a stack
			// to exercise the structural writers.
			if err := w.WriteBatch(&RawBatch{
				Gen: 1, M: NoThread, TimeUnits: 90,
				Events: []RawEvent{
					{Type: EvStrings},
					{Type: EvString, Args: []uint64{1}, Data: []byte("reason")},
				},
			}); err != nil {
				t.Fatal(err)
			}

			// Raw re-parse.
			rr, err := NewReader(bytes.NewReader(out.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := rr.ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			if raw.Generations[0].Frequency != 1e9 {
				t.Fatalf("freq = %d", raw.Generations[0].Frequency)
			}
			if v >= VersionGo125 && !raw.Generations[0].HasClockSnapshot {
				t.Fatalf("missing clock snapshot")
			}
			// Interpreted parse via the upstream reader.
			pr, err := exptrace.NewReader(bytes.NewReader(out.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			var n int
			for {
				_, err := pr.ReadEvent()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("upstream parse failed: %v", err)
				}
				n++
			}
			if n < 4 {
				t.Fatalf("too few events: %d", n)
			}
		})
	}
}

func TestBatchSizeLimitEnforced(t *testing.T) {
	var buf bytes.Buffer
	w, _ := NewWriter(&buf, VersionGo123)
	big := make([]RawEvent, MaxBatchSize)
	for i := range big {
		big[i] = RawEvent{Type: EvHeapAlloc, Args: []uint64{1, 1}}
	}
	err := w.WriteBatch(&RawBatch{Gen: 1, M: 0, TimeUnits: 0, Events: big})
	if err == nil {
		t.Fatalf("expected oversize batch error")
	}
}
