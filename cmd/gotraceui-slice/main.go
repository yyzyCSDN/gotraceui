// Command gotraceui-slice exports a causal slice of a Go execution trace.
//
// A causal slice contains the selected goroutines' events inside a half-open
// time window plus every event required to reconstruct the same native
// scheduler semantics, and nothing else: independent events and unreferenced
// string and stack definitions are omitted. The output is a valid, self
// contained trace in the same non-experimental v2 format (Go 1.22 through
// Go 1.25) as the input, accompanied by a machine-readable JSON manifest.
//
// Usage:
//
//	gotraceui-slice -goroutines 12,13 -window 1000:2000 -o slice.trace -manifest slice.json input.trace
//
// Timestamps in -window are nanoseconds on the parsed trace timeline (the
// same coordinates reported by gotraceui and go tool trace). The window is
// half-open: the start is inclusive, the end exclusive.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"honnef.co/go/gotraceui/trace/tslice"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gotraceui-slice:", err)
		if errors.Is(err, tslice.ErrInvalidSelection) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("gotraceui-slice", flag.ContinueOnError)
	var (
		gFlag    gorisFlag
		window   = fs.String("window", "", "half-open time window in nanoseconds, formatted as a:b (a inclusive, b exclusive)")
		out      = fs.String("o", "", "output path for the sliced trace binary (required)")
		manifest = fs.String("manifest", "", "output path for the machine-readable JSON manifest (required)")
	)
	fs.Var(&gFlag, "goroutines", "comma-separated goroutine IDs to retain; may be repeated")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: gotraceui-slice -goroutines <ids> -window <a:b> -o <trace> -manifest <json> <input trace>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("exactly one input trace file is required")
	}
	if *out == "" || *manifest == "" {
		return errors.New("both -o and -manifest are required")
	}
	if len(gFlag.ids) == 0 {
		return errors.New("-goroutines must contain at least one ID")
	}
	a, b, err := parseWindow(*window)
	if err != nil {
		return err
	}

	in, err := os.Open(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("opening input trace: %w", err)
	}
	res, err := tslice.Export(in, tslice.Selection{Goroutines: gFlag.ids, WindowA: a, WindowB: b})
	in.Close()
	if err != nil {
		return err
	}
	res.Manifest.Source.Path = fs.Arg(0)

	if err := writeAtomic(*out, res.Trace); err != nil {
		return fmt.Errorf("writing slice trace: %w", err)
	}
	mjson, err := json.MarshalIndent(res.Manifest, "", "  ")
	if err != nil {
		rmQuiet(*out)
		return fmt.Errorf("encoding manifest: %w", err)
	}
	mjson = append(mjson, '\n')
	if err := writeAtomic(*manifest, mjson); err != nil {
		rmQuiet(*out)
		return fmt.Errorf("writing manifest: %w", err)
	}

	selected, support := countRoles(res.Manifest)
	fmt.Printf("wrote %s (%d bytes) and %s (%d bytes): %d selected events, %d support events, %d generations\n",
		*out, len(res.Trace), *manifest, len(mjson), selected, support, len(res.Manifest.Output.Generations))
	return nil
}

func parseWindow(s string) (int64, int64, error) {
	if s == "" {
		return 0, 0, errors.New("-window is required, formatted a:b")
	}
	partA, partB, ok := strings.Cut(s, ":")
	if !ok {
		return 0, 0, fmt.Errorf("-window %q must be formatted as a:b", s)
	}
	a, err := strconv.ParseInt(strings.TrimSpace(partA), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid window start %q: %w", partA, err)
	}
	b, err := strconv.ParseInt(strings.TrimSpace(partB), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid window end %q: %w", partB, err)
	}
	if a < 0 || b < 0 || a >= b {
		return 0, 0, fmt.Errorf("window [%d,%d) must be a non-empty half-open interval of non-negative ns", a, b)
	}
	return a, b, nil
}

// gorisFlag is a repeatable, comma-separated list of goroutine IDs.
type gorisFlag struct {
	ids []int64
}

func (f *gorisFlag) String() string {
	parts := make([]string, len(f.ids))
	for i, id := range f.ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	return strings.Join(parts, ",")
}

func (f *gorisFlag) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid goroutine ID %q: %w", part, err)
		}
		if id <= 0 {
			return fmt.Errorf("goroutine IDs must be positive, got %d", id)
		}
		f.ids = append(f.ids, id)
	}
	return nil
}

// writeAtomic writes data to path via a temp file in the same directory and
// renames it into place, so failures never leave a successful-looking output.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".gotraceui-slice-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func rmQuiet(path string) {
	_ = os.Remove(path)
}

func countRoles(m tslice.Manifest) (selected, support int) {
	for _, ev := range m.Events {
		switch ev.Role {
		case "selected":
			selected++
		case "support":
			support++
		}
	}
	return
}
