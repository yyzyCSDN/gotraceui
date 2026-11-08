// Package tracev2 implements reading and writing of the Go execution tracer's
// version 2 wire format (Go 1.22 through Go 1.25, non-experimental).
//
// The event type constants and the event specification table are transcribed
// from golang.org/x/exp/trace/internal/tracev2 (BSD-style license, The Go
// Authors); see LICENSE-THIRD-PARTY. Only the parts of the format that the
// causal slicer needs are reproduced.
package tracev2

import "strconv"

// Version is the trace wire format version (the minor number in the
// "go 1.XX trace" header).
type Version uint32

const (
	// VersionGo122 is the wire format emitted by Go 1.22.
	VersionGo122 Version = 22
	// VersionGo123 is the wire format emitted by Go 1.23 and Go 1.24.
	VersionGo123 Version = 23
	// VersionGo125 is the wire format emitted by Go 1.25.
	VersionGo125 Version = 25
)

// Supported reports whether the causal slicer supports the given version.
// Only non-experimental v2 traces produced by Go 1.22 through Go 1.25 are
// supported.
func (v Version) Supported() bool {
	switch v {
	case VersionGo122, VersionGo123, VersionGo125:
		return true
	default:
		return false
	}
}

// String renders the version as it appears in the trace header, e.g. "go1.22".
func (v Version) String() string {
	return "go1." + strconv.Itoa(int(v))
}

// Header renders the 16-byte trace header for the version. The wire format
// uses the fixed layout "go 1.XX trace\x00\x00\x00".
func (v Version) Header() []byte {
	return []byte{'g', 'o', ' ', '1', '.', digit(v / 10), digit(v % 10), ' ', 't', 'r', 'a', 'c', 'e', 0, 0, 0}
}

func digit(d Version) byte {
	return byte('0' + d)
}

// EventType identifies a wire format event. The numeric values match the
// runtime's tracev2 event numbering.
type EventType uint8

const (
	EvNone EventType = iota // unused

	// Structural events.
	EvEventBatch
	EvStacks
	EvStack
	EvStrings
	EvString
	EvCPUSamples
	EvCPUSample
	EvFrequency

	// Procs.
	EvProcsChange
	EvProcStart
	EvProcStop
	EvProcSteal
	EvProcStatus

	// Goroutines.
	EvGoCreate
	EvGoCreateSyscall
	EvGoStart
	EvGoDestroy
	EvGoDestroySyscall
	EvGoStop
	EvGoBlock
	EvGoUnblock
	EvGoSyscallBegin
	EvGoSyscallEnd
	EvGoSyscallEndBlocked
	EvGoStatus

	// STW.
	EvSTWBegin
	EvSTWEnd

	// GC events.
	EvGCActive
	EvGCBegin
	EvGCEnd
	EvGCSweepActive
	EvGCSweepBegin
	EvGCSweepEnd
	EvGCMarkAssistActive
	EvGCMarkAssistBegin
	EvGCMarkAssistEnd
	EvHeapAlloc
	EvHeapGoal

	// Annotations.
	EvGoLabel
	EvUserTaskBegin
	EvUserTaskEnd
	EvUserRegionBegin
	EvUserRegionEnd
	EvUserLog

	// Coroutines. Added in Go 1.23.
	EvGoSwitch
	EvGoSwitchDestroy
	EvGoCreateBlocked

	// GoStatus with stack. Added in Go 1.23.
	EvGoStatusStack

	// Batch event for an experimental batch with a custom format. Added in Go 1.23.
	EvExperimentalBatch

	// Sync batch. Added in Go 1.25.
	EvSync
	EvClockSnapshot
)

// MaxBatchSize is the maximum allowed size of an event batch, matching the
// runtime's tracev2 limit.
const MaxBatchSize = 64 << 10

// MaxFramesPerStack is the maximum number of frames a stack definition may
// contain, matching the runtime's tracev2 limit.
const MaxFramesPerStack = 128

// MaxEventTrailerDataSize is the maximum size of a string event's data.
const MaxEventTrailerDataSize = 1 << 10

// EventSpec describes the wire layout of an event type.
type EventSpec struct {
	// Name is the canonical name of the event.
	Name string
	// Args are the argument names. Their length determines the number of
	// uvarint arguments the event carries on the wire.
	Args []string
	// StringIDs lists the argument indices that reference the string table.
	StringIDs []int
	// StackIDs lists the argument indices that reference the stack table.
	StackIDs []int
	// StartEv is the event that opens the range this event closes, or 0.
	StartEv EventType
	// IsTimedEvent is true for events appearing in M batches whose first
	// argument is a timestamp delta.
	IsTimedEvent bool
	// HasData is true for events with a byte trailer prefixed by its length.
	HasData bool
	// IsStack is true for stack table entries whose trailing arguments
	// describe individual stack frames.
	IsStack bool
}

// Arity returns the number of wire arguments of the event.
func (s EventSpec) Arity() int { return len(s.Args) }

// specs is the full event specification table, transcribed from
// tracev2.Specs. Version-specific tables are derived by slicing, exactly as
// the upstream library does.
var specs = [...]EventSpec{
	// "Structural" events.
	EvEventBatch: {Name: "EventBatch", Args: []string{"gen", "m", "time", "size"}},
	EvStacks:     {Name: "Stacks"},
	EvStack:      {Name: "Stack", Args: []string{"id", "nframes"}, IsStack: true},
	EvStrings:    {Name: "Strings"},
	EvString:     {Name: "String", Args: []string{"id"}, HasData: true},
	EvCPUSamples: {Name: "CPUSamples"},
	EvCPUSample:  {Name: "CPUSample", Args: []string{"time", "m", "p", "g", "stack"}, StackIDs: []int{4}},
	EvFrequency:  {Name: "Frequency", Args: []string{"freq"}},
	EvExperimentalBatch: {
		Name:    "ExperimentalBatch",
		Args:    []string{"exp", "gen", "m", "time"},
		HasData: true,
	},
	EvSync: {Name: "Sync"},

	// "Timed" events.
	EvProcsChange: {
		Name: "ProcsChange", Args: []string{"dt", "procs_value", "stack"},
		IsTimedEvent: true, StackIDs: []int{2},
	},
	EvProcStart: {
		Name: "ProcStart", Args: []string{"dt", "p", "p_seq"}, IsTimedEvent: true,
	},
	EvProcStop: {
		Name: "ProcStop", Args: []string{"dt"}, IsTimedEvent: true,
	},
	EvProcSteal: {
		Name: "ProcSteal", Args: []string{"dt", "p", "p_seq", "m"}, IsTimedEvent: true,
	},
	EvProcStatus: {
		Name: "ProcStatus", Args: []string{"dt", "p", "pstatus"}, IsTimedEvent: true,
	},
	EvGoCreate: {
		Name: "GoCreate", Args: []string{"dt", "new_g", "new_stack", "stack"},
		IsTimedEvent: true, StackIDs: []int{3, 2},
	},
	EvGoCreateSyscall: {
		Name: "GoCreateSyscall", Args: []string{"dt", "new_g"}, IsTimedEvent: true,
	},
	EvGoStart: {
		Name: "GoStart", Args: []string{"dt", "g", "g_seq"}, IsTimedEvent: true,
	},
	EvGoDestroy: {
		Name: "EvGoDestroy", Args: []string{"dt"}, IsTimedEvent: true,
	},
	EvGoDestroySyscall: {
		Name: "GoDestroySyscall", Args: []string{"dt"}, IsTimedEvent: true,
	},
	EvGoStop: {
		Name: "GoStop", Args: []string{"dt", "reason_string", "stack"},
		IsTimedEvent: true, StackIDs: []int{2}, StringIDs: []int{1},
	},
	EvGoBlock: {
		Name: "GoBlock", Args: []string{"dt", "reason_string", "stack"},
		IsTimedEvent: true, StackIDs: []int{2}, StringIDs: []int{1},
	},
	EvGoUnblock: {
		Name: "GoUnblock", Args: []string{"dt", "g", "g_seq", "stack"},
		IsTimedEvent: true, StackIDs: []int{3},
	},
	EvGoSyscallBegin: {
		Name: "GoSyscallBegin", Args: []string{"dt", "p_seq", "stack"},
		IsTimedEvent: true, StackIDs: []int{2},
	},
	EvGoSyscallEnd: {
		Name: "GoSyscallEnd", Args: []string{"dt"},
		StartEv: EvGoSyscallBegin, IsTimedEvent: true,
	},
	EvGoSyscallEndBlocked: {
		Name: "GoSyscallEndBlocked", Args: []string{"dt"},
		StartEv: EvGoSyscallBegin, IsTimedEvent: true,
	},
	EvGoStatus: {
		Name: "GoStatus", Args: []string{"dt", "g", "m", "gstatus"}, IsTimedEvent: true,
	},
	EvSTWBegin: {
		Name: "STWBegin", Args: []string{"dt", "kind_string", "stack"},
		IsTimedEvent: true, StackIDs: []int{2}, StringIDs: []int{1},
	},
	EvSTWEnd: {
		Name: "STWEnd", Args: []string{"dt"},
		StartEv: EvSTWBegin, IsTimedEvent: true,
	},
	EvGCActive: {
		Name: "GCActive", Args: []string{"dt", "gc_seq"},
		StartEv: EvGCBegin, IsTimedEvent: true,
	},
	EvGCBegin: {
		Name: "GCBegin", Args: []string{"dt", "gc_seq", "stack"},
		IsTimedEvent: true, StackIDs: []int{2},
	},
	EvGCEnd: {
		Name: "GCEnd", Args: []string{"dt", "gc_seq"},
		StartEv: EvGCBegin, IsTimedEvent: true,
	},
	EvGCSweepActive: {
		Name: "GCSweepActive", Args: []string{"dt", "p"},
		StartEv: EvGCSweepBegin, IsTimedEvent: true,
	},
	EvGCSweepBegin: {
		Name: "GCSweepBegin", Args: []string{"dt", "stack"},
		IsTimedEvent: true, StackIDs: []int{1},
	},
	EvGCSweepEnd: {
		Name: "GCSweepEnd", Args: []string{"dt", "swept_value", "reclaimed_value"},
		StartEv: EvGCSweepBegin, IsTimedEvent: true,
	},
	EvGCMarkAssistActive: {
		Name: "GCMarkAssistActive", Args: []string{"dt", "g"},
		StartEv: EvGCMarkAssistBegin, IsTimedEvent: true,
	},
	EvGCMarkAssistBegin: {
		Name: "GCMarkAssistBegin", Args: []string{"dt", "stack"},
		IsTimedEvent: true, StackIDs: []int{1},
	},
	EvGCMarkAssistEnd: {
		Name: "GCMarkAssistEnd", Args: []string{"dt"},
		StartEv: EvGCMarkAssistBegin, IsTimedEvent: true,
	},
	EvHeapAlloc: {
		Name: "HeapAlloc", Args: []string{"dt", "heapalloc_value"}, IsTimedEvent: true,
	},
	EvHeapGoal: {
		Name: "HeapGoal", Args: []string{"dt", "heapgoal_value"}, IsTimedEvent: true,
	},
	EvGoLabel: {
		Name: "GoLabel", Args: []string{"dt", "label_string"},
		IsTimedEvent: true, StringIDs: []int{1},
	},
	EvUserTaskBegin: {
		Name: "UserTaskBegin", Args: []string{"dt", "task", "parent_task", "name_string", "stack"},
		IsTimedEvent: true, StackIDs: []int{4}, StringIDs: []int{3},
	},
	EvUserTaskEnd: {
		Name: "UserTaskEnd", Args: []string{"dt", "task", "stack"},
		IsTimedEvent: true, StackIDs: []int{2},
	},
	EvUserRegionBegin: {
		Name: "UserRegionBegin", Args: []string{"dt", "task", "name_string", "stack"},
		IsTimedEvent: true, StackIDs: []int{3}, StringIDs: []int{2},
	},
	EvUserRegionEnd: {
		Name: "UserRegionEnd", Args: []string{"dt", "task", "name_string", "stack"},
		StartEv: EvUserRegionBegin, IsTimedEvent: true,
		StackIDs: []int{3}, StringIDs: []int{2},
	},
	EvUserLog: {
		Name: "UserLog", Args: []string{"dt", "task", "key_string", "value_string", "stack"},
		IsTimedEvent: true, StackIDs: []int{4}, StringIDs: []int{2, 3},
	},
	EvGoSwitch: {
		Name: "GoSwitch", Args: []string{"dt", "g", "g_seq"}, IsTimedEvent: true,
	},
	EvGoSwitchDestroy: {
		Name: "GoSwitchDestroy", Args: []string{"dt", "g", "g_seq"}, IsTimedEvent: true,
	},
	EvGoCreateBlocked: {
		Name: "GoCreateBlocked", Args: []string{"dt", "new_g", "new_stack", "stack"},
		IsTimedEvent: true, StackIDs: []int{3, 2},
	},
	EvGoStatusStack: {
		Name: "GoStatusStack", Args: []string{"dt", "g", "m", "gstatus", "stack"},
		IsTimedEvent: true, StackIDs: []int{4},
	},
	EvClockSnapshot: {
		Name: "ClockSnapshot", Args: []string{"dt", "mono", "sec", "nsec"}, IsTimedEvent: true,
	},
}

// Specs returns the event specifications valid for the version. It returns
// nil for unsupported versions.
func (v Version) Specs() []EventSpec {
	switch v {
	case VersionGo122:
		return specs[:EvUserLog+1]
	case VersionGo123:
		return specs[:EvExperimentalBatch+1]
	case VersionGo125:
		return specs[:EvClockSnapshot+1]
	default:
		return nil
	}
}
