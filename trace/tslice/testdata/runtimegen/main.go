// Command runtimegen produces a small real Go execution trace for the causal
// slicer tests. It is built and executed by the test suite; the trace format
// version is whatever the building toolchain emits (Go 1.23/1.24 produce the
// go1.23 wire format).
package main

import (
	"context"
	"os"
	"runtime/trace"
	"sync"
	"time"
)

func main() {
	f, err := os.Create(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := trace.Start(f); err != nil {
		panic(err)
	}
	defer trace.Stop()

	ctx, task := trace.NewTask(context.Background(), "root-task")
	var wg sync.WaitGroup
	ch := make(chan int, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			trace.WithRegion(ctx, "worker-region", func() {
				trace.Log(ctx, "cat", "worker log")
				ch <- i
				time.Sleep(2 * time.Millisecond)
			})
		}(i)
	}
	for i := 0; i < 4; i++ {
		<-ch
	}
	task.End()
	wg.Wait()
}
