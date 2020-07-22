package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {
		go func(index int) {
			var args DoTaskArgs
			args.JobName = jobName
			if phase == mapPhase {
				args.File = mapFiles[index]
			}
			args.Phase = phase
			args.TaskNumber = index
			args.NumOtherPhase = n_other

			done := false
			for !done {
				worker := <-registerChan
				done = call(worker, "Worker.DoTask", args, nil)
				// There are two cases
				// done == true, then this worker can be add to the worker queue
				// done == false, then this worker may have some problem now, we
				// add back to the queue, and retry it later, and hope that the
				// issue, e.g. network... get fixed
				go func() { registerChan <- worker }()
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
