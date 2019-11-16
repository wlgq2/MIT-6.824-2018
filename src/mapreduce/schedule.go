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

	//设置等待任务数量
	var wg sync.WaitGroup
	wg.Add(ntasks)
	//运行协程并行执行任务
	for i := 0; i < ntasks; i++ {
		go func(index int) {
			//rpc调用参数
			var args DoTaskArgs
			args.Phase = phase
			args.JobName = jobName
			args.NumOtherPhase = n_other
			args.TaskNumber = index
			if phase == mapPhase {
				args.File = mapFiles[index]
			}

			done := false
			for ;!done;{
				worker := <-registerChan
				done = call(worker, "Worker.DoTask", args, nil)
				go func(){registerChan <- worker  }()
			}
					
			//任务完成
			wg.Done()
		}(i)
	}
	//等待所有任务完成
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
