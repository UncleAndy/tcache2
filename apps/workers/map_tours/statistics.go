package map_tours

import (
	"time"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/log"
	"fmt"
)

func (worker *MapToursWorker) RunStatisticLoop() {
	ticker := time.NewTicker(10 * time.Second)
	go func(){
		for !ForceStopThreads {
			select {
			case <-ticker.C:
				worker.StatisticsOutput()
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (worker *MapToursWorker) StatisticsOutput() {
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		workerQueueToursName := fmt.Sprintf(ThreadMapToursQueueTemplate, i)

		cache.QueueSizesUpdateAll(workerQueueToursName)
		queue_length := cache.QueueSize(workerQueueToursName)

		log.Info.Printf("STAT: Queue size for map thread %d = %d\n", i, queue_length)
	}

}
