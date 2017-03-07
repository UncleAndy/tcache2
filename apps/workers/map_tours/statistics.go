package map_tours

import (
	"time"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/log"
	"fmt"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
)

const (
	MaxQueueSize = 100000
	WaitIncomeToursFlagName = "wait_income_map_tours_flag"
)

var (
	InToursCounter = int64(0)
	LastStatTime time.Time
)

func (worker *MapToursWorker) RunStatisticLoop() {
	LastStatTime = time.Now()

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
	delta := time.Since(LastStatTime)
	speed := float64(InToursCounter) / delta.Seconds()
	InToursCounter = 0
	LastStatTime = time.Now()

	log.Info.Printf("STAT: Map tours workers current speed = %.0f t/s\n", speed)

	queue_size := int64(0)
	zero_count := 0
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		workerQueueToursName := fmt.Sprintf(ThreadMapToursQueueTemplate, i)

		cache.QueueSizesUpdateAll(workerQueueToursName)
		queue_length := cache.QueueSize(workerQueueToursName)

		queue_size += queue_length

		if queue_length == 0 {
			zero_count++
		}
	}
	log.Info.Printf(
		"STAT: Queue sizes for map tours thread: %d (z:%d/%d)\n",
		queue_size,
		zero_count,
		worker.Settings.WorkerThreadsCount,
	)

	if queue_size > MaxQueueSize {
		worker_base.SetWaitIncomeToursFlag(WaitIncomeToursFlagName)
	} else {
		worker_base.CleanWaitIncomeToursFlag(WaitIncomeToursFlagName)
	}
}
