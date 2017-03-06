package map_tours

import (
	"time"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/log"
	"fmt"
	"strconv"
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

	sizes := ""
	sep := ""
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		workerQueueToursName := fmt.Sprintf(ThreadMapToursQueueTemplate, i)

		cache.QueueSizesUpdateAll(workerQueueToursName)
		queue_length := cache.QueueSize(workerQueueToursName)

		sizes = sizes + sep + strconv.FormatInt(queue_length, 10)
		sep = ", "
	}
	log.Info.Printf("STAT: Queue sizes for map tours thread: (%s)\n", sizes)
}
