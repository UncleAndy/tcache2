package map_tours_db_worker

import (
	"time"
	"github.com/uncleandy/tcache2/apps/data2db/db_worker_base"
	"sync"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/cache"
	"fmt"
)

var (
	InToursCounter = int64(0)
	LastToursCounter = int64(0)
	InToursCounterMutex = sync.Mutex{}
	LastStatTime time.Time
)

func (worker *MapToursDbWorker) RunStatisticLoop() {
	LastStatTime = time.Now()

	ticker := time.NewTicker(10 * time.Second)
	go func(){
		for !db_worker_base.ForceStopThreads {
			select {
			case <-ticker.C:
				worker.StatisticsOutput()
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (worker *MapToursDbWorker) StatisticsOutput() {
	delta := time.Since(LastStatTime)
	delta_tours := InToursCounter - LastToursCounter
	speed := float64(delta_tours) / delta.Seconds()
	LastStatTime = time.Now()
	LastToursCounter = InToursCounter

	// Queues size
	size_ins := int64(0)
	size_upd := int64(0)
	size_del := int64(0)
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		queue_ins := fmt.Sprintf(MapTourInsertThreadQueueTemplate, (worker.Settings.WorkerFirstThreadId + i))
		cache.QueueSizesUpdateAll(queue_ins)
		size_ins += cache.QueueSize(queue_ins)

		queue_upd := fmt.Sprintf(MapTourUpdateThreadQueueTemplate, (worker.Settings.WorkerFirstThreadId + i))
		cache.QueueSizesUpdateAll(queue_upd)
		size_upd += cache.QueueSize(queue_upd)

		queue_del := fmt.Sprintf(MapTourDeleteThreadQueueTemplate, (worker.Settings.WorkerFirstThreadId + i))
		cache.QueueSizesUpdateAll(queue_del)
		size_del += cache.QueueSize(queue_del)
	}

	log.Info.Printf(
		"STAT: Map tours db workers progress: %d/%d (i:%d, u:%d, :d:%d) (%.0f t/s)\n",
		InToursCounter,
		(size_ins + size_upd + size_del),
		size_ins,
		size_upd,
		size_del,
		speed,
	)
}
