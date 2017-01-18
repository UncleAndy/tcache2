package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
)

func init_workers() {
	worker_base.Workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}

	map_tours_settings := worker_base.Workers[0].GetSettings()
	map_tours_settings.AllThreadsCount = 3
	map_tours_settings.WorkerFirstThreadId = 0
	map_tours_settings.WorkerThreadsCount = 3

	partners_tours_settings := worker_base.Workers[1].GetSettings()
	partners_tours_settings.AllThreadsCount = 2
	partners_tours_settings.WorkerFirstThreadId = 0
	partners_tours_settings.WorkerThreadsCount = 2
}

func TestWorkerManagerLoop(t *testing.T) {
	init_test_redis_single()

	cache.AddQueue(sletat.LoaderQueueToursName, "1")
	cache.AddQueue(sletat.LoaderQueueToursName, "2")
	cache.AddQueue(sletat.LoaderQueueToursName, "3")
	cache.AddQueue(sletat.LoaderQueueToursName, "4")
	cache.AddQueue(sletat.LoaderQueueToursName, "5")

	// worker_base.ManagerLoop()




	cache.CleanQueue(sletat.LoaderQueueToursName)
}