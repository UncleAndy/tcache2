package main

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_worker"
	"github.com/uncleandy/tcache2/apps/data2db/partners_tours_db_worker"
)

var (
	Workers []worker_base.WorkerBaseInterface
)

func InitDbWorkers() {
	Workers = []worker_base.WorkerBaseInterface{
		&map_tours_db_worker.MapToursDbWorker{},
		&partners_tours_db_worker.PartnersToursDbWorker{},
	}
}

func main() {
	db.Init()
	cache.InitFromEnv()

	InitDbWorkers()
	RunDbWorkers()
	WaitDbWorkersFinish()
}

func RunDbWorkers() {
	for _, worker := range Workers {
		worker.Init()
		go worker.MainLoop()
	}
}

func WaitDbWorkersFinish() {
	for _, worker := range Workers {
		worker.WaitFinish()
	}
}

