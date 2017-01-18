package main

import (
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
)

func InitWorkers() {
	worker_base.Workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}
}

func main() {
	cache.InitFromEnv()
	db.Init()

	InitWorkers()
	worker_base.RunWorkers()
	worker_base.RunManagerLoop()
	worker_base.WaitWorkersFinish()
}
