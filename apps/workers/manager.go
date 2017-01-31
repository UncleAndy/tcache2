package main

import (
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
)

func InitManagers() {
	worker_base.Workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}
}

func main() {
	cache.InitFromEnv()

	InitManagers()
	worker_base.RunManagerLoop()
}
