package main

import (
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
)

const (
	workers = []worker_base.WorkerBaseInterface{
		map_tours.MapToursWorker{},
	}
)

func main() {
	cache.InitFromEnv()
	db.Init()

	for _, worker := range workers {
		worker.Init()
		go worker.MainLoop()
	}

	for _, worker := range workers {
		worker.WaitFinish()
	}
}
