package main

import (
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"time"
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
)

var (
	workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}
)

func main() {
	cache.InitFromEnv()
	db.Init()

	for _, worker := range workers {
		worker.Init()
		go worker.MainLoop()
	}

	// Only one loader tours manager
	if workers[0].IsPrimary() {
		ManagerLoop()
	}

	for _, worker := range workers {
		worker.WaitFinish()
	}
}

func ManagerLoop() {
	// Scan Redis tours loader queue & move tours to worker threads Redis queue
	go func() {
		for true {
			tour_str, err := cache.GetQueue(sletat.LoaderQueueToursName)
			if err != nil || tour_str == "" {
				time.Sleep(1 * time.Second)
				continue
			}

			for _, worker := range workers {
				worker.SendTour(tour_str)
			}
		}
	}()
}