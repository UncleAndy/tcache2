package worker_base

import (
	"time"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
)

type WorkerBaseInterface interface {
	Init()
	MainLoop()
	WaitFinish()
	SendTour(string)
	IsPrimary() bool
}

var (
	workers = []WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}
)

func RunWorkers() {
	for _, worker := range workers {
		worker.Init()
		go worker.MainLoop()
	}
}

func RunManagerLoop() {
	// Only one loader tours manager
	if workers[0].IsPrimary() {
		ManagerLoop()
	}
}

func WaitWorkersFinish() {
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
