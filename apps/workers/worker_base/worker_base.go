package worker_base

import (
	"time"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
)

type WorkerBaseInterface interface {
	Init()
	Stop()
	LoadWorkerConfig()
	LoadDictData()
	MainLoop()
	WaitFinish()
	SendTour(string)
	IsPrimary() bool
	GetSettings() *WorkerSettings
}

type WorkerSettings struct {
	WorkerFirstThreadId 	int		`yaml:"worker_first_thread_id"`
	WorkerThreadsCount 	int		`yaml:"worker_threads_count"`
	AllThreadsCount 	int		`yaml:"all_threads_count"`
}

var (
	Workers []WorkerBaseInterface
	ForceStopManagerLoop = false
	ToursBatchSize = int64(1000)
)

func RunWorkers() {
	for _, worker := range Workers {
		worker.Init()
		go worker.MainLoop()
	}
}

func RunManagerLoop() {
	ManagerLoop()
}

func WaitWorkersFinish() {
	for _, worker := range Workers {
		worker.WaitFinish()
	}
}

func ManagerLoop() {
	// Scan Redis tours loader queue & move tours to worker threads Redis queue
	println("Main loop start...")
	for !ForceStopManagerLoop {
		tours, err := cache.GetQueueBatch(sletat.LoaderQueueToursName, ToursBatchSize)
		if err != nil || len(tours) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for _, tour_str := range tours {
			for _, worker := range Workers {
				worker.SendTour(tour_str)
			}
		}
	}
	println("Main loop stoped.")
}
