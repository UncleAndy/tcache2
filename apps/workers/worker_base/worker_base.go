package worker_base

import (
	"github.com/uncleandy/tcache2/cache"
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
)

func RunWorkers() {
	for _, worker := range Workers {
		worker.Init()
		go worker.MainLoop()
	}
}

func WaitWorkersFinish() {
	for _, worker := range Workers {
		worker.WaitFinish()
	}
}

func CheckWaitIncomeToursFlag(name string) bool {
	_, err := cache.Get(0, name)

	return (err == nil)
}

func SetWaitIncomeToursFlag(name string) {
	cache.Set(0, name, "1")
}

func CleanWaitIncomeToursFlag(name string) {
	cache.Del(0, name)
}
