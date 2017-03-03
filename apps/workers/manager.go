package main

import (
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/db"
	"os"
	"syscall"
	"os/signal"
)

func InitManagers() {
	worker_base.Workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}
}

func InitManagersConfigs() {
	for _, worker := range worker_base.Workers {
		worker.LoadWorkerConfig()
		worker.LoadDictData()
	}
}

func SignalsInit() (chan os.Signal) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	return sigChan
}


func SignalsProcess(signals chan os.Signal) {
	<- signals

	println("Detect stop command. Please, wait...")

	worker_base.ForceStopManagerLoop = true
}

func main() {
	signals := SignalsInit()
	go SignalsProcess(signals)

	cache.InitFromEnv()
	cache.RedisInit()
	db.Init()
	InitManagers()
	InitManagersConfigs()
	worker_base.RunManagerLoop()
}
