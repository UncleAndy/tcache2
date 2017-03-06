package main

import (
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
//	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"os"
	"syscall"
	"os/signal"
	"github.com/uncleandy/tcache2/log"
)

func InitWorkers() {
	worker_base.Workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
//		&partners_tours.PartnersToursWorker{},
	}
}
func SignalsWorkerInit() (chan os.Signal) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	return sigChan
}


func SignalsWorkerProcess(signals chan os.Signal) {
	<- signals

	log.Info.Println("Detect stop command. Please, wait...")

	for _, worker := range worker_base.Workers {
		worker.Stop()
	}
}

func main() {
	signals := SignalsWorkerInit()
	go SignalsWorkerProcess(signals)

	cache.InitFromEnv()
	cache.RedisInit()
	db.Init()

	InitWorkers()
	worker_base.RunWorkers()
	worker_base.WaitWorkersFinish()
	log.Info.Println("Finished")
}
