package main

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_worker"
	"github.com/uncleandy/tcache2/apps/data2db/partners_tours_db_worker"
	"github.com/uncleandy/tcache2/apps/data2db/db_worker_base"
	"os"
	"syscall"
	"os/signal"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/apps_libs"
)

// TODO: Profile DB operations

const (
	PidFileName = "/var/tmp/tcache2_data2db_worker.pid"
)

var (
	Workers []db_worker_base.DbWorkerBaseInterface
)

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

	log.Info.Println("Detect stop command. Please, wait...")

	db_worker_base.ForceStopThreads = true
}

func InitDbWorkers() {
	Workers = []db_worker_base.DbWorkerBaseInterface{
		&map_tours_db_worker.MapToursDbWorker{},
		&partners_tours_db_worker.PartnersToursDbWorker{},
	}
}

func main() {
	apps_libs.PidProcess(PidFileName)
	defer os.Remove(PidFileName)

	signals := SignalsInit()
	go SignalsProcess(signals)

	db.Init()
	cache.InitFromEnv()
	cache.RedisInit()

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

