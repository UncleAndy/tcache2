package main

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/data2db/db_manager_base"
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_manager"
	"github.com/uncleandy/tcache2/apps/data2db/partners_tours_db_manager"
	"github.com/uncleandy/tcache2/log"
	"os"
	"syscall"
	"os/signal"
	"github.com/uncleandy/tcache2/apps_libs"
)

// TODO: Process statistics (speed)

const (
	PidFileName = "/var/tmp/tcache2_data2db_manager.pid"
)

var (
	Workers []db_manager_base.ManagerBaseInterface
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

	db_manager_base.ForceStopThreads = true
}

func InitWorkers() {
	Workers = []db_manager_base.ManagerBaseInterface{
		&map_tours_db_manager.MapToursDbManager{},
		&partners_tours_db_manager.PartnersToursDbManager{},
	}
}

func main() {
	apps_libs.PidProcess(PidFileName)
	defer os.Remove(PidFileName)

	log.Info.Println("DB manager start...")
	signals := SignalsInit()
	go SignalsProcess(signals)

	db.Init()
	cache.InitFromEnv()
	cache.RedisInit()

	InitWorkers()
	RunWorkers()
	WaitWorkersFinish()
	log.Info.Println("DB manager finished.")
}

func RunWorkers() {
	for _, worker := range Workers {
		worker.Init()
		go worker.ManagerLoop()
	}
}

func WaitWorkersFinish() {
	for _, worker := range Workers {
		worker.WaitFinish()
	}
}
