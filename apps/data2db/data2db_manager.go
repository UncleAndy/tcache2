package main

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/data2db/db_manager_base"
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_manager"
	"github.com/uncleandy/tcache2/apps/data2db/partners_tours_db_manager"
	"github.com/uncleandy/tcache2/log"
)

var (
	Workers []db_manager_base.ManagerBaseInterface
)

func InitWorkers() {
	Workers = []db_manager_base.ManagerBaseInterface{
		&map_tours_db_manager.MapToursDbManager{},
		&partners_tours_db_manager.PartnersToursDbManager{},
	}
}

func main() {
	log.Info.Println("DB manager start...")
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
