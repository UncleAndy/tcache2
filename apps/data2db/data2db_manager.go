package main

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/data2db/manager_base"
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_manager"
	"github.com/uncleandy/tcache2/apps/data2db/partners_tours_db_manager"
)

var (
	Workers []manager_base.ManagerBaseInterface
)

func InitWorkers() {
	Workers = []manager_base.ManagerBaseInterface{
		&map_tours_db_manager.MapToursDbManager{},
		&partners_tours_db_manager.PartnersToursDbManager{},
	}
}

func main() {
	db.Init()
	cache.InitFromEnv()

	InitWorkers()
	RunWorkers()
	WaitWorkersFinish()
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
