package map_tours_db_manager

import (
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_worker"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
)

const (
	EnvWorkerFileConfig = "MAP_TOURS_DB_MANAGER_CONFIG"
)

func (worker *MapToursDbManager) Init() {
	worker.LoadWorkerConfig(EnvWorkerFileConfig)
	worker.FinishChanel = make(chan bool)

	worker.TourFlushThreadDataCounter = map_tours_db_worker.MapTourFlushThreadDataCounter
	worker.TourInsertQueue = map_tours.MapTourInsertQueue
	worker.TourUpdateQueue = map_tours.MapTourUpdateQueue
	worker.TourDeleteQueue = map_tours.MapTourDeleteQueue
	worker.TourInsertThreadQueueTemplate = map_tours_db_worker.MapTourInsertThreadQueueTemplate
	worker.TourUpdateThreadQueueTemplate = map_tours_db_worker.MapTourUpdateThreadQueueTemplate
	worker.TourDeleteThreadQueueTemplate = map_tours_db_worker.MapTourDeleteThreadQueueTemplate
}
