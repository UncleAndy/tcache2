package partners_tours_db_manager

import (
	"github.com/uncleandy/tcache2/apps/data2db/partners_tours_db_worker"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"sync"
)

const (
	EnvWorkerFileConfig = "PARTNERS_TOURS_DB_MANAGER_CONFIG"
)

func (worker *PartnersToursDbManager) Init() {
	worker.ManagerType = "partners"
	worker.LoadWorkerConfig(EnvWorkerFileConfig)
	worker.FinishChanel = make(chan bool)
	worker.StatMutex = &sync.Mutex{}

	worker.TourInsertQueue = partners_tours.PartnersTourInsertQueue
	worker.TourUpdateQueue = partners_tours.PartnersTourUpdateQueue
	worker.TourDeleteQueue = partners_tours.PartnersTourDeleteQueue
	worker.TourInsertThreadQueueTemplate = partners_tours_db_worker.PartnersTourInsertThreadQueueTemplate
	worker.TourUpdateThreadQueueTemplate = partners_tours_db_worker.PartnersTourUpdateThreadQueueTemplate
	worker.TourDeleteThreadQueueTemplate = partners_tours_db_worker.PartnersTourDeleteThreadQueueTemplate
	worker.TourInsertThreadDataCounter = partners_tours_db_worker.PartnersTourInsertThreadDataCounter
	worker.TourUpdateThreadDataCounter = partners_tours_db_worker.PartnersTourUpdateThreadDataCounter
	worker.TourDeleteThreadDataCounter = partners_tours_db_worker.PartnersTourDeleteThreadDataCounter

	worker.RunStatisticLoop()
}
