package partners_tours_db_manager

import (
	"github.com/uncleandy/tcache2/apps/data2db/partners_tours_db_worker"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
)

const (
	EnvWorkerFileConfig = "PARTNERS_TOURS_DB_MANAGER_CONFIG"
)

func (worker *PartnersToursDbManager) Init() {
	worker.LoadWorkerConfig(EnvWorkerFileConfig)
	worker.FinishChanel = make(chan bool)

	worker.TourFlushThreadDataCounter = partners_tours_db_worker.PartnersTourFlushThreadDataCounter
	worker.TourInsertQueue = partners_tours.PartnersTourInsertQueue
	worker.TourUpdateQueue = partners_tours.PartnersTourUpdateQueue
	worker.TourDeleteQueue = partners_tours.PartnersTourDeleteQueue
	worker.TourInsertThreadQueueTemplate = partners_tours_db_worker.PartnersTourInsertThreadQueueTemplate
	worker.TourUpdateThreadQueueTemplate = partners_tours_db_worker.PartnersTourUpdateThreadQueueTemplate
	worker.TourDeleteThreadQueueTemplate = partners_tours_db_worker.PartnersTourDeleteThreadQueueTemplate
}
