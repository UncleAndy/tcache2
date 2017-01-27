package partners_tours_db_worker

import "github.com/uncleandy/tcache2/apps/workers/worker_base"

const (
	PartnersTourInsertThreadQueueTemplate = "partners_tours_insert_%d"
	PartnersTourUpdateThreadQueueTemplate = "partners_tours_update_%d"
	PartnersTourDeleteThreadQueueTemplate = "partners_tours_delete_%d"
	PartnersTourInsertThreadDataCounter = "partners_tours_insert_counter"
	PartnersTourUpdateThreadDataCounter = "partners_tours_update_counter"
	PartnersTourDeleteThreadDataCounter = "partners_tours_delete_counter"
)

type PartnersToursDbWorker struct {
	Settings worker_base.WorkerSettings
	FinishChanel chan bool
}
