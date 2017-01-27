package map_tours_db_worker

import "github.com/uncleandy/tcache2/apps/workers/worker_base"

type MapToursDbWorker struct {
	Settings worker_base.WorkerSettings
	FinishChanel chan bool
}
