package post_map_tours_price_logs

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"sync"
)

type PostMapToursWorker struct {
	Settings worker_base.WorkerSettings
	ToursChanel chan uint64
	FinishWaitGroup *sync.WaitGroup
}

func (post_worker *PostMapToursWorker) SendTour(tour_id uint64) {
	post_worker.ToursChanel <- tour_id
}
