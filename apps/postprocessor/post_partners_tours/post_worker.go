package post_partners_tours

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"sync"
)

type PostPartnersToursWorker struct {
	Settings worker_base.WorkerSettings
	ToursChanel chan uint64
	FinishWaitGroup *sync.WaitGroup
}

func (post_worker *PostPartnersToursWorker) SendTour(tour_id uint64) {
	post_worker.ToursChanel <- tour_id
}
