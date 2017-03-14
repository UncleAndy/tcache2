package partners_tours_db_manager

import (
	"time"
	"github.com/uncleandy/tcache2/apps/data2db/db_manager_base"
	"github.com/uncleandy/tcache2/log"
)

func (worker *PartnersToursDbManager) RunStatisticLoop() {
	worker.StatLastCheckTime = time.Now()

	ticker := time.NewTicker(10 * time.Second)
	go func(){
		for !db_manager_base.ForceStopThreads {
			select {
			case <-ticker.C:
				worker.StatisticsOutput()
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (worker *PartnersToursDbManager) StatisticsOutput() {
	delta := time.Since(worker.StatLastCheckTime)
	delta_tours := worker.StatProcessedTours - worker.StatLastProcessedTours
	speed := float64(delta_tours) / delta.Seconds()
	worker.StatLastCheckTime = time.Now()
	worker.StatLastProcessedTours = worker.StatProcessedTours

	log.Info.Printf(
		"STAT: Partners tours db manager progress of %s: %d (%.0f t/s)\n",
		worker.StatCurrentProcess,
		worker.StatProcessedTours,
		speed,
	)
}

