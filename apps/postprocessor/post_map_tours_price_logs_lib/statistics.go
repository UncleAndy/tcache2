package post_map_tours_price_logs

import (
	"time"
	"github.com/uncleandy/tcache2/log"
)

var (
	LastToursCounter = 0
	LastStatTime time.Time
)

func (post_worker *PostMapToursWorker) RunStatisticLoop() {
	LastStatTime = time.Now()

	ticker := time.NewTicker(10 * time.Second)
	go func(){
		for !ForceStopThreads {
			select {
			case <-ticker.C:
				post_worker.StatisticsOutput()
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (post_worker *PostMapToursWorker) StatisticsOutput() {
	delta := time.Since(LastStatTime)
	deltaToursCounter := WorkerKeysProcessed - LastToursCounter
	speed := float64(deltaToursCounter) / delta.Seconds()
	LastToursCounter = WorkerKeysProcessed
	LastStatTime = time.Now()

	log.Info.Printf("STAT: Map tours price log tours procesed: %d (%.0f t/s)\n", WorkerKeysProcessed, speed)
}