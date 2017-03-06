package sletat

import (
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/log"
	"time"
)

var (
	IdleCounter = 0
	WaitCounter = 0
	InToursCounter = int64(0)
	LastStatTime time.Time
)

func RunStatisticLoop() {
	LastStatTime = time.Now()
	ticker := time.NewTicker(10 * time.Second)
	go func(){
		for !ForceStopFlag {
			select {
			case <-ticker.C:
				StatisticsOutput()
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func StatisticsOutput() {
	cache.QueueSizesUpdateAll(LoaderQueueToursName)
	queue_length := cache.QueueSize(LoaderQueueToursName)

	if (queue_length == 0) {
		IdleCounter++
	}

	delta := time.Since(LastStatTime)
	speed := float64(InToursCounter) / delta.Seconds()
	InToursCounter = 0
	LastStatTime = time.Now()

	log.Info.Printf(
		"STAT: Queue size = %d, idle counter = %d, wait counter = %d (speed: %.0f t/s)\n",
		queue_length,
		IdleCounter,
		WaitCounter,
		speed,
	)
}