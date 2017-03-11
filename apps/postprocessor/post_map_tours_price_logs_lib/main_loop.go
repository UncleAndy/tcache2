package post_map_tours_price_logs

import (
	"sync"
	"github.com/uncleandy/tcache2/log"
)

var (
	ForceStopThreads = false
	WorkerKeysCountMutex *sync.Mutex
)

func (post_worker *PostMapToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	post_worker.InitThreads()
}

func (post_worker *PostMapToursWorker) InitThreads() {
	WorkerKeysCountMutex = &sync.Mutex{}
	for i := 0; i < post_worker.Settings.WorkerThreadsCount; i++ {
		thread := post_worker.Settings.WorkerFirstThreadId + i
		go func() {
			post_worker.Thread(thread)
		}()
	}
}

func (post_worker *PostMapToursWorker) FinishThreads() {
	close(post_worker.ToursChanel)
}

func (post_worker *PostMapToursWorker) Thread(thread_index int) {
	log.Info.Println("Start thread ", thread_index, "...")

	for id := range post_worker.ToursChanel {
		WorkerKeysCountMutex.Lock()
		WorkerKeysProcessed++
		WorkerKeysCountMutex.Unlock()

		post_worker.ProcessPriceLogs(id)
	}

	log.Info.Println("Finish thread ", thread_index)
	post_worker.FinishWaitGroup.Done()
}
