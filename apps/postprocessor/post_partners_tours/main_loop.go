package post_partners_tours

import (
	"github.com/uncleandy/tcache2/log"
	"sync"
)

var (
	WorkerKeysCount = 0
	WorkerKeysCountMutex *sync.Mutex
)

func (post_worker *PostPartnersToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	post_worker.InitThreads()
}

func (post_worker *PostPartnersToursWorker) InitThreads() {
	WorkerKeysCountMutex = &sync.Mutex{}
	for i := 0; i < post_worker.Settings.WorkerThreadsCount; i++ {
		thread := post_worker.Settings.WorkerFirstThreadId + i
		go func() {
			post_worker.Thread(thread)
		}()
	}
}

func (post_worker *PostPartnersToursWorker) FinishThreads() {
	close(post_worker.ToursChanel)
}

func (post_worker *PostPartnersToursWorker) Thread(thread_index int) {
	log.Info.Println("Start thread ", thread_index, "...")
	for id := range post_worker.ToursChanel {
		WorkerKeysCountMutex.Lock()
		WorkerKeysCount++
		WorkerKeysCountMutex.Unlock()

		post_worker.CheckClean(id)
	}

	log.Info.Println("Finish thread ", thread_index)
	post_worker.FinishWaitGroup.Done()
}
