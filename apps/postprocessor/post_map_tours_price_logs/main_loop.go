package post_map_tours_price_logs

func (post_worker *PostMapToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	post_worker.InitThreads()
}

func (post_worker *PostMapToursWorker) InitThreads() {
	for i := 0; i < post_worker.Settings.WorkerThreadsCount; i++ {
		post_worker.Thread(post_worker.Settings.WorkerFirstThreadId + i)
	}
}

func (post_worker *PostMapToursWorker) Thread(thread_index int) {
	go func() {
		for id := range post_worker.ToursChanel {
			if id % uint64(post_worker.Settings.AllThreadsCount) == thread_index {
				post_worker.ProcessPriceLogs(id)
			}
		}

		post_worker.FinishWaitGroup.Done()
	}()
}
