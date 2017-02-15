package post_partners_tours

func (post_worker *PostPartnersToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	post_worker.InitThreads()
}

func (post_worker *PostPartnersToursWorker) InitThreads() {
	for i := 0; i < post_worker.Settings.WorkerThreadsCount; i++ {
		post_worker.Thread(post_worker.Settings.WorkerFirstThreadId + i)
	}
}

func (post_worker *PostPartnersToursWorker) FinishThreads() {
	close(post_worker.ToursChanel)
}

func (post_worker *PostPartnersToursWorker) Thread(thread_index int) {
	go func() {
		for id := range post_worker.ToursChanel {
			if id % uint64(post_worker.Settings.AllThreadsCount) == uint64(thread_index) {
				post_worker.CheckClean(id)
			}
		}

		post_worker.FinishWaitGroup.Done()
	}()
}
