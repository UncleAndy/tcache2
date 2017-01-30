package db_worker_base

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"fmt"
	"github.com/uncleandy/tcache2/tours"
	"runtime"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/cache"
	"gopkg.in/redis.v4"
)

type DbWorkerBase struct {
	Settings worker_base.WorkerSettings
	FinishChanel chan bool
}

func (worker *DbWorkerBase) InsertProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string) {
	insert_queue := fmt.Sprintf(queue_template, thread_index)
	insert_tours := make([]tours.TourInterface, batch_size)
	insert_tours_index := 0
	for {
		id_str, err := cache.GetQueue(insert_queue)

		// Check finish loop
		if err == redis.Nil {
			_, err := cache.Get(0, thread_flag_key)
			if err != redis.Nil {
				// Flush data if present
				if insert_tours_index > 0 {
					worker.InsertToursFlush(&insert_tours, insert_tours_index)
					insert_tours_index = 0
				}

				cache.Incr(0, thread_flag_key)
				break
			} else {
				runtime.Gosched()
				continue
			}
		} else if err != nil {
			log.Error.Print("WARNING! Error read insert queue for db:"+err)
			continue
		}

		tour, err := worker.ReadTour(id_str)
		if err != nil {
			runtime.Gosched()
			continue
		}

		insert_tours[insert_tours_index] = tour
		insert_tours_index++
		if insert_tours_index >= batch_size {
			worker.InsertToursFlush(&insert_tours, insert_tours_index)
			insert_tours_index = 0
		}
	}
}

func (worker *DbWorkerBase) UpdateProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string) {
	update_queue := fmt.Sprintf(queue_template, thread_index)
	update_tours := make([]tours.TourInterface, batch_size)
	update_tours_index := 0
	for {
		id_str, err := cache.GetQueue(update_queue)

		// Check finish loop
		if err == redis.Nil {
			_, err := cache.Get(0, thread_flag_key)
			if err != redis.Nil {
				// Flush data if present
				if update_tours_index > 0 {
					worker.UpdateToursFlush(&update_tours, update_tours_index)
					update_tours_index = 0
				}

				cache.Incr(0, thread_flag_key)
				break
			} else {
				runtime.Gosched()
				continue
			}
		} else if err != nil {
			log.Error.Print("WARNING! Error read update queue for db:"+err)
			continue
		}

		tour, err := worker.ReadTour(id_str)
		if err != nil {
			runtime.Gosched()
			continue
		}

		update_tours[update_tours_index] = tour
		update_tours_index++
		if update_tours_index >= batch_size {
			worker.UpdateToursFlush(&update_tours, update_tours_index)
			update_tours_index = 0
		}
	}
}

func (worker *DbWorkerBase) DeleteProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string) {
	delete_queue := fmt.Sprintf(queue_template, thread_index)
	delete_tours := make([]string, batch_size)
	delete_tours_index := 0
	for {
		id_str, err := cache.GetQueue(delete_queue)

		// Check finish loop
		if err == redis.Nil {
			_, err := cache.Get(0, thread_flag_key)
			if err != redis.Nil {
				// Flush data if present
				if delete_tours_index > 0 {
					worker.DeleteToursFlush(&delete_tours, delete_tours_index)
					delete_tours_index = 0
				}

				cache.Incr(0, thread_flag_key)
				break
			} else {
				runtime.Gosched()
				continue
			}
		} else if id_str == "" || err != nil {
			log.Error.Print("WARNING! Error read delete queue for db:"+err)
			continue
		}

		delete_tours[delete_tours_index] = id_str
		delete_tours_index++
		if delete_tours_index >= batch_size {
			worker.DeleteToursFlush(&delete_tours, delete_tours_index)
			delete_tours_index = 0
		}
	}
}

func (worker *DbWorkerBase) ReadTour(id_str string) (tours.TourInterface, error) {
	log.Error.Fatal("It is base method. You can not call it!")
	return nil, nil
}

func (worker *DbWorkerBase) InsertToursFlush(tours *[]tours.TourInterface, size int) {
	log.Error.Fatal("It is base method. You can not call it!")
}

func (worker *DbWorkerBase) UpdateToursFlush(tours *[]tours.TourInterface, size int) {
	log.Error.Fatal("It is base method. You can not call it!")
}

func (worker *DbWorkerBase) DeleteToursFlush(tours *[]string, size int) {
	log.Error.Fatal("It is base method. You can not call it!")
}
