package db_worker_base

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"fmt"
	"github.com/uncleandy/tcache2/tours"
	"runtime"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/cache"
	"gopkg.in/redis.v4"
	"github.com/uncleandy/tcache2/db"
)


var (
	ForceStopThreads = false
)

type DbWorkerBase struct {
	Settings        worker_base.WorkerSettings
	FinishChanel    chan bool
	DbSQLAction     DbSQLActionInterface
	RedisTourReader RedisTourReaderInterface
	DbPool 		[]db.DbConnection
}

type RedisTourReaderInterface interface {
	ReadTour(id_str string) (tours.TourInterface, error)
}

type DbSQLActionInterface interface {
	InsertToursFlush(tours *[]tours.TourInterface, size int, db_conn *db.DbConnection)
	UpdateToursFlush(tours *[]tours.TourInterface, size int, db_conn *db.DbConnection)
	DeleteToursFlush(tours *[]string, size int, db_conn *db.DbConnection)
}

type DbWorkerBaseInterface interface {
	Init()
	MainLoop()
	WaitFinish()
	InsertProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string, db_conn *db.DbConnection)
	UpdateProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string, db_conn *db.DbConnection)
	DeleteProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string, db_conn *db.DbConnection)
}

func (worker *DbWorkerBase) InsertProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string, db_conn *db.DbConnection) {
	insert_queue := fmt.Sprintf(queue_template, thread_index)
	insert_tours := make([]tours.TourInterface, batch_size)
	insert_tours_index := 0
	for {
		var id_str string
		var err error
		if !ForceStopThreads {
			id_str, err = cache.GetQueue(insert_queue)
		}

		// Check finish loop
		if err == redis.Nil || ForceStopThreads {
			log.Info.Println("Insert queue ", insert_queue, " empty. Check finish flag: ", thread_flag_key, "...")
			flag, err := cache.Get(0, thread_flag_key)
			if err != redis.Nil || ForceStopThreads {
				log.Info.Println("Insert. Finish flag not null:", flag)
				// Flush data if present
				if insert_tours_index > 0 {
					worker.DbSQLAction.InsertToursFlush(&insert_tours, insert_tours_index, db_conn)
					insert_tours_index = 0
				}

				log.Info.Println("Insert. Finish flag increment.")
				cache.Incr(0, thread_flag_key)
				break
			} else {
				runtime.Gosched()
				continue
			}
		} else if err != nil {
			log.Error.Print("WARNING! Error read insert queue for db:", err)
			continue
		} else {
			//log.Info.Print("NOT NULL QUEUE ", insert_queue, " VALUE:", id_str)
		}

		tour, err := worker.RedisTourReader.ReadTour(id_str)
		if err != nil {
			runtime.Gosched()
			continue
		}

		insert_tours[insert_tours_index] = tour
		insert_tours_index++
		if insert_tours_index >= batch_size {
			worker.DbSQLAction.InsertToursFlush(&insert_tours, insert_tours_index, db_conn)
			insert_tours_index = 0
		}
	}
}

func (worker *DbWorkerBase) UpdateProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string, db_conn *db.DbConnection) {
	update_queue := fmt.Sprintf(queue_template, thread_index)
	update_tours := make([]tours.TourInterface, batch_size)
	update_tours_index := 0
	for {
		var id_str string
		var err error
		if !ForceStopThreads {
			id_str, err = cache.GetQueue(update_queue)
		}

		// Check finish loop
		if err == redis.Nil || ForceStopThreads {
			_, err := cache.Get(0, thread_flag_key)
			if err != redis.Nil || ForceStopThreads {
				// Flush data if present
				if update_tours_index > 0 {
					worker.DbSQLAction.UpdateToursFlush(&update_tours, update_tours_index, db_conn)
					update_tours_index = 0
				}

				cache.Incr(0, thread_flag_key)
				break
			} else {
				runtime.Gosched()
				continue
			}
		} else if err != nil {
			log.Error.Print("WARNING! Error read update queue for db:", err)
			continue
		}

		tour, err := worker.RedisTourReader.ReadTour(id_str)
		if err != nil {
			runtime.Gosched()
			continue
		}

		update_tours[update_tours_index] = tour
		update_tours_index++
		if update_tours_index >= batch_size {
			worker.DbSQLAction.UpdateToursFlush(&update_tours, update_tours_index, db_conn)
			update_tours_index = 0
		}
	}
}

func (worker *DbWorkerBase) DeleteProcessBy(thread_index int, batch_size int, queue_template string, thread_flag_key string, db_conn *db.DbConnection) {
	delete_queue := fmt.Sprintf(queue_template, thread_index)
	delete_tours := make([]string, batch_size)
	delete_tours_index := 0
	for {
		var id_str string
		var err error
		if !ForceStopThreads {
			id_str, err = cache.GetQueue(delete_queue)
		}

		// Check finish loop
		if err == redis.Nil || ForceStopThreads {
			_, err := cache.Get(0, thread_flag_key)
			if err != redis.Nil || ForceStopThreads {
				// Flush data if present
				if delete_tours_index > 0 {
					worker.DbSQLAction.DeleteToursFlush(&delete_tours, delete_tours_index, db_conn)
					delete_tours_index = 0
				}

				cache.Incr(0, thread_flag_key)
				break
			} else {
				runtime.Gosched()
				continue
			}
		} else if id_str == "" || err != nil {
			log.Error.Print("WARNING! Error read delete queue for db:", err)
			continue
		}

		delete_tours[delete_tours_index] = id_str
		delete_tours_index++
		if delete_tours_index >= batch_size {
			worker.DbSQLAction.DeleteToursFlush(&delete_tours, delete_tours_index, db_conn)
			delete_tours_index = 0
		}
	}
}

func (worker *DbWorkerBase) DbConnectionByThread(thread_index int) *db.DbConnection {
	return &worker.DbPool[thread_index - worker.Settings.WorkerFirstThreadId]
}