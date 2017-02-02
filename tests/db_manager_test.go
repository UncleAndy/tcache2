package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/apps/data2db/db_manager_base"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"gopkg.in/redis.v4"
	"runtime"
)

func InitTestDB() {

}

func TestDbManagerLoop(t *testing.T) {
	init_test_redis_multi()

	manager := db_manager_base.ManagerBase{}

	manager.Settings = worker_base.WorkerSettings{
		WorkerFirstThreadId: 	0,
		WorkerThreadsCount:	3,
		AllThreadsCount:	3,
	}
	manager.FinishChanel = make(chan bool)

	manager.TourInsertQueue = "test_insert_queue"
	manager.TourInsertThreadQueueTemplate = "test_insert_queue_%d"
	manager.TourInsertThreadDataCounter = "test_insert_data_counter"

	manager.TourUpdateQueue = "test_update_queue"
	manager.TourUpdateThreadQueueTemplate = "test_update_queue_%d"
	manager.TourUpdateThreadDataCounter = "test_update_data_counter"

	manager.TourDeleteQueue = "test_delete_queue"
	manager.TourDeleteThreadQueueTemplate = "test_delete_queue_%d"
	manager.TourDeleteThreadDataCounter = "test_delete_data_counter"

	cache.AddQueue(manager.TourInsertQueue, "1")
	cache.AddQueue(manager.TourInsertQueue, "2")
	cache.AddQueue(manager.TourInsertQueue, "3")
	cache.AddQueue(manager.TourInsertQueue, "4")

	cache.AddQueue(manager.TourUpdateQueue, "1")
	cache.AddQueue(manager.TourUpdateQueue, "2")
	cache.AddQueue(manager.TourUpdateQueue, "3")
	cache.AddQueue(manager.TourUpdateQueue, "4")

	cache.AddQueue(manager.TourDeleteQueue, "1")
	cache.AddQueue(manager.TourDeleteQueue, "2")
	cache.AddQueue(manager.TourDeleteQueue, "3")
	cache.AddQueue(manager.TourDeleteQueue, "4")

	// Wait counters and set it to all threads count value for finish ManagerLoop()
	go func() {
		var err error
		err = redis.Nil

		// Insert
		for err == redis.Nil {
			_, err = cache.Get(0, manager.TourInsertThreadDataCounter)
			runtime.Gosched()
		}
		cache.Set(0, manager.TourInsertThreadDataCounter, "3")

		// Update
		err = redis.Nil
		for err == redis.Nil {
			_, err = cache.Get(0, manager.TourUpdateThreadDataCounter)
			runtime.Gosched()
		}
		cache.Set(0, manager.TourUpdateThreadDataCounter, "3")

		// Delete
		err = redis.Nil
		for err == redis.Nil {
			_, err = cache.Get(0, manager.TourDeleteThreadDataCounter)
			runtime.Gosched()
		}
		cache.Set(0, manager.TourDeleteThreadDataCounter, "3")
	}()

	go manager.ManagerLoop()
	manager.WaitFinish()

	// Check treads queues
	insert_thread_queue_name_0 := fmt.Sprintf(manager.TourInsertThreadQueueTemplate, 0)
	insert_thread_queue_name_1 := fmt.Sprintf(manager.TourInsertThreadQueueTemplate, 1)
	insert_thread_queue_name_2 := fmt.Sprintf(manager.TourInsertThreadQueueTemplate, 2)
	insert_thread_queue_size_0 := cache.QueueSize(insert_thread_queue_name_0)
	insert_thread_queue_size_1 := cache.QueueSize(insert_thread_queue_name_1)
	insert_thread_queue_size_2 := cache.QueueSize(insert_thread_queue_name_2)

	if insert_thread_queue_size_0 <= 0 {
		t.Error("Wrong size of insert queueu 0. Expected > 0, got: ", insert_thread_queue_size_0)
	} else if insert_thread_queue_size_0 != 1 {
		t.Error("Wrong size of insert queueu 0. Expected 1, got: ", insert_thread_queue_size_0)
	}
	if insert_thread_queue_size_1 <= 0 {
		t.Error("Wrong size of insert queueu 1. Expected > 0, got: ", insert_thread_queue_size_1)
	} else if insert_thread_queue_size_1 != 2 {
		t.Error("Wrong size of insert queueu 1. Expected 2, got: ", insert_thread_queue_size_1)
	}
	if insert_thread_queue_size_2 <= 0 {
		t.Error("Wrong size of insert queueu 2. Expected > 0, got: ", insert_thread_queue_size_2)
	} else if insert_thread_queue_size_2 != 1 {
		t.Error("Wrong size of insert queueu 2. Expected 1, got: ", insert_thread_queue_size_2)
	}

	update_thread_queue_name_0 := fmt.Sprintf(manager.TourUpdateThreadQueueTemplate, 0)
	update_thread_queue_name_1 := fmt.Sprintf(manager.TourUpdateThreadQueueTemplate, 1)
	update_thread_queue_name_2 := fmt.Sprintf(manager.TourUpdateThreadQueueTemplate, 2)
	update_thread_queue_size_0 := cache.QueueSize(update_thread_queue_name_0)
	update_thread_queue_size_1 := cache.QueueSize(update_thread_queue_name_1)
	update_thread_queue_size_2 := cache.QueueSize(update_thread_queue_name_2)

	if update_thread_queue_size_0 <= 0 {
		t.Error("Wrong size of update queueu 0. Expected > 0, got: ", update_thread_queue_size_0)
	} else if update_thread_queue_size_0 != 1 {
		t.Error("Wrong size of update queueu 0. Expected 1, got: ", update_thread_queue_size_0)
	}
	if update_thread_queue_size_1 <= 0 {
		t.Error("Wrong size of update queueu 1. Expected > 0, got: ", update_thread_queue_size_1)
	} else if update_thread_queue_size_1 != 2 {
		t.Error("Wrong size of update queueu 1. Expected 2, got: ", update_thread_queue_size_1)
	}
	if update_thread_queue_size_2 <= 0 {
		t.Error("Wrong size of update queueu 2. Expected > 0, got: ", update_thread_queue_size_2)
	} else if update_thread_queue_size_2 != 1 {
		t.Error("Wrong size of update queueu 2. Expected 1, got: ", update_thread_queue_size_2)
	}

	delete_thread_queue_name_0 := fmt.Sprintf(manager.TourDeleteThreadQueueTemplate, 0)
	delete_thread_queue_name_1 := fmt.Sprintf(manager.TourDeleteThreadQueueTemplate, 1)
	delete_thread_queue_name_2 := fmt.Sprintf(manager.TourDeleteThreadQueueTemplate, 2)
	delete_thread_queue_size_0 := cache.QueueSize(delete_thread_queue_name_0)
	delete_thread_queue_size_1 := cache.QueueSize(delete_thread_queue_name_1)
	delete_thread_queue_size_2 := cache.QueueSize(delete_thread_queue_name_2)

	if delete_thread_queue_size_0 <= 0 {
		t.Error("Wrong size of delete queueu 0. Expected > 0, got: ", delete_thread_queue_size_0)
	} else if delete_thread_queue_size_0 != 1 {
		t.Error("Wrong size of delete queueu 0. Expected 1, got: ", delete_thread_queue_size_0)
	}
	if delete_thread_queue_size_1 <= 0 {
		t.Error("Wrong size of delete queueu 1. Expected > 0, got: ", delete_thread_queue_size_1)
	} else if delete_thread_queue_size_1 != 2 {
		t.Error("Wrong size of delete queueu 1. Expected 2, got: ", delete_thread_queue_size_1)
	}
	if delete_thread_queue_size_2 <= 0 {
		t.Error("Wrong size of delete queueu 2. Expected > 0, got: ", delete_thread_queue_size_2)
	} else if delete_thread_queue_size_2 != 1 {
		t.Error("Wrong size of delete queueu 2. Expected 1, got: ", delete_thread_queue_size_2)
	}

	cache.CleanQueue(insert_thread_queue_name_0)
	cache.CleanQueue(insert_thread_queue_name_1)
	cache.CleanQueue(insert_thread_queue_name_2)
	cache.CleanQueue(update_thread_queue_name_0)
	cache.CleanQueue(update_thread_queue_name_1)
	cache.CleanQueue(update_thread_queue_name_2)
	cache.CleanQueue(delete_thread_queue_name_0)
	cache.CleanQueue(delete_thread_queue_name_1)
	cache.CleanQueue(delete_thread_queue_name_2)
}
