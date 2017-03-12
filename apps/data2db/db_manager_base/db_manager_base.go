package db_manager_base

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/log"
	"fmt"
	"strconv"
	"time"
	"gopkg.in/yaml.v2"
	"os"
	"io/ioutil"
	"gopkg.in/redis.v4"
)

type ManagerBaseInterface interface {
	Init()
	ManagerLoop()
	WaitFinish()
}

type ManagerBase struct {
	Settings worker_base.WorkerSettings
	FinishChanel chan bool

	TourInsertQueue string
	TourUpdateQueue string
	TourDeleteQueue string
	TourInsertThreadQueueTemplate string
	TourUpdateThreadQueueTemplate string
	TourDeleteThreadQueueTemplate string

	TourInsertThreadDataCounter string
	TourUpdateThreadDataCounter string
	TourDeleteThreadDataCounter string
}

func (worker *ManagerBase) ManagerLoop() {
	log.Info.Println("Start manager main loop...")
	cache.Del(0, worker.TourInsertThreadDataCounter)
	cache.Del(0, worker.TourUpdateThreadDataCounter)
	cache.Del(0, worker.TourDeleteThreadDataCounter)

	insert_queue_length := cache.QueueSize(worker.TourInsertQueue)
	update_queue_length := cache.QueueSize(worker.TourUpdateQueue)
	delete_queue_length := cache.QueueSize(worker.TourDeleteQueue)

	if insert_queue_length > 0 {
		log.Info.Println("Start manager INSERT loop...")
		for i := int64(0); i < insert_queue_length; i++ {
			id_str, err := cache.GetQueue(worker.TourInsertQueue)
			if err != nil {
				log.Error.Print("Error get tour ID from insert queue:", err)
				continue
			}
			worker.SendTourInsert(id_str)
		}
		worker.ThreadsInsertDataFinished()
		log.Info.Println("Finish manager INSERT loop.")
	}

	if update_queue_length > 0 {
		log.Info.Println("Start manager UPDATE loop...")
		for i := int64(0); i < update_queue_length; i++ {
			id_str, err := cache.GetQueue(worker.TourUpdateQueue)
			if err != nil {
				log.Error.Print("Error get tour ID from update queue:", err)
				continue
			}
			worker.SendTourUpdate(id_str)
		}
		worker.ThreadsUpdateDataFinished()
		log.Info.Println("Finish manager UPDATE loop.")
	}

	if delete_queue_length > 0 {
		log.Info.Println("Start manager DELETE loop...")
		for i := int64(0); i < delete_queue_length; i++ {
			id_str, err := cache.GetQueue(worker.TourDeleteQueue)
			if err != nil {
				log.Error.Print("Error get tour ID from delete queue:", err)
				continue
			}
			worker.SendTourDelete(id_str)
		}
		worker.ThreadsDeleteDataFinished()
		log.Info.Println("Finish manager DELETE loop.")
	}

	if insert_queue_length > 0 || update_queue_length > 0 || delete_queue_length > 0 {
		log.Info.Println("Wait finish db workers processes...")
		worker.WaitThreadsFlushData()
		log.Info.Println("DB workers processes finished.")
	}
	worker.FinishChanel <- true
}

func (worker *ManagerBase) SendTourInsert(id_str string) {
	worker.SendTourTo(id_str, worker.TourInsertThreadQueueTemplate)
}

func (worker *ManagerBase) SendTourUpdate(id_str string) {
	worker.SendTourTo(id_str, worker.TourUpdateThreadQueueTemplate)
}

func (worker *ManagerBase) SendTourDelete(id_str string) {
	worker.SendTourTo(id_str, worker.TourDeleteThreadQueueTemplate)
}

func (worker *ManagerBase) SendTourTo(id_str string, template string) {
	id, err := strconv.ParseUint(id_str, 10, 64)
	if err != nil {
		log.Error.Print("Error parse uint:", err)
	}

	thread_idx := id % uint64(worker.Settings.AllThreadsCount)
	thread_key := fmt.Sprintf(template, thread_idx)
	cache.AddQueue(thread_key, id_str)
}

func (worker *ManagerBase) ThreadsInsertDataFinished() {
	cache.Set(0, worker.TourInsertThreadDataCounter, "0")
}

func (worker *ManagerBase) ThreadsUpdateDataFinished() {
	cache.Set(0, worker.TourUpdateThreadDataCounter, "0")
}

func (worker *ManagerBase) ThreadsDeleteDataFinished() {
	cache.Set(0, worker.TourDeleteThreadDataCounter, "0")
}

func (worker *ManagerBase) WaitThreadsFlushData() {
	for {
		if 	worker.ThreadsCounterFinished(worker.TourInsertThreadDataCounter) &&
			worker.ThreadsCounterFinished(worker.TourUpdateThreadDataCounter) &&
			worker.ThreadsCounterFinished(worker.TourDeleteThreadDataCounter) {
			break
		}

		time.Sleep(1 * time.Second)
	}
	cache.Del(0, worker.TourInsertThreadDataCounter)
	cache.Del(0, worker.TourUpdateThreadDataCounter)
	cache.Del(0, worker.TourDeleteThreadDataCounter)
}

func (worker *ManagerBase) ThreadsCounterFinished(counter_key string) bool {
	counter_str, err := cache.Get(0, counter_key)
	if err == redis.Nil {
		return true
	}
	if err != nil {
		log.Error.Print("Error read flush counter in manager:", err)
	}
	counter, err := strconv.ParseUint(counter_str, 10, 64)
	if err != nil {
		log.Error.Print("Error parse flush counter in manager:", err)
	}
	return counter >= uint64(worker.Settings.AllThreadsCount)
}

func (worker *ManagerBase) WaitFinish() {
	<- worker.FinishChanel
}

func  (worker *ManagerBase) LoadWorkerConfig(env_config_file_name string) {
	config_file := os.Getenv(env_config_file_name)
	if config_file == "" {
		log.Error.Fatalf("Map tours worker config file name required (%s environment)", env_config_file_name)
	}
	_, err := os.Stat(config_file)
	if os.IsNotExist(err) {
		log.Error.Fatalf("Map tours worker config file '%s' not exists.", config_file)
	}

	dat, err := ioutil.ReadFile(config_file)
	if err != nil {
		log.Error.Fatalln(err)
	}

	err = yaml.Unmarshal(dat, &worker.Settings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}

func (worker *ManagerBase) GetSettings() *worker_base.WorkerSettings {
	return &worker.Settings
}
