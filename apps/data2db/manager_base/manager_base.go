package manager_base

import (
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_worker"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/log"
	"fmt"
	"strconv"
	"time"
	"gopkg.in/yaml.v2"
	"os"
	"io/ioutil"
)

type ManagerBaseInterface interface {
	Init()
	ManagerLoop()
	WaitFinish()
}

type ManagerBase struct {
	Settings worker_base.WorkerSettings
	FinishChanel chan bool

	TourFlushThreadDataCounter string
	TourInsertQueue string
	TourUpdateQueue string
	TourDeleteQueue string
	TourInsertThreadQueueTemplate string
	TourUpdateThreadQueueTemplate string
	TourDeleteThreadQueueTemplate string
}

func (worker *ManagerBase) ManagerLoop() {
	cache.Del(0, worker.TourFlushThreadDataCounter)

	insert_queue_length := cache.QueueSize(worker.TourInsertQueue)
	update_queue_length := cache.QueueSize(worker.TourUpdateQueue)
	delete_queue_length := cache.QueueSize(worker.TourDeleteQueue)

	for i := 0; i < insert_queue_length; i++ {
		id_str, err := cache.GetQueue(worker.TourInsertQueue)
		if err != nil {
			log.Error.Print("Error get tour ID from insert queue:", err)
			continue
		}
		worker.SendTourInsert(id_str)
	}

	for i := 0; i < update_queue_length; i++ {
		id_str, err := cache.GetQueue(worker.TourUpdateQueue)
		if err != nil {
			log.Error.Print("Error get tour ID from update queue:", err)
			continue
		}
		worker.SendTourUpdate(id_str)
	}

	for i := 0; i < delete_queue_length; i++ {
		id_str, err := cache.GetQueue(worker.TourDeleteQueue)
		if err != nil {
			log.Error.Print("Error get tour ID from delete queue:", err)
			continue
		}
		worker.SendTourDelete(id_str)
	}

	worker.WaitThreadsFlushData()
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

func (worker *ManagerBase) WaitThreadsFlushData() {
	cache.Set(0, worker.TourFlushThreadDataCounter, '0')
	for true {
		counter_str, err := cache.Get(0, worker.TourFlushThreadDataCounter)
		if err != nil {
			log.Error.Print("Error read flush counter in manager:", err)
		}
		counter, err := strconv.ParseUint(counter_str, 10, 64)
		if err != nil {
			log.Error.Print("Error parse flush counter in manager:", err)
		}
		if counter >= worker.Settings.AllThreadsCount {
			break
		}
		time.Sleep(1 * time.Second)
	}
	cache.Del(0, worker.TourFlushThreadDataCounter)
}

func (worker *ManagerBase) WaitFinish() {
	<- worker.FinishChanel
}

func  (worker *ManagerBase) LoadWorkerConfig(env_config_file_name string) {
	config_file := os.Getenv(env_config_file_name)
	if config_file == "" {
		log.Error.Fatalf("Map tours worker config file name required (%s environment)", EnvWorkerFileConfig)
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
