package post_map_tours_price_logs

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"os"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/uncleandy/tcache2/log"
	"sync"
)

const (
	EnvWorkerFileConfig = "POST_MAP_TOURS_WORKER_CONFIG"
	MapTourUpdateMutexTemplate = "map_update_%d"
)

func (post_worker *PostMapToursWorker) Init() {
	post_worker.LoadWorkerConfig()

	post_worker.ToursChanel = make(chan uint64)
	post_worker.FinishWaitGroup = new(sync.WaitGroup)
	post_worker.FinishWaitGroup.Add(post_worker.Settings.WorkerThreadsCount)
}

func (post_worker *PostMapToursWorker) GetSettings() *worker_base.WorkerSettings {
	return &post_worker.Settings
}

func (post_worker *PostMapToursWorker) WaitFinish() {
	post_worker.FinishWaitGroup.Wait()
}

func  (post_worker *PostMapToursWorker) LoadWorkerConfig() {
	config_file := os.Getenv(EnvWorkerFileConfig)
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

	err = yaml.Unmarshal(dat, &post_worker.Settings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}
