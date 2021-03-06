package map_tours_db_worker

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"os"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/db"
)

const (
	EnvWorkerFileConfig = "MAP_TOURS_DB_WORKER_CONFIG"
	MapTourInsertThreadQueueTemplate = "map_tours_insert_%d"
	MapTourUpdateThreadQueueTemplate = "map_tours_update_%d"
	MapTourDeleteThreadQueueTemplate = "map_tours_delete_%d"
	MapTourInsertThreadDataCounter = "map_tours_insert_counter"
	MapTourUpdateThreadDataCounter = "map_tours_update_counter"
	MapTourDeleteThreadDataCounter = "map_tours_delete_counter"
)

type MapTourRedisReader struct {
}

type MapTourDbSQLAction struct {
}

func (worker *MapToursDbWorker) Init() {
	worker.LoadWorkerConfig()
	worker.FinishChanel = make(chan bool)

	worker.DbSQLAction = MapTourDbSQLAction{}
	worker.RedisTourReader = MapTourRedisReader{}

	worker.DbPool = make([]db.DbConnection, worker.Settings.WorkerThreadsCount)

	worker.RunStatisticLoop()
}

func (worker *MapToursDbWorker) WaitFinish() {
	<- worker.FinishChanel
}

func  (worker *MapToursDbWorker) LoadWorkerConfig() {
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

	err = yaml.Unmarshal(dat, &worker.Settings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}

func (worker *MapToursDbWorker) GetSettings() *worker_base.WorkerSettings {
	return &worker.Settings
}
