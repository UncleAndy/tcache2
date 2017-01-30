package partners_tours_db_worker

import (
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"os"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/uncleandy/tcache2/log"
)

const (
	EnvWorkerFileConfig = "PARTNERS_TOURS_DB_WORKER_CONFIG"
	PartnersTourInsertThreadQueueTemplate = "partners_tours_insert_%d"
	PartnersTourUpdateThreadQueueTemplate = "partners_tours_update_%d"
	PartnersTourDeleteThreadQueueTemplate = "partners_tours_delete_%d"
	PartnersTourInsertThreadDataCounter = "partners_tours_insert_counter"
	PartnersTourUpdateThreadDataCounter = "partners_tours_update_counter"
	PartnersTourDeleteThreadDataCounter = "partners_tours_delete_counter"
)

func (worker *PartnersToursDbWorker) Init() {
	worker.LoadWorkerConfig()
	worker.FinishChanel = make(chan bool)
}

func (worker *PartnersToursDbWorker) WaitFinish() {
	<- worker.FinishChanel
}

func  (worker *PartnersToursDbWorker) LoadWorkerConfig() {
	config_file := os.Getenv(EnvWorkerFileConfig)
	if config_file == "" {
		log.Error.Fatalf("Partners tours worker config file name required (%s environment)", EnvWorkerFileConfig)
	}
	_, err := os.Stat(config_file)
	if os.IsNotExist(err) {
		log.Error.Fatalf("Partners tours worker config file '%s' not exists.", config_file)
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

func (worker *PartnersToursDbWorker) GetSettings() *worker_base.WorkerSettings {
	return &worker.Settings
}
