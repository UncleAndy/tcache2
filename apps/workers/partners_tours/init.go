package partners_tours

import (
	"os"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
)

const (
	EnvWorkerFileConfig = "PARTNER_TOURS_WORKER_CONFIG"
)

type PartnersToursWorker struct {
	Settings worker_base.WorkerSettings
	FinishChanel chan bool
}

func (worker *PartnersToursWorker) Init() {
	worker.LoadDictData()
	worker.LoadToursData()
	worker.LoadWorkerConfig()
	worker.RunStatisticLoop()

	worker.FinishChanel = make(chan bool)
}

func (worker *PartnersToursWorker) GetSettings() *worker_base.WorkerSettings {
	return &worker.Settings
}

func (worker *PartnersToursWorker) WaitFinish() {
	<- worker.FinishChanel
}

func  (worker *PartnersToursWorker) LoadWorkerConfig() {
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
