package partners_tours

import (
	"os"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/uncleandy/tcache2/log"
)

const (
	EnvWorkerFileConfig = "PARTNER_TOURS_WORKER_CONFIG"
)

type PartnersToursWorkerSettings struct {
	WorkerFirstThreadId 	int		`yaml:"worker_first_thread_id"`
	WorkerThreadsCount 	int		`yaml:"worker_threads_count"`
	AllThreadsCount 	int		`yaml:"all_threads_count"`
}

type PartnersToursWorker struct {
	Settings PartnersToursWorkerSettings
	FinishChanel chan bool
}

func (worker *PartnersToursWorker) Init() {
	worker.LoadDictData()
	worker.LoadToursData()
	worker.LoadWorkerConfig()

	worker.FinishChanel = make(chan bool)
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
