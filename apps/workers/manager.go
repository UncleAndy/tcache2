package main

import (
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/db"
	"os"
	"syscall"
	"os/signal"
	"github.com/uncleandy/tcache2/log"
	"time"
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"sync"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

const (
	EnvManagerFileConfig = "WORKERS_MANAGER_CONFIG"
)

var (
	ForceStopManagerLoop = false
	ToursBatchSize = int64(1000)
	ManagerSettings worker_base.WorkerSettings
)

func InitManagers() {
	worker_base.Workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}
}

func InitManagersConfigs() {
	for _, worker := range worker_base.Workers {
		worker.LoadWorkerConfig()
		worker.LoadDictData()
	}
}

func SignalsInit() (chan os.Signal) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	return sigChan
}


func SignalsProcess(signals chan os.Signal) {
	<- signals

	log.Info.Println("Detect stop command. Please, wait...")

	ForceStopManagerLoop = true
}

func main() {
	signals := SignalsInit()
	go SignalsProcess(signals)

	cache.InitFromEnv()
	cache.RedisInit()
	db.Init()

	InitManagers()
	InitManagersConfigs()

	LoadManagerConfig()
	ManagerLoop()

	log.Info.Println("Finished")
}

func ManagerLoop() {
	println("Main loop start...")

	wg := sync.WaitGroup{}
	wg.Add(ManagerSettings.WorkerThreadsCount)

	for i := 0; i < ManagerSettings.WorkerThreadsCount; i++ {
		go func() {
			for !ForceStopManagerLoop {
				// Scan Redis tours loader queue & move tours to worker threads Redis queue
				tours, err := cache.GetQueueBatch(sletat.LoaderQueueToursName, ToursBatchSize)
				if err != nil || len(tours) == 0 {
					time.Sleep(1 * time.Second)
					continue
				}

				for _, tour_str := range tours {
					for _, worker := range worker_base.Workers {
						worker.SendTour(tour_str)
					}
				}
			}

			wg.Done()
		}()
	}

	wg.Wait()
	println("Main loop stoped.")
}

func  LoadManagerConfig() {
	config_file := os.Getenv(EnvManagerFileConfig)
	if config_file == "" {
		log.Error.Fatalf("Partners tours worker config file name required (%s environment)", EnvManagerFileConfig)
	}
	_, err := os.Stat(config_file)
	if os.IsNotExist(err) {
		log.Error.Fatalf("Partners tours worker config file '%s' not exists.", config_file)
	}

	dat, err := ioutil.ReadFile(config_file)
	if err != nil {
		log.Error.Fatalln(err)
	}

	err = yaml.Unmarshal(dat, &ManagerSettings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}
