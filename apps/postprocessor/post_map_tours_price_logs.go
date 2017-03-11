package main

import (
	"github.com/uncleandy/tcache2/apps/postprocessor/post_map_tours_price_logs_lib"
	"strings"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/cache"
	"strconv"
	"github.com/fellah/tcache/log"
	"github.com/uncleandy/tcache2/db"
)

var (
	worker *post_map_tours_price_logs.PostMapToursWorker
	ManagerKeysCount = 0
)

// Sync prices with price logs

func main() {
	cache.InitFromEnv()
	cache.RedisInit()
	db.Init()

	worker = &post_map_tours_price_logs.PostMapToursWorker{}

	worker.Init()
	worker.RunStatisticLoop()
	worker.InitThreads()

	ManagerLoop()

	worker.WaitFinish()

	log.Info.Println("Manager processed keys: ", ManagerKeysCount)
	log.Info.Println("Workers processed keys: ", post_map_tours_price_logs.WorkerKeysProcessed)
	log.Info.Println("Workers skiped keys: ", post_map_tours_price_logs.WorkerKeysSkip)
	log.Info.Println("Workers updated keys: ", post_map_tours_price_logs.WorkerPricesUpdated)
	log.Info.Println("Workers deleted keys: ", post_map_tours_price_logs.WorkerKeysDeleted)
	log.Info.Println("Workers bad keys: ", post_map_tours_price_logs.WorkerKeysBad)
}

func ManagerLoop() {
	keys_scan_name := strings.Replace(map_tours.MapTourKeyDataKeyTemplate, "%d", "*", -1)

	scanner := &cache.KeysScanner{}
	scanner.Init(keys_scan_name, 1000)
	for !scanner.Finished {
		id_keys := scanner.Next()
		for _, id_key_str := range id_keys {
			key_split := strings.Split(id_key_str, ":")
			id, err := strconv.ParseUint(key_split[1], 10, 64)
			if err != nil {
				log.Error.Print("Error parse ID from key ", id_key_str)
			} else {
				ManagerKeysCount++
				worker.SendTour(id)
			}
		}
	}
	worker.FinishThreads()
}
