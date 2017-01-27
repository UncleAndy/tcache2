package main

import (
	"github.com/uncleandy/tcache2/apps/postprocessor/post_map_tours_price_logs"
	"strings"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/cache"
	"strconv"
	"github.com/fellah/tcache/log"
	"github.com/uncleandy/tcache2/db"
)

var (
	worker *post_map_tours_price_logs.PostMapToursWorker
)

// Sync prices with price logs

func main() {
	cache.InitFromEnv()
	db.Init()

	worker = &post_map_tours_price_logs.PostMapToursWorker{}

	worker.Init()
	worker.InitThreads()

	ManagerLoop()

	worker.WaitFinish()
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
				worker.SendTour(id)
			}
		}
	}
	worker.FinishThreads()
}
