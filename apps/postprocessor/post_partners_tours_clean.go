package main

import (
	"github.com/uncleandy/tcache2/cache"
	"strings"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"strconv"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/apps/postprocessor/post_partners_tours"
	"github.com/uncleandy/tcache2/log"
)

var (
	worker *post_partners_tours.PostPartnersToursWorker
)

// Sync prices with price logs

func main() {
	cache.InitFromEnv()
	db.Init()

	worker = &post_partners_tours.PostPartnersToursWorker{}

	worker.Init()
	worker.InitThreads()

	PartnersManagerLoop()

	worker.WaitFinish()
}

func PartnersManagerLoop() {
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

