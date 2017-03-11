package main

import (
	"github.com/uncleandy/tcache2/cache"
	"strings"
	"strconv"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/apps/postprocessor/post_partners_tours"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
)

var (
	worker *post_partners_tours.PostPartnersToursWorker
	ManagerKeysCount = 0
)

// Sync prices with price logs

func main() {
	cache.InitFromEnv()
	cache.RedisInit()
	db.Init()

	worker = &post_partners_tours.PostPartnersToursWorker{}

	worker.Init()
	worker.InitThreads()

	PartnersManagerLoop()

	worker.WaitFinish()
	log.Info.Println("Manager keys count:", ManagerKeysCount)
	log.Info.Println("Workers keys count:", post_partners_tours.WorkerKeysCount, "(bads:", post_partners_tours.BadToursForAnalyze, ")")
	log.Info.Println("Deleted keys count:", post_partners_tours.DeletedKeysCount)
}

func PartnersManagerLoop() {
	keys_scan_name := strings.Replace(partners_tours.PartnersTourKeyDataKeyTemplate, "%d", "*", -1)

	log.Info.Println("Scan for redis: ", keys_scan_name)
	scanner := &cache.KeysScanner{}
	scanner.Init(keys_scan_name, 1000)
	for !scanner.Finished {
		id_keys := scanner.Next()
		for _, id_key_str := range id_keys {
			key_split := strings.Split(id_key_str, ":")
			id, err := strconv.ParseUint(key_split[1], 10, 64)
			if err != nil {
				post_partners_tours.BadToursForAnalyze++
				log.Error.Print("Error parse ID from key ", id_key_str)
			} else {
				ManagerKeysCount++
				worker.SendTour(id)
			}
		}
	}
	worker.FinishThreads()
}

