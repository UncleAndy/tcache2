package main

import (
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
)

func main() {
	cache.InitFromEnv()
	db.Init()

	worker_base.RunWorkers()
	worker_base.RunManagerLoop()
	worker_base.WaitWorkersFinish()
}
