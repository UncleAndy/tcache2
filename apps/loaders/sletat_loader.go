package main

import (
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"github.com/uncleandy/tcache2/cache"
)

func main() {
	cache.InitFromEnv()
	cache.RedisInit()
	sletat.Init()
	sletat.MainLoop()
}