package main

import (
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"github.com/uncleandy/tcache2/cache"
	"os"
	"github.com/uncleandy/tcache2/log"
)

const (
	EnvRedisFileConfig = "REDIS_CONFIG"
)

func redisInit() {
	redis_config_file := os.Getenv(EnvRedisFileConfig)
	if redis_config_file == "" {
		log.Error.Fatal("Redis config file name required (REDIS_CONFIG environment)")
	}
	_, err := os.Stat(redis_config_file)
	if os.IsNotExist(err) {
		log.Error.Fatalf("Redis config file '%s' not exists.", redis_config_file)
	}

	cache.ReadSettings(redis_config_file)
	cache.RedisInit()
}

func main() {
	redisInit()
	sletat.Init()
	sletat.MainLoop()
}