package main

import (
	"os"
	"github.com/uncleandy/tcache2/cache"
)

func main() {
	redis_yaml_file := os.Getenv("REDIS_CONFIG")

	cache.ReadSettings(redis_yaml_file)
	cache.RedisInit()
}