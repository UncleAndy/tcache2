package main

import (
	"os"
	"github.com/uncleandy/tcache2/cache"
)

func main() {
	redis_yaml_file := os.Getenv("REDIS_CONFIG")

	cache.Read_settings(redis_yaml_file)
	cache.Redis_init()
}