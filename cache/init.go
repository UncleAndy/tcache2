package cache

import (
	"gopkg.in/redis.v4"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sync"
	"os"
	"github.com/uncleandy/tcache2/log"
)

var RedisSettings *RedisMode

const (
	EnvRedisFileConfig = "REDIS_CONFIG"
)

func InitFromEnv() {
	redis_config_file := os.Getenv(EnvRedisFileConfig)
	if redis_config_file == "" {
		log.Error.Fatalf("Redis config file name required (%s environment)", EnvRedisFileConfig)
	}
	_, err := os.Stat(redis_config_file)
	if os.IsNotExist(err) {
		log.Error.Fatalf("Redis config file '%s' not exists.", redis_config_file)
	}

	ReadSettings(redis_config_file)
}

func ReadSettings(file_name string) {
	dat, err := ioutil.ReadFile(file_name)
	if err != nil {
		log.Error.Fatalln(err)
	}

	RedisSettings = &RedisMode{}

	err = yaml.Unmarshal(dat, RedisSettings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}

func RedisInit() {
	if len(RedisSettings.MainServers) <= 0 {
		log.Error.Fatalln("No main servers in config.")
	}

	redis_servers_connect(&RedisSettings.MainServers)
	queue_sizes_mutex_init(&RedisSettings.MainServers)
	redis_server_queue_sizes_init(&RedisSettings.MainServers)

	if RedisSettings.ReconfigureMode {
		if len(RedisSettings.OldServers) <= 0 {
			log.Error.Fatalln("No old servers in config for reconfigure mode.")
		}

		redis_servers_connect(&RedisSettings.OldServers)
		queue_sizes_mutex_init(&RedisSettings.OldServers)
		redis_server_queue_sizes_init(&RedisSettings.OldServers)
	}
}

func redis_servers_connect(servers *[]RedisServer) {
	for i, server := range *servers {
		(*servers)[i].Connection = redis.NewClient(&redis.Options{
			Addr:     	server.Addr,
			Password: 	server.Password,
			DB:       	server.Db,
		})

		_, err := (*servers)[i].Connection.Ping().Result()

		if err != nil {
			(*servers)[i].Connection = nil
			log.Error.Fatalln("Error connection to Redis server "+server.Addr)
		}
	}
}

func queue_sizes_mutex_init(servers *[]RedisServer) {
	for i, _ := range *servers {
		if (*servers)[i].QueueSizes == nil {
			(*servers)[i].QueueSizesMutex = &sync.Mutex{}
		}
	}
}

func redis_server_queue_sizes_init(servers *[]RedisServer) {
	for i, _ := range *servers {
		if (*servers)[i].QueueSizes == nil {
			(*servers)[i].QueueSizes = make(map[string]int64)
		}
	}
}

func servers_equals(server1 RedisServer, server2 RedisServer) bool {
	return (server1.Addr == server2.Addr && server1.Db == server2.Db)
}
