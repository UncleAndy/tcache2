package cache

import (
	"github.com/uncleandy/tcache2/types"
	"gopkg.in/redis.v4"
	"github.com/fellah/tcache/log"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var redis_mode *types.RedisMode

func Read_settings(file_name string) {
	dat, err := ioutil.ReadFile(file_name)
	if err != nil {
		log.Error.Fatalln(err)
	}

	redis_mode = &types.RedisMode{}

	err = yaml.Unmarshal(dat, redis_mode)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}

func Redis_init() {
	redis_servers_connect(&redis_mode.MainServers)

	if redis_mode.ReconfigureMode {
		redis_servers_connect(&redis_mode.NewServers)
	}
}

func redis_servers_connect(servers *[]types.RedisServer) {
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
