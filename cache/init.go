package cache

import (
	"github.com/uncleandy/tcache2/types"
	"gopkg.in/redis.v4"
	"github.com/fellah/tcache/log"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var RedisSettings *types.RedisMode

func ReadSettings(file_name string) {
	dat, err := ioutil.ReadFile(file_name)
	if err != nil {
		log.Error.Fatalln(err)
	}

	RedisSettings = &types.RedisMode{}

	err = yaml.Unmarshal(dat, RedisSettings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}

func RedisInit() {
	redis_servers_connect(&RedisSettings.MainServers)

	if RedisSettings.ReconfigureMode {
		redis_servers_connect(&RedisSettings.OldServers)
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

func servers_equals(server1 types.RedisServer, server2 types.RedisServer) bool {
	return (server1.Addr == server2.Addr && server1.Db == server2.Db)
}
