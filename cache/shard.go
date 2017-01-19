package cache

import "github.com/uncleandy/tcache2/log"

func main_shard_server(shard_index uint64) RedisServer {
	return shard_server(shard_index, RedisSettings.MainServers)
}

func old_shard_server(shard_index uint64) RedisServer {
	return shard_server(shard_index, RedisSettings.OldServers)
}

func shard_server(shard_index uint64, servers []RedisServer) RedisServer {
	if len(servers) <= 0 {
		log.Error.Fatalln("No redis servers in Settings.")
	}
	server_index := shard_index % uint64(len(servers))
	return servers[server_index]
}
