package cache

import "github.com/uncleandy/tcache2/types"

func main_shard_server(shard_index int) types.RedisServer {
	return shard_server(shard_index, RedisSettings.MainServers)
}

func old_shard_server(shard_index int) types.RedisServer {
	return shard_server(shard_index, RedisSettings.OldServers)
}

func shard_server(shard_index int, servers []types.RedisServer) types.RedisServer {
	server_index := shard_index % len(servers)
	return servers[server_index]
}
