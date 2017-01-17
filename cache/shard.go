package cache

func main_shard_server(shard_index uint64) RedisServer {
	return shard_server(shard_index, RedisSettings.MainServers)
}

func old_shard_server(shard_index uint64) RedisServer {
	return shard_server(shard_index, RedisSettings.OldServers)
}

func shard_server(shard_index uint64, servers []RedisServer) RedisServer {
	server_index := shard_index % uint64(len(servers))
	return servers[server_index]
}
