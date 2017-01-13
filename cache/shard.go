package cache

func main_shard_server(shard_index int) RedisServer {
	return shard_server(shard_index, RedisSettings.MainServers)
}

func old_shard_server(shard_index int) RedisServer {
	return shard_server(shard_index, RedisSettings.OldServers)
}

func shard_server(shard_index int, servers []RedisServer) RedisServer {
	server_index := shard_index % len(servers)
	return servers[server_index]
}
