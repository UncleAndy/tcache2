package cache

import (
	"gopkg.in/redis.v4"
)

func Get(shard_index int, key string) (string, error) {
	main_server := main_shard_server(shard_index)
	val, err := main_server.Connection.Get(key).Result()

	if err == redis.Nil && RedisSettings.ReconfigureMode {
		old_server := old_shard_server(shard_index)
		if !servers_equals(main_server, old_server) {
			val, err = old_server.Connection.Get(key).Result()
			if err == nil {
				// Move to main
				main_server.Connection.Set(key, val, 0)
				old_server.Connection.Del(key)
			}
		}
	}

	return val, err
}

func Set(shard_index int, key string, val string) error {
	main_server := main_shard_server(shard_index)
	_, err := main_server.Connection.Set(key, val, 0).Result()

	if err == nil && RedisSettings.ReconfigureMode {
		old_server := old_shard_server(shard_index)
		if !servers_equals(main_server, old_server) {
			old_server.Connection.Del(key)
		}
	}

	return err
}

func RPush(shard_index int, key string, val string) error {
	main_server := main_shard_server(shard_index)
	if RedisSettings.ReconfigureMode {
		// Check exists key and if not exists - copy from old
		old_server := old_shard_server(shard_index)
		if !servers_equals(main_server, old_server) &&
			!main_server.Connection.Exists(key).Val() &&
			old_server.Connection.Exists(key).Val() {
			old_list, err := old_server.Connection.LRange(key, 0, -1).Result()
			if err == nil {
				for _, s := range old_list {
					main_server.Connection.RPush(key, s)
				}
			}
		}
	}

	_, err := main_server.Connection.RPush(key, val).Result()

	if err == nil && RedisSettings.ReconfigureMode {
		old_server := old_shard_server(shard_index)
		if !servers_equals(main_server, old_server) {
			old_server.Connection.Del(key)
		}
	}

	return err
}

func LPop(shard_index int, key string) (string, error) {
	main_server := main_shard_server(shard_index)
	val, err := main_server.Connection.LPop(key).Result()

	if err == redis.Nil && RedisSettings.ReconfigureMode {
		old_server := old_shard_server(shard_index)
		if !servers_equals(main_server, old_server) {
			val, err = old_server.Connection.LPop(key).Result()
		}
	}

	return val, err
}

func LRange(shard_index int, key string, start int64, finish int64) ([]string, error) {
	main_server := main_shard_server(shard_index)
	val, err := main_server.Connection.LRange(key, start, finish).Result()

	if (err == redis.Nil || len(val) == 0) && RedisSettings.ReconfigureMode {
		old_server := old_shard_server(shard_index)
		if !servers_equals(main_server, old_server) {
			val, err = old_server.Connection.LRange(key, start, finish).Result()
			if err == nil {
				// Move to main
				old_list, err1 := old_server.Connection.LRange(key, 0, -1).Result()
				if err1 == nil {
					for _, s := range old_list {
						main_server.Connection.RPush(key, s)
					}
				}
				old_server.Connection.Del(key)
			}
		}
	}

	return val, err
}

func GenID(key string) (int64, error) {
	id, err := RedisSettings.MainServers[0].Connection.Incr(key).Result()
	return id, err
}
