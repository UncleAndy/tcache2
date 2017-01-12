package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
)

func TestQueueSizesUpdateAllEmpty(t *testing.T) {
	init_redis()

	cache.QueueSizesUpdateAll("test_queue1")

	for i, server := range cache.RedisSettings.MainServers {
		if server.QueueSizes["test_queue1"] != 0 {
			t.Errorf(
				"Wrong queue 'test_queue1' size for server MainServer[%d]: expect 0, got %d",
				i, server.QueueSizes["test_queue1"],
			)
		}
	}

	for i, server := range cache.RedisSettings.OldServers {
		if server.QueueSizes["test_queue1"] != 0 {
			t.Errorf(
				"Wrong queue 'test_queue1' size for server OldServer[%d]: expect 0, got %d",
				i, server.QueueSizes["test_queue1"],
			)
		}
	}
}

func TestQueueSizesUpdateAllValued(t *testing.T) {
	init_redis()

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "1")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "3")

	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "4")

	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue1", "5")
	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue1", "6")

	cache.QueueSizesUpdateAll("test_queue1")

	if cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"] != 3 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server MainServer[0]: expect 3, got %d",
			cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"],
		)
	}
	if cache.RedisSettings.MainServers[1].QueueSizes["test_queue1"] != 1 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server MainServer[1]: expect 1, got %d",
			cache.RedisSettings.MainServers[1].QueueSizes["test_queue1"],
		)
	}
	if cache.RedisSettings.MainServers[2].QueueSizes["test_queue1"] != 2 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server MainServer[2]: expect 2, got %d",
			cache.RedisSettings.MainServers[2].QueueSizes["test_queue1"],
		)
	}

	if cache.RedisSettings.OldServers[0].QueueSizes["test_queue1"] != 3 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server OldServer[0]: expect 3, got %d",
			cache.RedisSettings.OldServers[0].QueueSizes["test_queue1"],
		)
	}
	if cache.RedisSettings.OldServers[1].QueueSizes["test_queue1"] != 1 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server OldServer[1]: expect 1, got %d",
			cache.RedisSettings.OldServers[1].QueueSizes["test_queue1"],
		)
	}

	cache.RedisSettings.MainServers[0].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[1].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[2].Connection.Del("test_queue1")
}
