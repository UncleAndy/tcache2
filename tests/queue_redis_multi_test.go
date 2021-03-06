package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"gopkg.in/redis.v4"
)

func TestMultiQueueSizesUpdateAllEmpty(t *testing.T) {
	init_test_redis_multi()

	cache.QueueSizesUpdateAll("test_queue1")

	for i, server := range cache.RedisSettings.MainServers {
		if server.QueueSizes["test_queue1"] != 0 {
			t.Errorf(
				"Wrong queue 'test_queue1' size for server MainServer[%d]: expect 0, got %d",
				i, server.QueueSizes["test_queue1"],
			)
		}
	}
}

func TestMultiQueueSizesUpdateAllValued(t *testing.T) {
	init_test_redis_multi()

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

	cache.RedisSettings.MainServers[0].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[1].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[2].Connection.Del("test_queue1")
}

func TestMultiAddQueueEmpty(t *testing.T) {
	init_test_redis_multi()

	cache.AddQueue("test_queue1", "Value1")
	cache.AddQueue("test_queue1", "Value2")
	cache.AddQueue("test_queue1", "Value3")

	for i, server := range cache.RedisSettings.MainServers {
		if server.QueueSizes["test_queue1"] != 1 {
			t.Errorf(
				"Wrong queue 'test_queue1' size for server MainServer[%d]: expect 1, got %d",
				i, server.QueueSizes["test_queue1"],
			)
		}
	}

	cache.AddQueue("test_queue1", "Value1")
	cache.AddQueue("test_queue1", "Value2")
	cache.AddQueue("test_queue1", "Value3")

	for i, server := range cache.RedisSettings.MainServers {
		if server.QueueSizes["test_queue1"] != 2 {
			t.Errorf(
				"Wrong queue 'test_queue1' size for server MainServer[%d]: expect 2, got %d",
				i, server.QueueSizes["test_queue1"],
			)
		}
	}

	len0, err0 := cache.RedisSettings.MainServers[0].Connection.LLen("test_queue1").Result()
	len1, err1 := cache.RedisSettings.MainServers[1].Connection.LLen("test_queue1").Result()
	len2, err2 := cache.RedisSettings.MainServers[2].Connection.LLen("test_queue1").Result()

	if err0 != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' from MainServer[0]: %s",
			err0,
		)
	} else if len0 != 2 {
		t.Errorf(
			"Wrong queue 'test_queue1' real size for server MainServer[%d]: expect 2, got %d",
			0, len0,
		)
	}
	if err1 != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' from MainServer[1]: %s",
			err1,
		)
	} else if len1 != 2 {
		t.Errorf(
			"Wrong queue 'test_queue1' real size for server MainServer[%d]: expect 2, got %d",
			1, len1,
		)
	}
	if err2 != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' from MainServer[2]: %s",
			err2,
		)
	} else if len2 != 2 {
		t.Errorf(
			"Wrong queue 'test_queue1' real size for server MainServer[%d]: expect 2, got %d",
			2, len2,
		)
	}

	cache.CleanQueue("test_queue1")
}

func TestMultiAddQueueWithSizesUpdate(t *testing.T) {
	init_test_redis_multi()

	for i := 0; i < (cache.MaxOperationsBeforeQueueUpdates - 10); i++ {
		cache.AddQueue("test_queue1", "Val")
	}

	cache.RedisSettings.MainServers[0].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[1].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[2].Connection.Del("test_queue1")

	for i := 0; i < 20; i++ {
		cache.AddQueue("test_queue1", "Val")
	}

	if cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"] >= 10 {
		t.Error(
			"QueueSizes not updated. Expected < 10, got:",
			cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"],
		)
	}
	if cache.RedisSettings.MainServers[1].QueueSizes["test_queue1"] >= 10 {
		t.Error(
			"QueueSizes not updated. Expected < 10, got:",
			cache.RedisSettings.MainServers[1].QueueSizes["test_queue1"],
		)
	}
	if cache.RedisSettings.MainServers[2].QueueSizes["test_queue1"] >= 10 {
		t.Error(
			"QueueSizes not updated. Expected < 10, got:",
			cache.RedisSettings.MainServers[2].QueueSizes["test_queue1"],
		)
	}

	cache.CleanQueue("test_queue1")
}

func TestMultiGetQueueFromMain(t *testing.T) {
	init_test_redis_multi()

	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value3")

	val, err := cache.GetQueue("test_queue1")

	if err != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' over GetQueue: %s",
			err,
		)
	} else if val != "Value1" {
		t.Errorf(
			"Wrong data read from 'test_queue1' over GetQueue: expected 'Value1', got '%s'",
			val,
		)
	}

	val, err = cache.GetQueue("test_queue1")

	if err != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' over GetQueue: %s",
			err,
		)
	} else if val != "Value2" {
		t.Errorf(
			"Wrong data read from 'test_queue1' over GetQueue: expected 'Value2', got '%s'",
			val,
		)
	}

	val, err = cache.GetQueue("test_queue1")

	if err != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' over GetQueue: %s",
			err,
		)
	} else if val != "Value3" {
		t.Errorf(
			"Wrong data read from 'test_queue1' over GetQueue: expected 'Value3', got '%s'",
			val,
		)
	}

	val, err = cache.GetQueue("test_queue1")

	if err != redis.Nil && err.Error() != "No data in queue" {
		t.Errorf(
			"Can not read queue 'test_queue1' over GetQueue: %s",
			err,
		)
	}
	if val != "" {
		t.Errorf(
			"Wrong data read from 'test_queue1' over GetQueue: expected '', got '%s'",
			val,
		)
	}

	cache.CleanQueue("test_queue1")
}

func TestMultiCleanQueue(t *testing.T) {
	init_test_redis_multi()

	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value3")

	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue1", "Value3")

	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue2", "Value1")
	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue2", "Value2")
	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue2", "Value3")

	cache.CleanQueue("test_queue1")

	len1, err1 := cache.RedisSettings.MainServers[1].Connection.LLen("test_queue1").Result()
	len2, err2 := cache.RedisSettings.MainServers[2].Connection.LLen("test_queue1").Result()
	len3, err3 := cache.RedisSettings.MainServers[2].Connection.LLen("test_queue2").Result()

	if err1 != redis.Nil && len1 != 0 {
		t.Errorf(
			"Wrong queue 'test_queue1' length on MainServer[1] after clean: %d",
			len1,
		)
	}
	if err2 != redis.Nil && len2 != 0 {
		t.Errorf(
			"Wrong queue 'test_queue1' length on MainServer[2] after clean: %d",
			len2,
		)
	}

	if err3 != nil {
		t.Errorf(
			"Error read queue 'test_queue2' on MainServer[2] after clean: %s",
			err3,
		)
	} else if len3 != 3 {
		t.Errorf(
			"Wrong queue 'test_queue1' length on MainServer[2] after clean: expecetd 3, got %d",
			len3,
		)
	}

	cache.CleanQueue("test_queue2")
}

func TestMultiQueueSizeMethod(t *testing.T) {
	init_test_redis_multi()

	cache.CleanQueue("test_queue1")
	cache.CleanQueue("test_queue2")
	cache.CleanQueue("test_queue3")

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue1", "Value3")

	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value3")

	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue2", "Value1")
	cache.RedisSettings.MainServers[2].Connection.RPush("test_queue2", "Value2")
	cache.RedisSettings.MainServers[1].Connection.RPush("test_queue2", "Value3")

	size1 := cache.QueueSize("test_queue1")
	size2 := cache.QueueSize("test_queue2")
	size3 := cache.QueueSize("test_queue3")

	if size1 != 6 {
		t.Error("Wrong queue size. Expected 6, got", size1)
	}
	if size2 != 3 {
		t.Error("Wrong queue size. Expected 3, got", size2)
	}
	if size3 != 0 {
		t.Error("Wrong queue size. Expected 0, got", size3)
	}

	cache.CleanQueue("test_queue1")
	cache.CleanQueue("test_queue2")
	cache.CleanQueue("test_queue3")
}