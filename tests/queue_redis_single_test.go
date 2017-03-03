package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"gopkg.in/redis.v4"
)

func TestSingleQueueSizesUpdateAllEmpty(t *testing.T) {
	init_test_redis_single()

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

func TestSingleQueueSizesUpdateAllValued(t *testing.T) {
	init_test_redis_single()

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "1")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "3")

	cache.QueueSizesUpdateAll("test_queue1")

	if cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"] != 3 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server MainServer[0]: expect 3, got %d",
			cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"],
		)
	}

	cache.RedisSettings.MainServers[0].Connection.Del("test_queue1")
}

func TestSingleAddQueueEmpty(t *testing.T) {
	init_test_redis_single()

	cache.AddQueue("test_queue1", "Value1")
	cache.AddQueue("test_queue1", "Value2")
	cache.AddQueue("test_queue1", "Value3")

	for i, server := range cache.RedisSettings.MainServers {
		if server.QueueSizes["test_queue1"] != 3 {
			t.Errorf(
				"Wrong queue 'test_queue1' size for server MainServer[%d]: expect 3, got %d",
				i, server.QueueSizes["test_queue1"],
			)
		}
	}

	cache.AddQueue("test_queue1", "Value1")
	cache.AddQueue("test_queue1", "Value2")
	cache.AddQueue("test_queue1", "Value3")

	for i, server := range cache.RedisSettings.MainServers {
		if server.QueueSizes["test_queue1"] != 6 {
			t.Errorf(
				"Wrong queue 'test_queue1' size for server MainServer[%d]: expect 6, got %d",
				i, server.QueueSizes["test_queue1"],
			)
		}
	}

	len0, err0 := cache.RedisSettings.MainServers[0].Connection.LLen("test_queue1").Result()

	if err0 != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' from MainServer[0]: %s",
			err0,
		)
	} else if len0 != 6 {
		t.Errorf(
			"Wrong queue 'test_queue1' real size for server MainServer[%d]: expect 6, got %d",
			0, len0,
		)
	}

	cache.CleanQueue("test_queue1")
}

func TestSingleAddQueueWithSizesUpdate(t *testing.T) {
	init_test_redis_single()

	for i := 0; i < (cache.MaxOperationsBeforeQueueUpdates - 10); i++ {
		cache.AddQueue("test_queue1", "Val")
	}

	cache.RedisSettings.MainServers[0].Connection.Del("test_queue1")

	for i := 0; i < 20; i++ {
		cache.AddQueue("test_queue1", "Val")
	}

	if cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"] >= 21 {
		t.Error(
			"QueueSizes not updated. Expected < 21, got:",
			cache.RedisSettings.MainServers[0].QueueSizes["test_queue1"],
		)
	}

	cache.CleanQueue("test_queue1")
}

func TestSingleGetQueueFromMain(t *testing.T) {
	init_test_redis_single()

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value3")

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

	if err != redis.Nil && err != nil && err.Error() != "No data in queue" {
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


func TestSingleGetQueueBatchFromMain(t *testing.T) {
	init_test_redis_single()

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value3")

	val, err := cache.GetQueueBatch("test_queue1", 10)

	if err != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' over GetQueueBatch: %s",
			err,
		)
	} else if len(val) != 3 {
		t.Errorf(
			"Wrong list length from 'test_queue1' over GetQueueBatch: expected 3, got '%d'",
			len(val),
		)
	} else if val[0] != "Value1" {
		t.Errorf(
			"Wrong data read [0] from 'test_queue1' over GetQueueBatch: expected 'Value1', got '%s'",
			val[0],
		)
	} else if val[1] != "Value2" {
		t.Errorf(
			"Wrong data read [1] from 'test_queue1' over GetQueueBatch: expected 'Value2', got '%s'",
			val[1],
		)
	} else if val[2] != "Value3" {
		t.Errorf(
			"Wrong data read [2] from 'test_queue1' over GetQueueBatch: expected 'Value3', got '%s'",
			val[2],
		)
	}

	val, err = cache.GetQueueBatch("test_queue1", 10)

	if err != redis.Nil && err != nil && err.Error() != "No data in queue" {
		t.Errorf(
			"Can not read empty queue 'test_queue1' over GetQueueBatch: %s",
			err,
		)
	}
	if len(val) != 0 {
		t.Errorf(
			"Wrong list length from empty 'test_queue1' over GetQueueBatch: expected 0, got '%d'",
			len(val),
		)
	}

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value3")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value4")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value5")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value6")

	val, err = cache.GetQueueBatch("test_queue1", 4)

	if err != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' over GetQueueBatch: %s",
			err,
		)
	} else if len(val) != 4 {
		t.Errorf(
			"Wrong list length from 'test_queue1' over GetQueueBatch: expected 4, got '%d'",
			len(val),
		)
	} else if val[0] != "Value1" {
		t.Errorf(
			"Wrong data read [0] from 'test_queue1' over GetQueueBatch: expected 'Value1', got '%s'",
			val[0],
		)
	} else if val[1] != "Value2" {
		t.Errorf(
			"Wrong data read [1] from 'test_queue1' over GetQueueBatch: expected 'Value2', got '%s'",
			val[1],
		)
	} else if val[2] != "Value3" {
		t.Errorf(
			"Wrong data read [2] from 'test_queue1' over GetQueueBatch: expected 'Value3', got '%s'",
			val[2],
		)
	} else if val[3] != "Value4" {
		t.Errorf(
			"Wrong data read [3] from 'test_queue1' over GetQueueBatch: expected 'Value4', got '%s'",
			val[3],
		)
	}

	val, err = cache.GetQueueBatch("test_queue1", 4)

	if err != nil {
		t.Errorf(
			"Can not read queue 'test_queue1' over GetQueueBatch: %s",
			err,
		)
	} else if len(val) != 2 {
		t.Errorf(
			"Wrong list length from 'test_queue1' over GetQueueBatch: expected 2, got '%d'",
			len(val),
		)
	} else if val[0] != "Value5" {
		t.Errorf(
			"Wrong data read [0] from 'test_queue1' over GetQueueBatch: expected 'Value5', got '%s'",
			val[0],
		)
	} else if val[1] != "Value6" {
		t.Errorf(
			"Wrong data read [1] from 'test_queue1' over GetQueueBatch: expected 'Value6', got '%s'",
			val[1],
		)
	}


	cache.CleanQueue("test_queue1")
}

func TestSingleCleanQueue(t *testing.T) {
	init_test_redis_single()

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue1", "Value3")

	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue2", "Value1")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue2", "Value2")
	cache.RedisSettings.MainServers[0].Connection.RPush("test_queue2", "Value3")

	cache.CleanQueue("test_queue1")

	len1, err1 := cache.RedisSettings.MainServers[0].Connection.LLen("test_queue1").Result()
	len3, err3 := cache.RedisSettings.MainServers[0].Connection.LLen("test_queue2").Result()

	if err1 != redis.Nil && len1 != 0 {
		t.Errorf(
			"Wrong queue 'test_queue1' length on MainServer[1] after clean: %d",
			len1,
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
