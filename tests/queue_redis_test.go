package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"gopkg.in/redis.v4"
)

func TestQueueSizesUpdateAllEmpty(t *testing.T) {
	init_test_redis()

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
	init_test_redis()

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

	if cache.RedisSettings.OldServers[0].QueueSizes["test_queue1"] != 0 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server OldServer[0]: expect 0, got %d",
			cache.RedisSettings.OldServers[0].QueueSizes["test_queue1"],
		)
	}
	if cache.RedisSettings.OldServers[1].QueueSizes["test_queue1"] != 0 {
		t.Errorf(
			"Wrong queue 'test_queue1' size for server OldServer[1]: expect 0, got %d",
			cache.RedisSettings.OldServers[1].QueueSizes["test_queue1"],
		)
	}

	cache.RedisSettings.MainServers[0].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[1].Connection.Del("test_queue1")
	cache.RedisSettings.MainServers[2].Connection.Del("test_queue1")
}

func TestAddQueueEmpty(t *testing.T) {
	init_test_redis()

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

func TestAddQueueWithSizesUpdate(t *testing.T) {
	init_test_redis()

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

func TestGetQueueFromMain(t *testing.T) {
	init_test_redis()

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

func TestGetQueueFromOld(t *testing.T) {
	init_test_redis()

	cache.RedisSettings.OldServers[1].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.OldServers[1].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.OldServers[1].Connection.RPush("test_queue1", "Value3")

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

func TestCleanQueue(t *testing.T) {
	init_test_redis()

	cache.RedisSettings.OldServers[1].Connection.RPush("test_queue1", "Value1")
	cache.RedisSettings.OldServers[1].Connection.RPush("test_queue1", "Value2")
	cache.RedisSettings.OldServers[1].Connection.RPush("test_queue1", "Value3")

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

	len0, err0 := cache.RedisSettings.OldServers[1].Connection.LLen("test_queue1").Result()
	len1, err1 := cache.RedisSettings.MainServers[1].Connection.LLen("test_queue1").Result()
	len2, err2 := cache.RedisSettings.MainServers[2].Connection.LLen("test_queue1").Result()
	len3, err3 := cache.RedisSettings.MainServers[2].Connection.LLen("test_queue2").Result()

	if err0 != redis.Nil && len0 != 0 {
		t.Errorf(
			"Wrong queue 'test_queue1' length on OldServer[1] after clean: %d",
			len0,
		)
	}
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
