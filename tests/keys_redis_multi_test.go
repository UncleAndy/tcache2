package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"fmt"
)

func TestRedisKeysScan(t *testing.T) {
	init_test_redis_multi()

	cache.Set(0, "test_key_redis_keys_scan01", "Value01")
	cache.Set(0, "test_key_redis_keys_scan02", "Value02")
	cache.Set(0, "test_key_redis_keys_scan03", "Value03")
	cache.Set(1, "test_key_redis_keys_scan11", "Value11")
	cache.Set(1, "test_key_redis_keys_scan12", "Value12")
	cache.Set(1, "test_key_redis_keys_scan13", "Value13")
	cache.Set(2, "test_key_redis_keys_scan21", "Value21")
	cache.Set(2, "test_key_redis_keys_scan22", "Value22")
	cache.Set(2, "test_key_redis_keys_scan23", "Value23")

	max_list_size := int64(2)

	test_res := make(map[string]bool)
	scanner := cache.KeysScanner{}
	scanner.Init("test_key_redis_keys_scan*", max_list_size)
	for !scanner.Finished {
		keys_list := scanner.Next()

		if int64(len(keys_list)) > max_list_size {
			t.Error(
				"Wrong KeyScanner Next results list size. Expected < ", max_list_size,
				", got", len(keys_list))
		}

		for _, key := range keys_list {
			test_res[key] = true
		}
	}

	if len(test_res) != 9 {
		t.Error("Wrong KeyScanner results count. Expected 9, got", len(test_res), "\n",
		fmt.Sprintf("%+v\n", test_res))
	}

	max_list_size = 5

	test_res2 := make(map[string]bool)
	scanner2 := cache.KeysScanner{}
	scanner2.Init("test_key_redis_keys_scan*", max_list_size)
	for !scanner2.Finished {
		keys_list := scanner2.Next()

		if int64(len(keys_list)) > max_list_size {
			t.Error(
				"Wrong KeyScanner Next results list size. Expected < ", max_list_size,
				", got", len(keys_list))
		}

		for _, key := range keys_list {
			test_res2[key] = true
		}
	}

	if len(test_res2) != 9 {
		t.Error("Wrong KeyScanner2 results count. Expected 9, got", len(test_res), "\n",
			fmt.Sprintf("%+v\n", test_res))
	}


	cache.RedisSettings.MainServers[0].Connection.Del("test_key_redis_keys_scan01")
	cache.RedisSettings.MainServers[0].Connection.Del("test_key_redis_keys_scan02")
	cache.RedisSettings.MainServers[0].Connection.Del("test_key_redis_keys_scan03")
	cache.RedisSettings.MainServers[1].Connection.Del("test_key_redis_keys_scan11")
	cache.RedisSettings.MainServers[1].Connection.Del("test_key_redis_keys_scan12")
	cache.RedisSettings.MainServers[1].Connection.Del("test_key_redis_keys_scan13")
	cache.RedisSettings.MainServers[2].Connection.Del("test_key_redis_keys_scan21")
	cache.RedisSettings.MainServers[2].Connection.Del("test_key_redis_keys_scan22")
	cache.RedisSettings.MainServers[2].Connection.Del("test_key_redis_keys_scan23")
}
