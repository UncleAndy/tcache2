package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"reflect"
)

func init_test_redis_single() {
	cache.RedisSettings = nil
	cache.ReadSettings("redis_test_main_mode_single.yaml")
	cache.RedisInit()
}

func TestSingleCacheSetSimple(t *testing.T) {
	init_test_redis_single()

	cache.Set(0, "test_key1", "Value1")
	cache.Set(1, "test_key2", "Value2")
	cache.Set(2, "test_key3", "Value3")

	val0, err0 := cache.RedisSettings.MainServers[0].Connection.Get("test_key1").Result()
	val1, err1 := cache.RedisSettings.MainServers[0].Connection.Get("test_key2").Result()
	val2, err2 := cache.RedisSettings.MainServers[0].Connection.Get("test_key3").Result()

	if err0 != nil {
		t.Error("Can not read 'test_key1' from MainServer[0]")
	}
	if err1 != nil {
		t.Error("Can not read 'test_key2' from MainServer[1]")
	}
	if err2 != nil {
		t.Error("Can not read 'test_key3' from MainServer[2]")
	}

	if val0 != "Value1" {
		t.Error("MainServer[0] 'test_key1' value expected 'Value1', got '", val0, "'")
	}
	if val1 != "Value2" {
		t.Error("MainServer[1] 'test_key2' value expected 'Value2', got '", val1, "'")
	}
	if val2 != "Value3" {
		t.Error("MainServer[2] 'test_key3' value expected 'Value3', got '", val2, "'")
	}

	cache.RedisSettings.MainServers[0].Connection.Del("test_key1")
	cache.RedisSettings.MainServers[0].Connection.Del("test_key2")
	cache.RedisSettings.MainServers[0].Connection.Del("test_key3")
}

func TestSingleCacheGetSimple(t *testing.T) {
	init_test_redis_single()

	cache.Set(0, "test_key1", "Value1")
	cache.Set(1, "test_key2", "Value2")
	cache.Set(2, "test_key3", "Value3")

	val0, err0 := cache.Get(0, "test_key1")
	val1, err1 := cache.Get(1, "test_key2")
	val2, err2 := cache.Get(2, "test_key3")

	if err0 != nil {
		t.Error("Can not read 'test_key1' from MainServer[0]")
	}
	if err1 != nil {
		t.Error("Can not read 'test_key2' from MainServer[1]")
	}
	if err2 != nil {
		t.Error("Can not read 'test_key3' from MainServer[2]")
	}

	if val0 != "Value1" {
		t.Error("MainServer[0] 'test_key1' value expected 'Value1', got '", val0, "'")
	}
	if val1 != "Value2" {
		t.Error("MainServer[1] 'test_key2' value expected 'Value2', got '", val1, "'")
	}
	if val2 != "Value3" {
		t.Error("MainServer[2] 'test_key3' value expected 'Value3', got '", val2, "'")
	}

	cache.RedisSettings.MainServers[0].Connection.Del("test_key1")
}

func TestSingleRPushSimple(t *testing.T) {
	init_test_redis_single()

	cache.RPush(0, "arr_test_key1", "Value1")
	cache.RPush(0, "arr_test_key1", "Value2")
	cache.RPush(0, "arr_test_key1", "Value3")

	cache.RPush(2, "arr_test_key2", "Value4")
	cache.RPush(2, "arr_test_key2", "Value5")
	cache.RPush(2, "arr_test_key2", "Value6")

	val0, err0 := cache.RedisSettings.MainServers[0].Connection.LRange("arr_test_key1", 0, -1).Result()
	val1, err1 := cache.RedisSettings.MainServers[0].Connection.LRange("arr_test_key2", 0, -1).Result()

	if err0 != nil {
		t.Error("Can not read 'arr_test_key1' from MainServer[0]: ", err0)
	}
	if reflect.TypeOf(val0).String() != "[]string" {
		t.Error("Wrong type of 'arr_test_key1' from MainServer[0] (no string array)")
	}
	if len(val0) != 3 {
		t.Error("Wrong size of array 'arr_test_key1': expected 3, got", len(val0))
	}
	if val0[0] != "Value1" || val0[1] != "Value2" || val0[2] != "Value3" {
		t.Errorf("Wrong data in array 'arr_test_key1': expected ['Valume1','Valume2','Valume3'], got %+v", val0)
	}

	if err1 != nil {
		t.Error("Can not read 'arr_test_key2' from MainServer[2]: ", err1)
	}
	if reflect.TypeOf(val1).String() != "[]string" {
		t.Error("Wrong type of 'arr_test_key2' from MainServer[2] (no string array)")
	} else if len(val1) != 3 {
		t.Error("Wrong size of array 'arr_test_key2': expected 3, got", len(val1))
	} else if val1[0] != "Value4" || val1[1] != "Value5" || val1[2] != "Value6" {
		t.Errorf("Wrong data in array 'arr_test_key2': expected ['Valume4','Valume5','Valume6'], got %+v", val1)
	}

	cache.RedisSettings.MainServers[0].Connection.Del("arr_test_key1")
	cache.RedisSettings.MainServers[0].Connection.Del("arr_test_key2")
}

func TestSingleLPopSimple(t *testing.T) {
	init_test_redis_single()

	cache.RedisSettings.MainServers[0].Connection.RPush("arr_test_key1", "Value1")
	cache.RedisSettings.MainServers[0].Connection.RPush("arr_test_key1", "Value2")

	val, err := cache.LPop(5, "arr_test_key1")

	val0, err0 := cache.RedisSettings.MainServers[0].Connection.LRange("arr_test_key1", 0, -1).Result()

	if err != nil {
		t.Error("Can not read list element 'arr_test_key1' from MainServer[2]: ", err)
	}
	if val != "Value1" {
		t.Error("MainServer[2] LPop('test_key1') value expected 'Value1', got '", val, "'")
	}

	if err0 != nil {
		t.Error("Can not read list 'arr_test_key1' from MainServer[2]: ", err0)
	}
	if len(val0) != 1 {
		t.Error("Wrong final list lenght: expected 1, got", len(val0))
	} else if val0[0] != "Value2" {
		t.Error("Wrong final list content[0]: expected 'Value2', got", val0[0])
	}

	cache.RedisSettings.MainServers[0].Connection.Del("arr_test_key1")
}

func TestSingleLRangeSimple(t *testing.T) {
	init_test_redis_single()

	cache.RedisSettings.MainServers[0].Connection.RPush("arr_test_key1", "Value1")
	cache.RedisSettings.MainServers[0].Connection.RPush("arr_test_key1", "Value2")
	cache.RedisSettings.MainServers[0].Connection.RPush("arr_test_key1", "Value3")

	val, err := cache.LRange(0, "arr_test_key1", 0, -1)

	if err != nil {
		t.Error("Can not read list 'arr_test_key1' from MainServers[0] over LRange: ", err)
	}
	if reflect.TypeOf(val).String() != "[]string" {
		t.Error("Wrong type of 'arr_test_key1' from MainServer[0] (no string array)")
	} else if len(val) != 3 {
		t.Error("Wrong size of array 'arr_test_key1': expected 3, got", len(val))
	} else if val[0] != "Value1" || val[1] != "Value2" || val[2] != "Value3" {
		t.Errorf("Wrong data in array 'arr_test_key1': expected ['Valume1','Valume2','Valume3'], got %+v", val)
	}

	cache.RedisSettings.MainServers[0].Connection.Del("arr_test_key1")
}
