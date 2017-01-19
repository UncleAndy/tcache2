package tests

import (
	"testing"
	"fmt"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"time"
	"github.com/uncleandy/tcache2/tours"
	"strconv"
	"gopkg.in/redis.v4"
)

func random_tour_map() *tours.TourMap {
	return &tours.TourMap{
		TourBase: *random_tour_base(),
	}
}

func TestMapToursThreadProcessSimple(t *testing.T) {
	init_test_redis_single()
	init_workers()

	tour1 := *random_tour_map()
	tour2 := tour1
	tour3 := tour1

	// Different KeyData required for test
	tour2.HotelId += 1
	tour2.Price += 1
	tour3.HotelId += 2
	tour3.Price += 2

	thread_index := 0
	thread_queue := fmt.Sprintf(map_tours.ThreadMapToursQueueTemplate, thread_index)

	cache.AddQueue(thread_queue, tour1.ToString())
	cache.AddQueue(thread_queue, tour2.ToString())
	cache.AddQueue(thread_queue, tour3.ToString())

	map_tours.ForceStopThreads = false
	worker_base.Workers[0].MainLoop()

	for !cache.IsEmptyQueue(thread_queue) {
		time.Sleep(TestWaitTime)
	}
	map_tours.ForceStopThreads = true
	time.Sleep(GoroutineFinishWaitTime)

	id_key_tour1 := fmt.Sprintf(map_tours.MapTourIDKeyTemplate, tour1.KeyData())
	id1str, err1 := cache.Get(tour1.KeyDataCRC32(), id_key_tour1)
	var id1 uint64
	var key_data1_key string
	var price_data1_key string
	if err1 != nil {
		t.Error("Can not read map tour ID data from", id_key_tour1, ". Error:", err1)
	} else {
		val1int, err1int := strconv.ParseUint(id1str, 10, 64)
		if err1int != nil || val1int <= 0 {
			t.Error("Bad ID value '", id1str,
				"' for map tour from", id_key_tour1,
				". Error:", err1int)
		}
		id1 = val1int

		key_data1_key = fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, id1)
		key_data1, err1kd := cache.Get(id1, key_data1_key)
		if err1kd != nil {
			t.Error("Can not read map tour KEY data from", key_data1_key, ". Error:", err1kd)
		} else if key_data1 != tour1.KeyData() {
			t.Error("Wrong KEY DATA map tour from", key_data1_key,
				". Expected:", tour1.KeyData(), ", got: ", key_data1)
		}

		price_data1_key = fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, id1)
		price_data1, err1pd := cache.Get(id1, price_data1_key)
		if err1pd != nil {
			t.Error("Can not read map tour PRICE data from", price_data1_key, ". Error:", err1pd)
		} else if price_data1 != tour1.PriceData() {
			t.Error("Wrong PRICE DATA map tour from", price_data1_key,
				". Expected:", tour1.PriceData(), ", got: ", price_data1)
		}
	}

	id_key_tour2 := fmt.Sprintf(map_tours.MapTourIDKeyTemplate, tour2.KeyData())
	id2str, err2 := cache.Get(tour2.KeyDataCRC32(), id_key_tour2)
	var id2 uint64
	var key_data2_key string
	var price_data2_key string
	if err2 != nil {
		t.Error("Can not read map tour ID data from", id_key_tour2, ". Error:", err2)
	} else {
		val2int, err2int := strconv.ParseUint(id2str, 10, 64)
		if err2int != nil || val2int <= 0 {
			t.Error("Bad ID value '", id2str,
				"' for map tour from", id_key_tour2,
				". Error:", err2int)
		}
		id2 = val2int

		key_data2_key = fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, id2)
		key_data2, err2kd := cache.Get(id2, key_data2_key)
		if err2kd != nil {
			t.Error("Can not read map tour KEY data from", key_data2_key, ". Error:", err2kd)
		} else if key_data2 != tour2.KeyData() {
			t.Error("Wrong KEY DATA map tour from", key_data2_key,
				". Expected:", tour1.KeyData(), ", got: ", key_data2)
		}

		price_data2_key = fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, id2)
		price_data2, err2pd := cache.Get(id2, price_data2_key)
		if err2pd != nil {
			t.Error("Can not read map tour PRICE data from", price_data2_key, ". Error:", err2pd)
		} else if price_data2 != tour2.PriceData() {
			t.Error("Wrong PRICE DATA map tour from", price_data2_key,
				". Expected:", tour2.PriceData(), ", got: ", price_data2)
		}
	}

	id_key_tour3 := fmt.Sprintf(map_tours.MapTourIDKeyTemplate, tour3.KeyData())
	id3str, err3 := cache.Get(tour3.KeyDataCRC32(), id_key_tour3)
	var id3 uint64
	var key_data3_key string
	var price_data3_key string
	if err3 != nil {
		t.Error("Can not read map tour ID data from", id_key_tour3, ". Error:", err3)
	} else {
		val3int, err3int := strconv.ParseUint(id3str, 10, 64)
		if err3int != nil || val3int <= 0 {
			t.Error("Bad ID value '", id3str,
				"' for map tour from", id_key_tour3,
				". Error:", err3int)
		}
		id3 = val3int

		key_data3_key = fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, id3)
		key_data3, err3kd := cache.Get(id3, key_data3_key)
		if err3kd != nil {
			t.Error("Can not read map tour KEY data from", key_data3_key, ". Error:", err3kd)
		} else if key_data3 != tour3.KeyData() {
			t.Error("Wrong KEY DATA map tour from", key_data3_key,
				". Expected:", tour3.KeyData(), ", got: ", key_data3)
		}

		price_data3_key = fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, id3)
		price_data3, err3pd := cache.Get(id3, price_data3_key)
		if err3pd != nil {
			t.Error("Can not read map tour PRICE data from", price_data3_key, ". Error:", err3pd)
		} else if price_data3 != tour3.PriceData() {
			t.Error("Wrong PRICE DATA map tour from", price_data3_key,
				". Expected:", tour3.PriceData(), ", got: ", price_data3)
		}
	}

	len_ins, err := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours.MapTourInsertQueue).Result()
	if err != nil {
		t.Error("Can not read map tour INSERT queue length:", err)
	} else if len_ins != 3 {
		t.Error("Wrong map tour INSERT queue length. Expected 3, got", len_ins)
	}

	len_upd, err := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours.MapTourUpdateQueue).Result()
	if err != nil && err != redis.Nil {
		t.Error("Can not read map tour UPDATE queue length:", err)
	} else if len_upd != 0 && err != redis.Nil {
		t.Error("Wrong map tour UPDATE queue length. Expected 0, got", len_upd)
	}


	cache.CleanQueue(thread_queue)
	cache.CleanQueue(map_tours.MapTourInsertQueue)
	cache.CleanQueue(map_tours.MapTourUpdateQueue)
	cache.Del(tour1.KeyDataCRC32(), id_key_tour1)
	cache.Del(id1, key_data1_key)
	cache.Del(id1, price_data1_key)
	cache.Del(tour2.KeyDataCRC32(), id_key_tour2)
	cache.Del(id2, key_data2_key)
	cache.Del(id2, price_data2_key)
	cache.Del(tour3.KeyDataCRC32(), id_key_tour3)
	cache.Del(id3, key_data3_key)
	cache.Del(id3, price_data3_key)
}

func TestMapToursThreadProcessPriceUpdate(t *testing.T) {
	init_test_redis_single()
	init_workers()

	tour1 := *random_tour_map()
	tour2 := tour1

	tour2.Price -= 1

	if tour1.KeyData() != tour2.KeyData() || tour1.PriceData() == tour2.PriceData() {
		t.Error("Wrong to initialize price for test.")
	}

	thread_index := 0
	thread_queue := fmt.Sprintf(map_tours.ThreadMapToursQueueTemplate, thread_index)

	cache.AddQueue(thread_queue, tour1.ToString())
	cache.AddQueue(thread_queue, tour2.ToString())

	map_tours.ForceStopThreads = false
	worker_base.Workers[0].MainLoop()

	for !cache.IsEmptyQueue(thread_queue) {
		time.Sleep(TestWaitTime)
	}
	map_tours.ForceStopThreads = true
	time.Sleep(GoroutineFinishWaitTime)

	id_key_tour1 := fmt.Sprintf(map_tours.MapTourIDKeyTemplate, tour1.KeyData())
	id1str, err1 := cache.Get(tour1.KeyDataCRC32(), id_key_tour1)
	var id1 uint64
	var key_data1_key string
	var price_data1_key string
	if err1 != nil {
		t.Error("Can not read map tour ID data from", id_key_tour1, ". Error:", err1)
	} else {
		val1int, err1int := strconv.ParseUint(id1str, 10, 64)
		if err1int != nil || val1int <= 0 {
			t.Error("Bad ID value '", id1str,
				"' for map tour from", id_key_tour1,
				". Error:", err1int)
		}
		id1 = val1int

		key_data1_key = fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, id1)
		key_data1, err1kd := cache.Get(id1, key_data1_key)
		if err1kd != nil {
			t.Error("Can not read map tour KEY data from", key_data1_key, ". Error:", err1kd)
		} else if key_data1 != tour1.KeyData() {
			t.Error("Wrong KEY DATA map tour from", key_data1_key,
				". Expected:", tour1.KeyData(), ", got: ", key_data1)
		}

		price_data1_key = fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, id1)
		price_data1, err1pd := cache.Get(id1, price_data1_key)
		if err1pd != nil {
			t.Error("Can not read map tour PRICE data from", price_data1_key, ". Error:", err1pd)
		} else if price_data1 != tour2.PriceData() {
			t.Error("Wrong PRICE DATA map tour from", price_data1_key,
				". Expected:", tour2.PriceData(), ", got: ", price_data1)
		}
	}

	len_ins, err := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours.MapTourInsertQueue).Result()
	if err != nil {
		t.Error("Can not read map tour INSERT queue length:", err)
	} else if len_ins != 1 {
		t.Error("Wrong map tour INSERT queue length. Expected 1, got", len_ins)
	}

	len_upd, err := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours.MapTourUpdateQueue).Result()
	if err != nil {
		t.Error("Can not read map tour UPDATE queue length:", err)
	} else if len_upd != 1 {
		t.Error("Wrong map tour UPDATE queue length. Expected 1, got", len_upd)
	}

	cache.CleanQueue(thread_queue)
	cache.CleanQueue(map_tours.MapTourInsertQueue)
	cache.CleanQueue(map_tours.MapTourUpdateQueue)
	cache.Del(tour1.KeyDataCRC32(), id_key_tour1)
	cache.Del(id1, key_data1_key)
	cache.Del(id1, price_data1_key)
}

func TestMapToursThreadProcessPriceLogUpdate(t *testing.T) {
	init_test_redis_single()
	init_workers()

	init_test_redis_single()
	init_workers()

	tour1 := *random_tour_map()
	tour2 := tour1
	tour3 := tour1

	tour2.Price += 1
	tour3.Price += 1

	if tour1.KeyData() != tour2.KeyData() || tour1.PriceData() == tour2.PriceData() {
		t.Error("Wrong to initialize price for test.")
	}

	thread_index := 0
	thread_queue := fmt.Sprintf(map_tours.ThreadMapToursQueueTemplate, thread_index)

	cache.AddQueue(thread_queue, tour1.ToString())
	cache.AddQueue(thread_queue, tour2.ToString())
	cache.AddQueue(thread_queue, tour3.ToString())

	map_tours.ForceStopThreads = false
	worker_base.Workers[0].MainLoop()

	for !cache.IsEmptyQueue(thread_queue) {
		time.Sleep(TestWaitTime)
	}
	map_tours.ForceStopThreads = true
	time.Sleep(GoroutineFinishWaitTime)

	id_key_tour1 := fmt.Sprintf(map_tours.MapTourIDKeyTemplate, tour1.KeyData())
	id1str, err1 := cache.Get(tour1.KeyDataCRC32(), id_key_tour1)
	var id1 uint64
	var key_data1_key string
	var price_data1_key string
	var price_log_data1_key string
	if err1 != nil {
		t.Error("Can not read map tour ID data from", id_key_tour1, ". Error:", err1)
	} else {
		val1int, err1int := strconv.ParseUint(id1str, 10, 64)
		if err1int != nil || val1int <= 0 {
			t.Error("Bad ID value '", id1str,
				"' for map tour from", id_key_tour1,
				". Error:", err1int)
		}
		id1 = val1int

		key_data1_key = fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, id1)
		key_data1, err1kd := cache.Get(id1, key_data1_key)
		if err1kd != nil {
			t.Error("Can not read map tour KEY data from", key_data1_key, ". Error:", err1kd)
		} else if key_data1 != tour1.KeyData() {
			t.Error("Wrong KEY DATA map tour from", key_data1_key,
				". Expected:", tour1.KeyData(), ", got: ", key_data1)
		}

		price_data1_key = fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, id1)
		price_data1, err1pd := cache.Get(id1, price_data1_key)
		if err1pd != nil {
			t.Error("Can not read map tour PRICE data from", price_data1_key, ". Error:", err1pd)
		} else if price_data1 != tour1.PriceData() {
			t.Error("Wrong PRICE DATA map tour from", price_data1_key,
				". Expected:", tour1.PriceData(), ", got: ", price_data1)
		}

		price_log_data1_key = fmt.Sprintf(map_tours.MapTourPriceLogKeyTemplate, id1)
		price_log_data1, err1pld := cache.LRange(id1, price_log_data1_key, 0, -1)
		if err1pld != nil {
			t.Error("Can not read map tour PRICE LOG data from", price_log_data1_key,
				". Error:", err1pld)
		} else {
			if len(price_log_data1) != 2 {
				t.Error("Wrong PRICE LOG DATA array length. Expected 2, got: ",
					len(price_log_data1))
			} else {
				if price_log_data1[0] != tour2.PriceData() {
					t.Error("Wrong PRICE LOG DATA map tour from", price_log_data1_key,
						"[0]. Expected:", tour2.PriceData(),
						", got: ", price_log_data1[0])
				}
				if price_log_data1[1] != tour3.PriceData() {
					t.Error("Wrong PRICE LOG DATA map tour from", price_log_data1_key,
						"[1]. Expected:", tour3.PriceData(),
						", got: ", price_log_data1[1])
				}
			}
		}
	}

	len_ins, err := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours.MapTourInsertQueue).Result()
	if err != nil {
		t.Error("Can not read map tour INSERT queue length:", err)
	} else if len_ins != 1 {
		t.Error("Wrong map tour INSERT queue length. Expected 1, got", len_ins)
	}

	len_upd, err := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours.MapTourUpdateQueue).Result()
	if err != nil && err != redis.Nil {
		t.Error("Can not read map tour UPDATE queue length:", err)
	} else if len_upd != 0 && err != redis.Nil {
		t.Error("Wrong map tour UPDATE queue length. Expected 0, got", len_upd)
	}

	cache.CleanQueue(thread_queue)
	cache.CleanQueue(map_tours.MapTourInsertQueue)
	cache.CleanQueue(map_tours.MapTourUpdateQueue)
	cache.Del(tour1.KeyDataCRC32(), id_key_tour1)
	cache.Del(id1, key_data1_key)
	cache.Del(id1, price_data1_key)
	cache.Del(id1, price_log_data1_key)
}