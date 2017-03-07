package tests

import (
	"testing"
	"fmt"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"time"
	"github.com/uncleandy/tcache2/tours"
	"strconv"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"gopkg.in/redis.v4"
	"github.com/bouk/monkey"
)

func random_tour_partners() *tours.TourPartners {
	return &tours.TourPartners{
		TourBase: *random_tour_base(),
	}
}

func TestPartnersToursThreadProcessSimple(t *testing.T) {
	init_test_redis_single()
	init_workers()

	tour1 := *random_tour_partners()
	tour2 := tour1
	tour3 := tour1

	// Different KeyData required for test
	tour2.SourceId += 1
	tour2.Price += 1
	tour3.SourceId += 2
	tour3.Price += 2

	thread_index := 0
	thread_queue := fmt.Sprintf(partners_tours.ThreadPartnersToursQueueTemplate, thread_index)

	cache.AddQueue(thread_queue, tour1.ToString())
	cache.AddQueue(thread_queue, tour2.ToString())
	cache.AddQueue(thread_queue, tour3.ToString())

	monkey.Patch(partners_tours.IsSkipTour, func(_ *tours.TourPartners) bool {
		return false
	})

	partners_tours.ForceStopThreads = false
	go worker_base.Workers[1].MainLoop()

	for !cache.IsEmptyQueue(thread_queue) {
		println("Queue not empty. Wait...")
		time.Sleep(TestWaitTime)
	}
	partners_tours.ForceStopThreads = true
	time.Sleep(GoroutineFinishWaitTime)

	id_key_tour1 := fmt.Sprintf(partners_tours.PartnersTourIDKeyTemplate, tour1.KeyData())
	id1str, err1 := cache.Get(tour1.KeyDataCRC32(), id_key_tour1)
	var id1 uint64
	var key_data1_key string
	var price_data1_key string
	if err1 != nil {
		t.Error("Can not read partners tour ID data from", id_key_tour1, ". Error:", err1)
	} else {
		val1int, err1int := strconv.ParseUint(id1str, 10, 64)
		if err1int != nil || val1int <= 0 {
			t.Error("Bad ID value '", id1str,
				"' for partners tour from", id_key_tour1,
				". Error:", err1int)
		}
		id1 = val1int

		key_data1_key = fmt.Sprintf(partners_tours.PartnersTourKeyDataKeyTemplate, id1)
		key_data1, err1kd := cache.Get(id1, key_data1_key)
		if err1kd != nil {
			t.Error("Can not read partners tour KEY data from", key_data1_key, ". Error:", err1kd)
		} else if key_data1 != tour1.KeyData() {
			t.Error("Wrong KEY DATA partners tour from", key_data1_key,
				". Expected:", tour1.KeyData(), ", got: ", key_data1)
		}

		price_data1_key = fmt.Sprintf(partners_tours.PartnersTourPriceDataKeyTemplate, id1)
		price_data1, err1pd := cache.Get(id1, price_data1_key)
		if err1pd != nil {
			t.Error("Can not read partners tour PRICE data from", price_data1_key, ". Error:", err1pd)
		} else if price_data1 != tour1.PriceData() {
			t.Error("Wrong PRICE DATA partners tour from", price_data1_key,
				". Expected:", tour1.PriceData(), ", got: ", price_data1)
		}
	}

	id_key_tour2 := fmt.Sprintf(partners_tours.PartnersTourIDKeyTemplate, tour2.KeyData())
	id2str, err2 := cache.Get(tour2.KeyDataCRC32(), id_key_tour2)
	var id2 uint64
	var key_data2_key string
	var price_data2_key string
	if err2 != nil {
		t.Error("Can not read partners tour ID data from", id_key_tour2, ". Error:", err2)
	} else {
		val2int, err2int := strconv.ParseUint(id2str, 10, 64)
		if err2int != nil || val2int <= 0 {
			t.Error("Bad ID value '", id2str,
				"' for partners tour from", id_key_tour2,
				". Error:", err2int)
		}
		id2 = val2int

		key_data2_key = fmt.Sprintf(partners_tours.PartnersTourKeyDataKeyTemplate, id2)
		key_data2, err2kd := cache.Get(id2, key_data2_key)
		if err2kd != nil {
			t.Error("Can not read partners tour KEY data from", key_data2_key, ". Error:", err2kd)
		} else if key_data2 != tour2.KeyData() {
			t.Error("Wrong KEY DATA partners tour from", key_data2_key,
				". Expected:", tour1.KeyData(), ", got: ", key_data2)
		}

		price_data2_key = fmt.Sprintf(partners_tours.PartnersTourPriceDataKeyTemplate, id2)
		price_data2, err2pd := cache.Get(id2, price_data2_key)
		if err2pd != nil {
			t.Error("Can not read partners tour PRICE data from", price_data2_key, ". Error:", err2pd)
		} else if price_data2 != tour2.PriceData() {
			t.Error("Wrong PRICE DATA partners tour from", price_data2_key,
				". Expected:", tour2.PriceData(), ", got: ", price_data2)
		}
	}

	id_key_tour3 := fmt.Sprintf(partners_tours.PartnersTourIDKeyTemplate, tour3.KeyData())
	id3str, err3 := cache.Get(tour3.KeyDataCRC32(), id_key_tour3)
	var id3 uint64
	var key_data3_key string
	var price_data3_key string
	if err3 != nil {
		t.Error("Can not read partners tour ID data from", id_key_tour3, ". Error:", err3)
	} else {
		val3int, err3int := strconv.ParseUint(id3str, 10, 64)
		if err3int != nil || val3int <= 0 {
			t.Error("Bad ID value '", id3str,
				"' for partners tour from", id_key_tour3,
				". Error:", err3int)
		}
		id3 = val3int

		key_data3_key = fmt.Sprintf(partners_tours.PartnersTourKeyDataKeyTemplate, id3)
		key_data3, err3kd := cache.Get(id3, key_data3_key)
		if err3kd != nil {
			t.Error("Can not read partners tour KEY data from", key_data3_key, ". Error:", err3kd)
		} else if key_data3 != tour3.KeyData() {
			t.Error("Wrong KEY DATA partners tour from", key_data3_key,
				". Expected:", tour3.KeyData(), ", got: ", key_data3)
		}

		price_data3_key = fmt.Sprintf(partners_tours.PartnersTourPriceDataKeyTemplate, id3)
		price_data3, err3pd := cache.Get(id3, price_data3_key)
		if err3pd != nil {
			t.Error("Can not read partners tour PRICE data from", price_data3_key, ". Error:", err3pd)
		} else if price_data3 != tour3.PriceData() {
			t.Error("Wrong PRICE DATA partners tour from", price_data3_key,
				". Expected:", tour3.PriceData(), ", got: ", price_data3)
		}
	}

	len_ins, err := cache.RedisSettings.MainServers[0].Connection.LLen(partners_tours.PartnersTourInsertQueue).Result()
	if err != nil {
		t.Error("Can not read partners tour INSERT queue length:", err)
	} else if len_ins != 3 {
		t.Error("Wrong partners tour INSERT queue length. Expected 3, got", len_ins)
	}

	len_upd, err := cache.RedisSettings.MainServers[0].Connection.LLen(partners_tours.PartnersTourUpdateQueue).Result()
	if err != nil && err != redis.Nil {
		t.Error("Can not read partners tour UPDATE queue length:", err)
	} else if len_upd != 0 && err != redis.Nil {
		t.Error("Wrong partners tour UPDATE queue length. Expected 0, got", len_upd)
	}

	cache.CleanQueue(thread_queue)
	cache.CleanQueue(partners_tours.PartnersTourInsertQueue)
	cache.CleanQueue(partners_tours.PartnersTourUpdateQueue)
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

func TestPartnersToursThreadProcessPriceUpdate(t *testing.T) {
	init_test_redis_single()
	init_workers()

	tour1 := *random_tour_partners()
	tour2 := tour1

	tour2.Price -= 1

	if tour1.KeyData() != tour2.KeyData() || tour1.PriceData() == tour2.PriceData() {
		t.Error("Wrong to initialize price for test.")
	}

	thread_index := 0
	thread_queue := fmt.Sprintf(partners_tours.ThreadPartnersToursQueueTemplate, thread_index)

	cache.AddQueue(thread_queue, tour1.ToString())
	cache.AddQueue(thread_queue, tour2.ToString())

	monkey.Patch(partners_tours.IsSkipTour, func(_ *tours.TourPartners) bool {
		return false
	})

	partners_tours.ForceStopThreads = false
	go worker_base.Workers[1].MainLoop()

	for !cache.IsEmptyQueue(thread_queue) {
		println("Queue not empty. Wait...")
		time.Sleep(TestWaitTime)
	}
	partners_tours.ForceStopThreads = true
	time.Sleep(GoroutineFinishWaitTime)

	id_key_tour1 := fmt.Sprintf(partners_tours.PartnersTourIDKeyTemplate, tour1.KeyData())
	id1str, err1 := cache.Get(tour1.KeyDataCRC32(), id_key_tour1)
	var id1 uint64
	var key_data1_key string
	var price_data1_key string
	if err1 != nil {
		t.Error("Can not read partners tour ID data from", id_key_tour1, ". Error:", err1)
	} else {
		val1int, err1int := strconv.ParseUint(id1str, 10, 64)
		if err1int != nil || val1int <= 0 {
			t.Error("Bad ID value '", id1str,
				"' for partners tour from", id_key_tour1,
				". Error:", err1int)
		}
		id1 = val1int

		key_data1_key = fmt.Sprintf(partners_tours.PartnersTourKeyDataKeyTemplate, id1)
		key_data1, err1kd := cache.Get(id1, key_data1_key)
		if err1kd != nil {
			t.Error("Can not read partners tour KEY data from", key_data1_key, ". Error:", err1kd)
		} else if key_data1 != tour1.KeyData() {
			t.Error("Wrong KEY DATA partners tour from", key_data1_key,
				". Expected:", tour1.KeyData(), ", got: ", key_data1)
		}

		price_data1_key = fmt.Sprintf(partners_tours.PartnersTourPriceDataKeyTemplate, id1)
		price_data1, err1pd := cache.Get(id1, price_data1_key)
		if err1pd != nil {
			t.Error("Can not read partners tour PRICE data from", price_data1_key, ". Error:", err1pd)
		} else if price_data1 != tour2.PriceData() {
			t.Error("Wrong PRICE DATA partners tour from", price_data1_key,
				". Expected:", tour2.PriceData(), ", got: ", price_data1)
		}
	}


	len_ins, err := cache.RedisSettings.MainServers[0].Connection.LLen(partners_tours.PartnersTourInsertQueue).Result()
	if err != nil {
		t.Error("Can not read partners tour INSERT queue length:", err)
	} else if len_ins != 1 {
		t.Error("Wrong partners tour INSERT queue length. Expected 1, got", len_ins)
	}

	len_upd, err := cache.RedisSettings.MainServers[0].Connection.LLen(partners_tours.PartnersTourUpdateQueue).Result()
	if err != nil {
		t.Error("Can not read partners tour UPDATE queue length:", err)
	} else if len_upd != 1 {
		t.Error("Wrong partners tour UPDATE queue length. Expected 1, got", len_upd)
	}

	cache.CleanQueue(thread_queue)
	cache.CleanQueue(partners_tours.PartnersTourInsertQueue)
	cache.CleanQueue(partners_tours.PartnersTourUpdateQueue)
	cache.Del(tour1.KeyDataCRC32(), id_key_tour1)
	cache.Del(id1, key_data1_key)
	cache.Del(id1, price_data1_key)
}
