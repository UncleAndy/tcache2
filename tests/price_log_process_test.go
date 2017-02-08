package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"fmt"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/postprocessor/post_map_tours_price_logs_lib"
	"github.com/bouk/monkey"
	"reflect"
	"github.com/uncleandy/tcache2/tours"
	"time"
	"gopkg.in/redis.v4"
)

func TestPriceLogProcess(t *testing.T) {
	init_test_redis_single()

	// Init data
	tour0 := random_tour_map()
	tour1 := random_tour_map()
	tour2 := random_tour_map()
	tour3 := random_tour_map()
	price_log := []string{}

	tour0.UpdateDate = "2017-01-01 10:00:00"
	tour0.Price = 10000
	price_log = append(price_log, tour0.PriceData())

	tour3.UpdateDate = "2017-01-20 01:00:00"
	tour3.Price = 5000
	price_log = append(price_log, tour3.PriceData())

	tour2.UpdateDate = "2017-01-03 08:00:00"
	tour2.Price = 1000
	price_log = append(price_log, tour2.PriceData())

	tour1.UpdateDate = "2017-01-01 12:00:00"
	tour1.Price = 2500
	price_log = append(price_log, tour1.PriceData())

	price_log_worker := post_map_tours_price_logs.PostMapToursWorker{}

	monkey.PatchInstanceMethod(
		reflect.TypeOf(&post_map_tours_price_logs.PostMapToursWorker{}),
		"PriceDataExpired",
		func(_ *post_map_tours_price_logs.PostMapToursWorker, _ *tours.TourMap) bool {
			return true
		},
	)
	monkey.PatchInstanceMethod(
		reflect.TypeOf(&post_map_tours_price_logs.PostMapToursWorker{}),
		"CurrentExpireTime",
		func(_ *post_map_tours_price_logs.PostMapToursWorker) time.Time {
			return time.Date(2016, 12, 1, 12, 0, 0, 0, time.UTC)
		},
	)

	// Case 1: When tour price is last by time
	tour := random_tour_map()
	tour.Price = 100000
	tour.UpdateDate = "2017-01-21 23:00:00"

	// Set case data to Redis
	tour_id := uint64(1)
	price_data_key :=  fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour_id)
	cache.Set(tour_id, price_data_key, tour.PriceData())
	price_log_key := fmt.Sprintf(map_tours.MapTourPriceLogKeyTemplate, tour_id)
	cache.Del(tour_id, price_log_key)
	for _, price_row := range price_log {
		cache.RPush(tour_id, price_log_key, price_row)
	}

	// Process
	price_log_worker.ProcessPriceLogs(tour_id)

	// Read data
	result_price_data, err := cache.Get(tour_id, price_data_key)
	if err != redis.Nil {
		t.Error("Price data should by clean, but got ", price_data_key)
	}

	result_price_log, err := cache.LRange(tour_id, price_log_key, 0, -1)
	if err != redis.Nil && len(result_price_log) != 0 {
		t.Error("Price log data should by clean, but got ", result_price_log)
	}

	// Case 2: If tour price lower all prices from price log
	// Set init data
	tour.Price = 100
	tour.UpdateDate = "2016-01-01 10:00:00"
	cache.Set(tour_id, price_data_key, tour.PriceData())
	cache.Del(tour_id, price_log_key)
	for _, price_row := range price_log {
		cache.RPush(tour_id, price_log_key, price_row)
	}

	// Process
	price_log_worker.ProcessPriceLogs(tour_id)

	// Read data
	result_price_data, err = cache.Get(tour_id, price_data_key)
	if err != nil {
		t.Error("Can not read price data from ", price_data_key)
	}

	result_price_log, err = cache.LRange(tour_id, price_log_key, 0, -1)
	if err != nil {
		t.Error("Can not read price log from ", price_log_key)
	}

	// Check result
	if result_price_data != tour2.PriceData() {
		t.Error("Tour price data change! Expected ", tour2.PriceData(), ", got", result_price_data)
	}

	// Must keep only records in price log AFTER current (1)
	if len(result_price_log) != 1 {
		t.Error("Tour price log should not change! Expected len = 1 ",
			", got", len(result_price_log))
	}

	// Case 3: If price log lower tour price - should update & partial clean price_log
	tour.Price = 1500
	tour.UpdateDate = "2016-01-01 10:00:00"
	cache.Set(tour_id, price_data_key, tour.PriceData())
	cache.Del(tour_id, price_log_key)
	for _, price_row := range price_log {
		cache.RPush(tour_id, price_log_key, price_row)
	}

	// Process
	price_log_worker.ProcessPriceLogs(tour_id)

	// Read data
	result_price_data, err = cache.Get(tour_id, price_data_key)
	if err != nil {
		t.Error("Can not read price data from ", price_data_key)
	}

	result_price_log, err = cache.LRange(tour_id, price_log_key, 0, -1)
	if err != nil {
		t.Error("Can not read price log from ", price_log_key)
	}

	// Check result
	if result_price_data != tour2.PriceData() {
		t.Error("Wrong tour price data! Expected ", tour2.PriceData(), ", got", result_price_data)
	}

	if len(result_price_log) != 1 {
		t.Error("Wrong tour price log len! Expected len = 1, got", len(result_price_log))
	}
	if result_price_log[0] != tour3.PriceData() {
		t.Error("Wrong tour price_log[0] content! Expected len = ", tour3.PriceData(),
			", got", result_price_log[0])
	}

	monkey.UnpatchAll()
}