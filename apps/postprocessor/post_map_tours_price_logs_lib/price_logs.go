package post_map_tours_price_logs

import (
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/log"
	"gopkg.in/redis.v4"
	"github.com/uncleandy/tcache2/tours"
	"time"
	"strconv"
)

var (
	WorkerKeysProcessed = 0
	WorkerPricesUpdated = 0
	WorkerKeysDeleted = 0
	WorkerKeysBad = 0
	WorkerKeysSkip = 0
)

const (
	PriceDataExpiredDuration = 1 * time.Hour
)

func (post_worker *PostMapToursWorker) ProcessPriceLogs(tour_id uint64) {
	price_log_key := fmt.Sprintf(map_tours.MapTourPriceLogKeyTemplate, tour_id)
	price_log, err := cache.LRange(tour_id, price_log_key, 0, -1)
	if err != nil {
		return
	}

	mutex := tours.MapTourUpdateLocker(tour_id)
	mutex.Lock()
	defer mutex.Unlock()

	price_data_key := fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour_id)
	price_data, err := cache.Get(tour_id, price_data_key)
	if err != nil {
		if err == redis.Nil {
			log.Error.Print("WARNING!!! No price data for tour ", tour_id)
		} else {
			log.Error.Print("Error when read price data for tour ", tour_id, ":", err)
		}

		WorkerKeysCountMutex.Lock()
		WorkerKeysBad++
		WorkerKeysCountMutex.Unlock()
	}

	tour := tours.TourMap{}
	tour.FromPriceData(price_data)

	if !post_worker.PriceDataExpired(&tour) {
		WorkerKeysCountMutex.Lock()
		WorkerKeysSkip++
		WorkerKeysCountMutex.Unlock()
		return
	}

	expire_time := post_worker.CurrentExpireTime().Format("2006-01-02 15:04:05")
	price_time, err := time.Parse("2006-01-02 15:04:05", tour.UpdateDate)
	if err != nil {
		WorkerKeysCountMutex.Lock()
		WorkerKeysBad++
		WorkerKeysCountMutex.Unlock()
		log.Error.Print("Wrong tour.UpdateTime string for tour: ", tour.UpdateDate, "\n", err)
	} else {
		if post_worker.CurrentExpireTime().Unix() < price_time.Unix() {
			expire_time = price_time.Format("2006-01-02 15:04:05")
		}
	}

	// Select log records only after current time of price
	actual_logs := PriceLogAfterTime(price_log, expire_time)

	if len(actual_logs) <= 0 {
		WorkerKeysCountMutex.Lock()
		WorkerKeysDeleted++
		WorkerKeysCountMutex.Unlock()

		post_worker.DeleteMapTour(tour_id, "", price_data_key, price_log_key)
		return
	}

	// Find min price from price_logs
	var min_price_data string
	var min_price_time string
	min_price := -1
	for _, price_log_row := range actual_logs {
		tour_price := tours.TourMap{}
		err := tour_price.FromPriceData(price_log_row)
		if err != nil {
			continue
		}

		if min_price == -1 || tour_price.Price <= min_price {
			min_price_data = price_log_row
			min_price_time = tour_price.UpdateDate
			min_price = tour_price.Price
		}
	}

	if min_price_data != "" {
		WorkerKeysCountMutex.Lock()
		WorkerPricesUpdated++
		WorkerKeysCountMutex.Unlock()

		// Save new price data
		cache.Set(tour_id, price_data_key, min_price_data)

		// Save new price_log if not empty
		new_price_logs := PriceLogAfterTime(price_log, min_price_time)
		cache.Del(tour_id, price_log_key)
		for _, new_price_log_row := range new_price_logs {
			cache.RPush(tour_id, price_log_key, new_price_log_row)
		}

		// Add id to update queue
		cache.AddQueue(map_tours.MapTourUpdateQueue, strconv.FormatUint(tour_id, 10))
	} else {
		WorkerKeysCountMutex.Lock()
		WorkerKeysDeleted++
		WorkerKeysCountMutex.Unlock()

		post_worker.DeleteMapTour(tour_id, "", price_data_key, price_log_key)
	}
}

func (post_worker *PostMapToursWorker) PriceDataExpired(tour *tours.TourMap) bool {
	price_time, err := time.Parse("2006-01-02 15:04:05", tour.UpdateDate)
	if err != nil {
		log.Error.Print("Wrong tour.UpdateTime string for tour: ", tour.UpdateDate, "\n", err)
		return false
	}

	expire_time_unix := time.Now().Add(-PriceDataExpiredDuration).UTC().Unix()
	return price_time.Unix() <= expire_time_unix
}

func (post_worker *PostMapToursWorker) CurrentExpireTime() time.Time {
	return time.Now().Add(-PriceDataExpiredDuration).UTC()
}

// Return list of price log records with UpdateDate after from param
func PriceLogAfterTime(price_log []string, time_str string) []string {
	if len(price_log) <= 0 {
		return price_log
	}

	compare_time, err := time.Parse("2006-01-02 15:04:05", time_str)
	if err != nil {
		log.Error.Print("Wrong time_str param in PriceLogAfterTime: ", time_str, "\n", err)
	}
	compare_time_unix := compare_time.Unix()

	result := []string{}

	for _, row := range price_log {
		tour := tours.TourMap{}
		err := tour.FromPriceData(row)
		if err != nil {
			log.Error.Print("Can not convert PriceData in PriceLogAfterTime: '", row, "' - ", err)
			continue
		}

		log_price_time, err := time.Parse("2006-01-02 15:04:05", tour.UpdateDate)
		if log_price_time.Unix() > compare_time_unix {
			result = append(result, row)
		}
	}

	return result
}

// Delete map tour from cache and set ID to queue fro delete from DB
func (post_worker *PostMapToursWorker) DeleteMapTour(tour_id uint64, key_data_key string, price_data_key string, price_log_key string) {
	if key_data_key == "" {
		key_data_key = fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour_id)
	}
	if price_data_key == "" {
		price_data_key = fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour_id)
	}
	if price_log_key == "" {
		price_log_key = fmt.Sprintf(map_tours.MapTourPriceLogKeyTemplate, tour_id)
	}

	cache.Del(tour_id, price_log_key)
	cache.Del(tour_id, price_data_key)

	key_data, err := cache.Get(tour_id, key_data_key)
	if err == nil {
		cache.Del(tour_id, fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour_id))
		tour := tours.TourMap{}
		tour.FromKeyData(key_data)
		cache.Del(tour.KeyDataCRC32(), fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
	}

	// Delete tour from DB
	cache.AddQueue(map_tours.MapTourDeleteQueue, strconv.FormatUint(tour_id, 10))
}
