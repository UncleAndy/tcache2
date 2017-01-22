package post_map_tours_price_logs

import (
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/log"
	"gopkg.in/redis.v4"
	"github.com/uncleandy/tcache2/tours"
	"time"
)

func (post_worker *PostMapToursWorker) ProcessPriceLogs(tour_id uint64) {
	price_log_key := fmt.Sprintf(map_tours.MapTourPriceLogKeyTemplate, tour_id)
	price_log, err := cache.LRange(tour_id, price_log_key, 0, -1)
	if err != nil {
		return
	}

	mutex := tours.LockMapTourUpdate(tour_id)
	defer mutex.Unlock()

	price_data_key := fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour_id)
	price_data, err := cache.Get(tour_id, price_data_key)
	if err != nil {
		if err == redis.Nil {
			log.Error.Print("WARNING!!! No price data for tour ", tour_id)
		} else {
			log.Error.Print("Error when read price data for tour ", tour_id, ":", err)
		}
	}

	tour := tours.TourMap{}
	tour.FromPriceData(price_data)

	// Select log records only after current time of price
	actual_logs := PriceLogAfterTime(price_log, tour.UpdateDate)

	// Find min price from price_logs
	var min_price_data string
	var min_price_time string
	for _, price_log_row := range actual_logs {
		tour_price := tours.TourMap{}
		err := tour_price.FromPriceData(price_log_row)
		if err != nil {
			continue
		}

		if tour_price.Price < tour.Price {
			min_price_data = price_log_row
			min_price_time = tour_price.UpdateDate
		}
	}

	if min_price_data != "" {
		// Save new price data
		cache.Set(tour_id, price_data_key, min_price_data)

		// Save new price_log if not empty
		new_price_logs := PriceLogAfterTime(min_price_time)
		cache.Del(tour_id, price_log_key)
		for _, new_price_log_row := range new_price_logs {
			cache.RPush(tour_id, price_log_key, new_price_log_row)
		}
	}
}

// Return list of price log records with UpdateDate after from param
func PriceLogAfterTime(price_log []string, time_str string) []string {
	if len(price_log) <= 0 {
		return price_log
	}

	compare_time, err := time.Parse("2015-03-07 11:06:39", time_str)
	if err != nil {
		log.Error.Print("Wrong time_str param in PriceLogAfterTime: ", time_str)
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

		log_price_time, err := time.Parse("2015-03-07 11:06:39", tour.UpdateDate)
		if log_price_time.Unix() > compare_time_unix {
			result = append(result, row)
		}
	}

	return result
}
