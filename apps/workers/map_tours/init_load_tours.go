package map_tours

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
	"strconv"
	"gopkg.in/redis.v4"
)

const (
	batchSizeForToursLoad = 10000
)

// Sync map tours data from DB to Redis
func (worker *MapToursWorker) LoadToursData() {
	db.CheckConnect()

	var last_id uint64
	var last_count int
	last_id = 0
	last_count = 1
	for last_count > 0 {
		last_count = 0
		rows, err := db.SendQuery(
			`SELECT
				id,
				source_id, price, currency_id, checkin, nights, adults, kids, hotel_id, town_id,
				meal_id, created_at, dpt_city_id, country_id, price_byr,
				price_eur, price_usd, kid1age, kid2age, kid3age, price_updated_at,
				tickets_included, has_econom_tickets_dpt, has_econom_tickets_rtn, hotel_is_in_stop,
				fuel_surcharge_min, fuel_surcharge_max, room_name, ht_place_name, tour_url
			FROM cached_sletat_tours
			WHERE id > ?
			ORDER BY id
			LIMIT ?`,
			last_id,
			batchSizeForToursLoad,
		)
		if err != nil {
			log.Error.Fatal("Can not read CachedSletatTours data. Error: ", err)
		}

		defer rows.Close()

		if rows.Err() != nil {
			log.Error.Fatal("Can not read CachedSletatTours data. Error: ", rows.Err())
		}

		kid1age := -1
		kid2age := -1
		kid3age := -1

		tour := tours.TourMap{}
		tour.Kid1Age = &kid1age
		tour.Kid2Age = &kid2age
		tour.Kid3Age = &kid3age

		for rows.Next() {
			err = rows.Scan(
				&last_id,
				&tour.SourceId,
				&tour.Price,
				&tour.CurrencyId,
				&tour.Checkin,
				&tour.Nights,
				&tour.Adults,
				&tour.Kids,
				&tour.HotelId,
				&tour.TownId,
				&tour.MealId,
				&tour.CreateDate,
				&tour.DptCityId,
				&tour.CountryId,
				&tour.PriceByr,
				&tour.PriceEur,
				&tour.PriceUsd,
				tour.Kid1Age,
				tour.Kid2Age,
				tour.Kid3Age,
				&tour.UpdateDate,
				&tour.TicketsIncluded,
				&tour.HasEconomTicketsDpt,
				&tour.HasEconomTicketsRtn,
				&tour.HotelIsInStop,
				&tour.FuelSurchargeMin,
				&tour.FuelSurchargeMax,
				&tour.RoomName,
				&tour.HtPlaceName,
				&tour.TourUrl,
			)
			if err != nil {
				log.Error.Println(err)
			}

			if last_id >= 0 && tour.Adults > 0 {
				shard_crc := tour.KeyDataCRC32()
				last_id_str := strconv.FormatUint(last_id, 10)

				cache.Set(shard_crc, "mtk:" + tour.KeyData(), last_id_str)
				cache.Set(last_id, "mtkk:" + last_id_str, tour.KeyData())
				cache.Set(last_id, "mtp:" + last_id_str, tour.PriceData())

				worker.SetCurrentID(last_id)
			}

			last_count++
		}
	}
}

func (worker *MapToursWorker) SetCurrentID(id uint64) {
	current_id, err := cache.GetID(tours.TourMapRedisGenIdKey)
	if err != nil && err != redis.Nil {
		log.Error.Print("Can not get MapTours CurrentID sequence from Redis key ", tours.TourMapRedisGenIdKey)
	}

	if err == redis.Nil {
		current_id = 0
	}

	if id > current_id {
		cache.SetID(tours.TourMapRedisGenIdKey, id)
	}
}
