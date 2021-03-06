package map_tours

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
	"gopkg.in/redis.v4"
	"fmt"
	"strconv"
)

const (
	batchSizeForToursLoad = 10000
)

// Sync map tours data from DB to Redis
func (worker *MapToursWorker) LoadToursData() {
	log.Info.Println("Start load map tours data...")
	db.CheckConnect()

	var last_id uint64
	var last_count int
	last_id = 0
	last_count = 1
	all_count := 0
	for last_count > 0 {
		last_count = 0
		rows, err := db.SendQuery(
			`SELECT
				id,
				source_id, price, currency_id, checkin, nights, adults, kids, hotel_id, town_id,
				meal_id, created_at, dpt_city_id, country_id, price_byr,
				price_eur, price_usd, kid1age, kid2age, kid3age, price_updated_at,
				tickets_included, has_econom_tickets_dpt, has_econom_tickets_rtn, hotel_is_in_stop,
				fuel_surcharge_min, fuel_surcharge_max, COALESCE(room_name, ''),
				COALESCE(ht_place_name, ''), COALESCE(tour_url, '')
			FROM cached_sletat_tours
			WHERE id > $1
			ORDER BY id
			LIMIT $2`,
			last_id,
			batchSizeForToursLoad,
		)
		if err != nil {
			log.Error.Fatal("Can not read CachedSletatTours data. Error: ", err)
		}


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
				// Convert times
				tour.Checkin = db.ConvertTime(tour.Checkin)
				tour.CreateDate = db.ConvertTime(tour.CreateDate)
				tour.UpdateDate = db.ConvertTime(tour.UpdateDate)

				cache.Set(tour.KeyDataCRC32(),
					fmt.Sprintf(MapTourIDKeyTemplate, tour.KeyData()),
					strconv.FormatUint(last_id, 10))
				cache.Set(last_id,
					fmt.Sprintf(MapTourKeyDataKeyTemplate, last_id),
					tour.KeyData())
				cache.Set(last_id,
					fmt.Sprintf(MapTourPriceDataKeyTemplate, last_id),
					tour.PriceData())

				worker.SetCurrentID(last_id)
			}

			last_count++
		}
		rows.Close()

		all_count += last_count
		log.Info.Println("Loaded map tours:", all_count)
	}
	log.Info.Println("Finish load map tours data.")
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
