package partners_tours

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

// Sync partners tours data from DB to Redis
// TODO: Tests process
func (worker *PartnersToursWorker) LoadToursData() {
	db.CheckConnect()

	// Load data town_id -> country_id
	country_by_town := map[int]int{}
	rows, err := db.SendQuery(
		`SELECT sletat_city_id, sletat_country_id
		FROM sletat_cities`,
	)
	if err != nil {
		log.Error.Fatal("Can not read SletatCities data. Error: ", err)
	}

	if rows.Err() != nil {
		log.Error.Fatal("Can not read SletatCities data. Error: ", rows.Err())
	}

	for rows.Next() {
		var town_id int
		var country_id int
		err = rows.Scan(
			&town_id,
			&country_id,
		)

		if err != nil {
			log.Error.Println(err)
		} else {
			country_by_town[town_id] = country_id
		}
	}
	rows.Close()

	var last_id uint64
	var last_count int
	last_id = 0
	last_count = 1
	for last_count > 0 {
		last_count = 0
		rows, err := db.SendQuery(
			`SELECT
				id,
				nights, adults, kids, kid1age, kid2age, kid3age, checkin, dpt_city_id,
				town_id, operator_id, price, hotel_id, tickets_included,
				has_econom_tickets_dpt, has_econom_tickets_rtn, hotel_is_in_stop,
				sletat_request_id, sletat_offer_id, few_econom_tickets_dpt,
				few_econom_tickets_rtn, few_places_in_hotel, flags, description, tour_url,
 				room_name, receiving_party, update_date, meal_id, meal_name, ht_place_name,
 				created_at
			FROM partners_tours
			WHERE id > $1
			ORDER BY id
			LIMIT $2`,
			last_id,
			batchSizeForToursLoad,
		)
		if err != nil {
			log.Error.Fatal("Can not read PartnersTours data. Error: ", err)
		}

		if rows.Err() != nil {
			log.Error.Fatal("Can not read PartnersTours data. Error: ", rows.Err())
		}

		kid1age := -1
		kid2age := -1
		kid3age := -1

		tour := tours.TourPartners{}
		tour.Kid1Age = &kid1age
		tour.Kid2Age = &kid2age
		tour.Kid3Age = &kid3age

		for rows.Next() {
			err = rows.Scan(
				&last_id,
				&tour.Nights,
				&tour.Adults,
				&tour.Kids,
				tour.Kid1Age,
				tour.Kid2Age,
				tour.Kid3Age,
				&tour.Checkin,
				&tour.DptCityId,
				&tour.TownId,
				&tour.SourceId,
				&tour.Price,
				&tour.HotelId,
				&tour.TicketsIncluded,
				&tour.HasEconomTicketsDpt,
				&tour.HasEconomTicketsRtn,
				&tour.HotelIsInStop,
				&tour.RequestId,
				&tour.OfferId,
				&tour.FewEconomTicketsDpt,
				&tour.FewEconomTicketsRtn,
				&tour.FewPlacesInHotel,
				&tour.Flags,
				&tour.Description,
				&tour.TourUrl,
				&tour.RoomName,
				&tour.ReceivingParty,
				&tour.UpdateDate,
				&tour.MealId,
				&tour.MealName,
				&tour.HtPlaceName,
				&tour.CreateDate,
			)
			if err != nil {
				log.Error.Println(err)
			}

			if last_id >= 0 && tour.Adults > 0 {
				// Convert times
				tour.Checkin = db.ConvertTime(tour.Checkin)
				tour.CreateDate = db.ConvertTime(tour.CreateDate)
				tour.UpdateDate = db.ConvertTime(tour.UpdateDate)

				tour.CountryId = country_by_town[tour.TownId]

				shard_crc := tour.KeyDataCRC32()
				last_id_str := strconv.FormatUint(last_id, 10)

				cache.Set(shard_crc, "ptk:" + tour.KeyData(), last_id_str)
				cache.Set(last_id, "ptkk:" + last_id_str, tour.KeyData())
				cache.Set(last_id, "ptp:" + last_id_str, tour.PriceData())

				worker.SetCurrentID(last_id)
			}

			last_count++
		}
		rows.Close()
	}
}

func (worker *PartnersToursWorker) SetCurrentID(id uint64) {
	current_id, err := cache.GetID(tours.TourPartnersRedisGenIdKey)
	if err != nil && err != redis.Nil {
		log.Error.Print("Can not get MapTours CurrentID sequence from Redis key ", tours.TourPartnersRedisGenIdKey)
	}

	if err == redis.Nil {
		current_id = 0
	}

	if id > current_id {
		cache.SetID(tours.TourPartnersRedisGenIdKey, id)
	}
}
