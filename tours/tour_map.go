package tours

import (
	"strconv"
	"strings"
	"hash/crc32"
	"github.com/uncleandy/tcache2/cache"
	"time"
	"github.com/hjr265/redsync.go/redsync"
	"reflect"
)

const (
	TourMapKeyDataSeparator = "|"
	TourMapKeyDataSeparatorCode = "&#124;"
	TourMapRedisGenIdKey = "serial_map_tour"
	MapTourUpdateMutexTemplate = "map_update_%d"
)

var (
	TourMapKeyDataFields = DataOrderFields{
		StringFields	: map[string]int{
			"Checkin": 1,
		},
		IntFields	: map[string]int{
			"HotelId"	: 0,
			"DptCityId"	: 2,
			"Nights"	: 3,
			"Adults"	: 4,
			"MealId"	: 5,
			"Kids"		: 6,
		},
		RefIntFields	: map[string]int{
			"Kid1Age"	: 7,
			"Kid2Age"	: 8,
			"Kid3Age"	: 9,
		},
	}
	TourMapPriceDataFields = DataOrderFields{
		StringFields	: map[string]int{
			"UpdateDate"	: 1,
			"RoomName"	: 8,
			"HtPlaceName"	: 9,
			"TourUrl"	: 10,
		},
		IntFields	: map[string]int{
			"Price"			: 0,
			"FuelSurchargeMin"	: 2,
			"FuelSurchargeMax"	: 3,
			"TicketsIncluded"	: 4,
			"HasEconomTicketsDpt"	: 5,
			"HasEconomTicketsRtn"	: 6,
			"HotelIsInStop"		: 7,
		},
		RefIntFields	: map[string]int{},
	}
	TourMapSQLFields = DataSQLFields{
		StringFields: map[string]string{
			"UpdateDate":	"price_updated_at",
			"Checkin":	"checkin",
			"TourUrl":	"tour_url",
			"RoomName":	"room_name",
			"HtPlaceName":	"ht_place_name",
			"CreateDate":	"created_at",
		},
		IntFields: map[string]string{
			"SourceId":		"source_id",
			"Price":                "price",
			"CurrencyId":           "currency_id",
			"Nights":               "nights",
			"Adults":               "adults",
			"Kids":                 "kids",
			"HotelId":              "hotel_id",
			"TownId":               "town_id",
			"MealId":               "meal_id",
			"TicketsIncluded":      "tickets_included",
			"HasEconomTicketsDpt":  "has_econom_tickets_dpt",
			"HasEconomTicketsRtn":  "has_econom_tickets_rtn",
			"HotelIsInStop":        "hotel_is_in_stop",
			"DptCityId":            "dpt_city_id",
			"CountryId":            "country_id",
			"PriceByr":             "price_byr",
			"PriceEur":             "price_eur",
			"PriceUsd":             "price_usd",
			"FuelSurchargeMin":     "fuel_surcharge_min",
			"FuelSurchargeMax":     "fuel_surcharge_max",
		},
		RefIntFields: map[string]string{
			"Kid1Age":              "kid1age",
			"Kid2Age":              "kid2age",
			"Kid3Age":              "kid3age",
		},
	}
)

type TourMap struct {
	TourBase
}

func (t *TourMap) KeyData() string {
	kid1age := -1
	if t.Kid1Age != nil {
		kid1age = *(t.Kid1Age)
	}

	kid2age := -1
	if t.Kid2Age != nil {
		kid2age = *(t.Kid2Age)
	}

	kid3age := -1
	if t.Kid3Age != nil {
		kid3age = *(t.Kid3Age)
	}

	key_data := []string{
		strconv.Itoa(t.HotelId),
		t.Checkin,
		strconv.Itoa(t.DptCityId),
		strconv.Itoa(t.Nights),
		strconv.Itoa(t.Adults),
		strconv.Itoa(t.MealId),
		strconv.Itoa(t.Kids),
		strconv.Itoa(kid1age),
		strconv.Itoa(kid2age),
		strconv.Itoa(kid3age),
	}
	return strings.Join(key_data, TourMapKeyDataSeparator)
}

func (t *TourMap) FromKeyData(key_data string) error {
	return t.FieldsFromString(key_data, &TourMapKeyDataFields)
}

func (t *TourMap) PriceData() string {
	price_data := []string{
		strconv.Itoa(t.Price),
		t.UpdateDate,
		strconv.Itoa(t.FuelSurchargeMin),
		strconv.Itoa(t.FuelSurchargeMax),
		strconv.Itoa(t.TicketsIncluded),
		strconv.Itoa(t.HasEconomTicketsDpt),
		strconv.Itoa(t.HasEconomTicketsRtn),
		strconv.Itoa(t.HotelIsInStop),
		TourEscaped(t.RoomName, TourMapKeyDataSeparator, TourMapKeyDataSeparatorCode),
		TourEscaped(t.HtPlaceName, TourMapKeyDataSeparator, TourMapKeyDataSeparatorCode),
		TourEscaped(t.TourUrl, TourMapKeyDataSeparator, TourMapKeyDataSeparatorCode),
	}
	return strings.Join(price_data, TourMapKeyDataSeparator)
}

func (t *TourMap) FromPriceData(price_data string) error {
	return t.FieldsFromString(price_data, &TourMapPriceDataFields)
}

func (t *TourMap) KeyDataCRC32() uint64 {
	key_data := t.KeyData()
	return uint64(crc32.ChecksumIEEE([]byte(key_data)))
}

func (t *TourMap) GenId() (uint64, error) {
	return cache.NewID(TourMapRedisGenIdKey)
}

func (t *TourMap) PriceBiggerThen(price_data_str string) (bool, error) {
	price_data := strings.Split(price_data_str, TourMapKeyDataSeparator)
	price, err := strconv.ParseInt(price_data[0], 10, 64)
	if err != nil {
		return true, err
	}

	return price > 0 && int64(t.Price) > price, nil
}

func (t *TourMap) UpdateDateLaterThen(price_data_str string) (bool, error) {
	price_data := strings.Split(price_data_str, TourMapKeyDataSeparator)
	old_update_time, err := time.Parse("2006-01-02 15:04:05", price_data[1])
	if err != nil {
		return false, err
	}

	new_update_time, err := time.Parse("2006-01-02 15:04:05", t.UpdateDate)
	if err != nil {
		return false, err
	}

	return new_update_time.After(old_update_time), nil
}

func (t *TourBase) InsertSQLFieldsSet() string {
	return t.InsertSQLFieldsSetBy(&TourMapSQLFields)
}

func (t *TourBase) InsertSQLDataSet() string {
	return t.InsertSQLDataSetBy(&TourMapSQLFields)
}

func (t *TourBase) UpdateSQLString() string {
	return t.UpdateSQLStringBy(&TourMapSQLFields)
}

func LockMapTourUpdate(id uint64) *redsync.Mutex {
	return LockTourUpdate(MapTourUpdateMutexTemplate, id)
}
