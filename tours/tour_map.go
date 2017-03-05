package tours

import (
	"strconv"
	"strings"
	"hash/crc32"
	"github.com/uncleandy/tcache2/cache"
	"time"
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
			"CreateDate"	: 11,
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
		IdSQLField: 	"id",
		StringFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "UpdateDate", 	SQLField: "price_updated_at" },
			DataSQLFieldPair { Field: "Checkin",		SQLField: "checkin" },
			DataSQLFieldPair { Field: "TourUrl",		SQLField: "tour_url" },
			DataSQLFieldPair { Field: "RoomName",		SQLField: "room_name" },
			DataSQLFieldPair { Field: "HtPlaceName",	SQLField: "ht_place_name" },
			DataSQLFieldPair { Field: "CreateDate",		SQLField: "created_at" },
		},
		IntToStringFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "TicketsIncluded",    SQLField: "tickets_included" },
			DataSQLFieldPair { Field: "HasEconomTicketsDpt",SQLField: "has_econom_tickets_dpt" },
			DataSQLFieldPair { Field: "HasEconomTicketsRtn",SQLField: "has_econom_tickets_rtn" },
			DataSQLFieldPair { Field: "HotelIsInStop",      SQLField: "hotel_is_in_stop" },
		},
		IntFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "SourceId",		SQLField: "source_id" },
			DataSQLFieldPair { Field: "Price",              SQLField: "price" },
			DataSQLFieldPair { Field: "CurrencyId",         SQLField: "currency_id" },
			DataSQLFieldPair { Field: "Nights",             SQLField: "nights" },
			DataSQLFieldPair { Field: "Adults",             SQLField: "adults" },
			DataSQLFieldPair { Field: "Kids",               SQLField: "kids" },
			DataSQLFieldPair { Field: "HotelId",            SQLField: "hotel_id" },
			DataSQLFieldPair { Field: "TownId",             SQLField: "town_id" },
			DataSQLFieldPair { Field: "MealId",             SQLField: "meal_id" },
			DataSQLFieldPair { Field: "DptCityId",          SQLField: "dpt_city_id" },
			DataSQLFieldPair { Field: "CountryId",          SQLField: "country_id" },
			DataSQLFieldPair { Field: "PriceByr",           SQLField: "price_byr" },
			DataSQLFieldPair { Field: "PriceEur",           SQLField: "price_eur" },
			DataSQLFieldPair { Field: "PriceUsd",           SQLField: "price_usd" },
			DataSQLFieldPair { Field: "FuelSurchargeMin",   SQLField: "fuel_surcharge_min" },
			DataSQLFieldPair { Field: "FuelSurchargeMax",   SQLField: "fuel_surcharge_max" },
		},
		RefIntFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "Kid1Age",            SQLField: "kid1age" },
			DataSQLFieldPair { Field: "Kid2Age",            SQLField: "kid2age" },
			DataSQLFieldPair { Field: "Kid3Age",            SQLField: "kid3age" },
		},
	}
)

type TourMap struct {
	TourBase
}

func (t *TourMap) KeyData() string {
	return t.FieldsToString(&TourMapKeyDataFields)
}

func (t *TourMap) FromKeyData(key_data string) error {
	return t.FieldsFromString(key_data, &TourMapKeyDataFields)
}

func (t *TourMap) PriceData() string {
	return t.FieldsToString(&TourMapPriceDataFields)
}

func (t *TourMap) FromPriceData(price_data string) error {
	return t.FieldsFromString(price_data, &TourMapPriceDataFields)
}

func (t *TourMap) KeyDataCRC32() uint64 {
	key_data := t.KeyData()
	return uint64(crc32.ChecksumIEEE([]byte(key_data)))
}

func (t *TourMap) GenId() (uint64, error) {
	var err error
	t.Id, err = cache.NewID(TourMapRedisGenIdKey)
	return t.Id, err
}

func (t *TourMap) GetId() uint64 {
	return t.Id
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

func MapTourUpdateLocker(id uint64) *cache.RedisMutex {
	return TourUpdateLocker(MapTourUpdateMutexTemplate, id)
}
