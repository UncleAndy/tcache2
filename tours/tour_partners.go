package tours

import (
	"strings"
	"strconv"
	"hash/crc32"
	"time"
	"github.com/uncleandy/tcache2/cache"
)

const (
	TourPartnersKeyDataSeparator = "|"
	TourPartnersKeyDataSeparatorCode = "&#124;"
	TourPartnersRedisGenIdKey = "serial_partners_tour"
)

type TourPartners struct {
	TourBase
}

var (
	TourPartnersKeyDataFields = DataOrderFields{
		StringFields	: map[string]int{
			"Checkin"	: 4,
		},
		IntFields	: map[string]int{
			"SourceId"	: 0,
			"CountryId"	: 1,
			"TownId"	: 2,
			"Adults"	: 3,
			"Nights"	: 5,
			"Kids"		: 6,
			"DptCityId"	: 10,
		},
		RefIntFields	: map[string]int{
			"Kid1Age"	: 7,
			"Kid2Age"	: 8,
			"Kid3Age"	: 9,
		},
	}
	TourPartnersPriceDataFields = DataOrderFields{
		StringFields	: map[string]int{
			"UpdateDate"		: 1,
			"MealName"		: 16,
			"RoomName"		: 17,
			"HtPlaceName"		: 18,
			"TourUrl"		: 19,
			"ReceivingParty"	: 20,
			"Description"		: 21,
			"CreateDate"		: 22,
		},
		IntFields	: map[string]int{
			"Price"			: 0,
			"FuelSurchargeMin"	: 2,
			"FuelSurchargeMax"	: 3,
			"TicketsIncluded"	: 4,
			"HasEconomTicketsDpt"	: 5,
			"HasEconomTicketsRtn"	: 6,
			"HotelIsInStop"		: 7,
			"FewEconomTicketsDpt"	: 8,
			"FewEconomTicketsRtn"	: 9,
			"FewPlacesInHotel"	: 10,
			"Flags"			: 11,
			"HotelId"		: 12,
			"MealId"		: 13,
			"RequestId"		: 14,
			"OfferId"		: 15,
		},
		RefIntFields	: map[string]int{},
	}
	TourPartnersSQLFields = DataSQLFields{
		IdSQLField: 	"id",
		StringFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "Checkin",		SQLField: "checkin" },
			DataSQLFieldPair { Field: "Description",	SQLField: "description" },
			DataSQLFieldPair { Field: "TourUrl",		SQLField: "tour_url" },
			DataSQLFieldPair { Field: "RoomName",		SQLField: "room_name" },
			DataSQLFieldPair { Field: "ReceivingParty",	SQLField: "receiving_party" },
			DataSQLFieldPair { Field: "UpdateDate",		SQLField: "update_date" },
			DataSQLFieldPair { Field: "CreateDate",		SQLField: "created_at" },
			DataSQLFieldPair { Field: "MealName",		SQLField: "meal_name" },
			DataSQLFieldPair { Field: "HtPlaceName",	SQLField: "ht_place_name" },
		},
		IntToStringFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "TicketsIncluded",	SQLField: "tickets_included" },
			DataSQLFieldPair { Field: "HasEconomTicketsDpt",SQLField: "has_econom_tickets_dpt" },
			DataSQLFieldPair { Field: "HasEconomTicketsRtn",SQLField: "has_econom_tickets_rtn" },
			DataSQLFieldPair { Field: "HotelIsInStop",	SQLField: "hotel_is_in_stop" },
			DataSQLFieldPair { Field: "RequestId",		SQLField: "sletat_request_id" },
			DataSQLFieldPair { Field: "OfferId",		SQLField: "sletat_offer_id" },
			DataSQLFieldPair { Field: "FewEconomTicketsDpt",SQLField: "few_econom_tickets_dpt" },
			DataSQLFieldPair { Field: "FewEconomTicketsRtn",SQLField: "few_econom_tickets_rtn" },
			DataSQLFieldPair { Field: "FewPlacesInHotel",	SQLField: "few_places_in_hotel" },
		},
		IntFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "Nights",		SQLField: "nights" },
			DataSQLFieldPair { Field: "Adults",		SQLField: "adulst" },
			DataSQLFieldPair { Field: "Kids",		SQLField: "kids" },
			DataSQLFieldPair { Field: "DptCityId",		SQLField: "dpt_city_id" },
			DataSQLFieldPair { Field: "TownId",		SQLField: "town_id" },
			DataSQLFieldPair { Field: "SourceId",		SQLField: "operator_id" },
			DataSQLFieldPair { Field: "Price",		SQLField: "price" },
			DataSQLFieldPair { Field: "HotelId",		SQLField: "hotel_id" },
			DataSQLFieldPair { Field: "Flags",		SQLField: "flags" },
			DataSQLFieldPair { Field: "MealId",		SQLField: "meal_id" },
		},
		RefIntFields: []DataSQLFieldPair{
			DataSQLFieldPair { Field: "Kid1Age",            SQLField: "kid1age" },
			DataSQLFieldPair { Field: "Kid2Age",            SQLField: "kid2age" },
			DataSQLFieldPair { Field: "Kid3Age",            SQLField: "kid3age" },
		},
	}
)

func (t *TourPartners) KeyData() string {
	return t.FieldsToString(&TourPartnersKeyDataFields)
}

func (t *TourPartners) PriceData() string {
	return t.FieldsToString(&TourPartnersPriceDataFields)
}

func (t *TourPartners) KeyDataCRC32() uint64 {
	key_data := t.KeyData()
	return uint64(crc32.ChecksumIEEE([]byte(key_data)))
}


func (t *TourPartners) GenId() (uint64, error) {
	return cache.NewID(TourPartnersRedisGenIdKey)
}

func (t *TourPartners) GetId() uint64 {
	return t.Id
}

func (t *TourPartners) PriceBiggerThen(price_data_str string) (bool, error) {
	price_data := strings.Split(price_data_str, TourPartnersKeyDataSeparator)
	price, err := strconv.ParseInt(price_data[0], 10, 64)
	if err != nil {
		return true, err
	}

	return price > 0 && int64(t.Price) > price, nil
}

func (t *TourPartners) UpdateDateLaterThen(price_data_str string) (bool, error) {
	price_data := strings.Split(price_data_str, TourPartnersKeyDataSeparator)
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

func (t *TourPartners) FromKeyData(key_data string) error {
	return t.FieldsFromString(key_data, &TourPartnersKeyDataFields)
}

func (t *TourPartners) FromPriceData(price_data string) error {
	return t.FieldsFromString(price_data, &TourPartnersPriceDataFields)
}

func (t *TourPartners) InsertSQLFieldsSet() string {
	return t.InsertSQLFieldsSetBy(&TourPartnersSQLFields)
}

func (t *TourPartners) InsertSQLDataSet() string {
	return t.InsertSQLDataSetBy(&TourPartnersSQLFields)
}

func (t *TourPartners) UpdateSQLString() string {
	return t.UpdateSQLStringBy(&TourPartnersSQLFields)
}
