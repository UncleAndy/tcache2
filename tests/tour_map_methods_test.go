package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/tours"
)

func TourMapFixture() tours.TourMap {
	kid1age := -1
	kid2age := -1
	kid3age := -1
	return tours.TourMap{
		tours.TourBase{
			Id: 		111,
			SourceId:      	1,
			UpdateDate:     "2017-01-01",
			Price:          20000,
			CurrencyId:     2,
			Checkin:        "2017-01-30",
			Nights:         10,
			Adults:         2,
			Kids:           0,
			Kid1Age:        &kid1age,
			Kid2Age:        &kid2age,
			Kid3Age:        &kid3age,
			HotelId:        3,
			TownId:         4,
			MealId:         5,
			MealName:       "MealName text",
			TicketsIncluded:        6,
			HasEconomTicketsDpt:    7,
			HasEconomTicketsRtn:    8,
			HotelIsInStop:  9,
			RequestId:      10,
			OfferId:        11,
			FewEconomTicketsDpt:    12,
			FewEconomTicketsRtn:    13,
			FewPlacesInHotel: 	14,
			Flags:          15,
			Description:	"Description \"text\" |||",
			TourUrl:        "http://site.com/tour1",
			RoomName:       "Room | \"name\"",
			ReceivingParty: "Receiving \"Party\" text",
			HtPlaceName:    "| Place name",
			CreateDate:     "2017-01-02",
			DptCityId:      16,
			CountryId:      17,

			PriceByr:       40000,
			PriceEur:       500,
			PriceUsd:       600,

			FuelSurchargeMin:	18,
			FuelSurchargeMax:       19,
		},
	}
}

func TestTourMapMethods(t *testing.T) {
	tour := TourMapFixture()

	key_data := tour.KeyData()
	key_data_expected := "3|2017-01-30|16|10|2|5|0|-1|-1|-1"
	if key_data != key_data_expected {
		t.Error("TourMap KeyData wrong. Expected:\n", key_data_expected, "\ngot:\n", key_data)
	}

	price_data := tour.PriceData()
	price_data_expected := "20000|2017-01-01|18|19|6|7|8|9|Room &#124; \"name\"|&#124; Place name|http://site.com/tour1"
	if price_data != price_data_expected {
		t.Error("TourMap PriceData wrong. Expected:\n", price_data_expected, "\ngot:\n", price_data)
	}

	crc32 := tour.KeyDataCRC32()
	if crc32 == 0 {
		t.Error("TourMap KeyDataCRC32 wrong. Is 0.")
	}

	id, err := tour.GenId()
	if err != nil {
		t.Error("TourMap GenId wrong. Error: ", err)
	} else if id == 0 {
		t.Error("TourMap GenId wrong. Is 0.")
	}
}

func TestTourMapSQLMethods(t *testing.T) {
	tour := TourMapFixture()
	tour_fixture_insert_fields := "id, source_id, price, currency_id, nights, adults, kids, hotel_id, "+
		"town_id, meal_id, dpt_city_id, country_id, price_byr, price_eur, price_usd, "+
		"fuel_surcharge_min, fuel_surcharge_max, "+
		"tickets_included, has_econom_tickets_dpt, has_econom_tickets_rtn, hotel_is_in_stop, "+
		"kid1age, kid2age, kid3age, "+
		"price_updated_at, checkin, tour_url, room_name, ht_place_name, created_at"

	insert_fields := tour.InsertSQLFieldsSet()
	if insert_fields != tour_fixture_insert_fields {
		t.Error("Wrong insert fields set. Expected:\n", tour_fixture_insert_fields,
			"\n Got:\n", insert_fields)
	}

	tour_fixture_insert_values := "111, 1, 20000, 2, 10, 2, 0, 3, 4, 5, 16, 17, 40000, 500, 600, "+
		"18, 19, \"6\", \"7\", \"8\", \"9\", -1, -1, -1, \"2017-01-01\", \"2017-01-30\", \"http://site.com/tour1\", "+
		"\"Room | \\\"name\\\"\", \"| Place name\", \"2017-01-02\""

	insert_values := tour.InsertSQLDataSet()
	if insert_values != tour_fixture_insert_values {
		t.Error("Wrong insert values set. Expected:\n", tour_fixture_insert_values,
			"\n Got:\n", insert_values)
	}

	tour_fixture_update_data := "source_id = 1, price = 20000, currency_id = 2, nights = 10, adults = 2, "+
		"kids = 0, hotel_id = 3, town_id = 4, meal_id = 5, dpt_city_id = 16, country_id = 17, "+
		"price_byr = 40000, price_eur = 500, price_usd = 600, "+
		"fuel_surcharge_min = 18, fuel_surcharge_max = 19, tickets_included = \"6\", "+
		"has_econom_tickets_dpt = \"7\", has_econom_tickets_rtn = \"8\", hotel_is_in_stop = \"9\", "+
		"kid1age = -1, kid2age = -1, kid3age = -1, price_updated_at = \"2017-01-01\", "+
		"checkin = \"2017-01-30\", tour_url = \"http://site.com/tour1\", "+
		"room_name = \"Room | \\\"name\\\"\", ht_place_name = \"| Place name\", "+
		"created_at = \"2017-01-02\""

	update_data := tour.UpdateSQLString()
	if update_data != tour_fixture_update_data {
		t.Error("Wrong insert values set. Expected:\n", tour_fixture_update_data,
			"\n Got:\n", update_data)
	}
}