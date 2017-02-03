package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/tours"
)

func TourPartnersFixture() tours.TourPartners {
	kid1age := -1
	kid2age := -1
	kid3age := -1
	return tours.TourPartners{
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
			Description:	"Description 'text' |||",
			TourUrl:        "http://site.com/tour1",
			RoomName:       "Room | 'name'",
			ReceivingParty: "Receiving 'Party' text",
			HtPlaceName:    "| Place name",
			CreateDate:     "2017-01-02",
			DptCityId:      16,
			CountryId:      17,

			PriceByr:       40000,
			PriceEur:       500,
			PriceUsd:       600,

			FuelSurchargeMin:	0,
			FuelSurchargeMax:       0,
		},
	}
}

func TestTourPartnersMethods(t *testing.T) {
	tour := TourPartnersFixture()

	key_data := tour.KeyData()
	key_data_expected := "1|17|4|2|2017-01-30|10|0|-1|-1|-1|16"
	if key_data != key_data_expected {
		t.Error("TourPartners KeyData wrong. Expected:\n", key_data_expected, "\ngot:\n", key_data)
	}

	price_data := tour.PriceData()
	price_data_expected := "20000|2017-01-01|0|0|6|7|8|9|12|13|14|15|3|5|10|11|MealName text|Room &#124; 'name'|&#124; Place name|http://site.com/tour1|Receiving 'Party' text|Description 'text' &#124;&#124;&#124;|2017-01-02"
	if price_data != price_data_expected {
		t.Error("TourPartners PriceData wrong. Expected:\n", price_data_expected, "\ngot:\n", price_data)
	}

	crc32 := tour.KeyDataCRC32()
	if crc32 == 0 {
		t.Error("TourPartners KeyDataCRC32 wrong. Is 0.")
	}

	id, err := tour.GenId()
	if err != nil {
		t.Error("TourPartners GenId wrong. Error: ", err)
	} else if id == 0 {
		t.Error("TourPartners GenId wrong. Is 0.")
	}
}

func TestTourPartnersSQLMethods(t *testing.T) {
	tour := TourPartnersFixture()
	tour_fixture_insert_fields := "id, nights, adults, kids, dpt_city_id, town_id, operator_id, price, "+
		"hotel_id, flags, meal_id, tickets_included, has_econom_tickets_dpt, "+
		"has_econom_tickets_rtn, hotel_is_in_stop, sletat_request_id, sletat_offer_id, "+
		"few_econom_tickets_dpt, few_econom_tickets_rtn, few_places_in_hotel, "+
		"kid1age, kid2age, kid3age, "+
		"checkin, description, tour_url, room_name, receiving_party, update_date, created_at, "+
		"meal_name, ht_place_name"

	insert_fields := tour.InsertSQLFieldsSet()
	if insert_fields != tour_fixture_insert_fields {
		t.Error("Wrong insert fields set. Expected:\n", tour_fixture_insert_fields,
			"\n Got:\n", insert_fields)
	}

	tour_fixture_insert_values := "111, 10, 2, 0, 16, 4, 1, 20000, 3, 15, 5, '6', '7', '8', '9', "+
		"'10', '11', '12', '13', '14', -1, -1, -1, '2017-01-30', 'Description ''text'' |||', "+
		"'http://site.com/tour1', 'Room | ''name''', 'Receiving ''Party'' text', '2017-01-01', "+
		"'2017-01-02', 'MealName text', '| Place name'"

	insert_values := tour.InsertSQLDataSet()
	if insert_values != tour_fixture_insert_values {
		t.Error("Wrong insert values set. Expected:\n", tour_fixture_insert_values,
			"\n Got:\n", insert_values)
	}

	tour_fixture_update_data := "nights = 10, adults = 2, kids = 0, dpt_city_id = 16, town_id = 4, operator_id = 1, price = 20000, "+
		"hotel_id = 3, flags = 15, meal_id = 5, tickets_included = '6', has_econom_tickets_dpt = '7', "+
		"has_econom_tickets_rtn = '8', hotel_is_in_stop = '9', sletat_request_id = '10', sletat_offer_id = '11', "+
		"few_econom_tickets_dpt = '12', few_econom_tickets_rtn = '13', few_places_in_hotel = '14', "+
		"kid1age = -1, kid2age = -1, kid3age = -1, "+
		"checkin = '2017-01-30', description = 'Description ''text'' |||', "+
		"tour_url = 'http://site.com/tour1', room_name = 'Room | ''name''', "+
		"receiving_party = 'Receiving ''Party'' text', update_date = '2017-01-01', "+
		"created_at = '2017-01-02', "+
		"meal_name = 'MealName text', ht_place_name = '| Place name'"

	update_data := tour.UpdateSQLString()
	if update_data != tour_fixture_update_data {
		t.Error("Wrong insert values set. Expected:\n", tour_fixture_update_data,
			"\n Got:\n", update_data)
	}
}