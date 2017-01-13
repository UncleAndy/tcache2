package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/tours"
)

func TestTourMapMethods(t *testing.T) {
	kid1age := -1
	kid2age := -1
	kid3age := -1
	tour := tours.TourMap{
		tours.TourBase{
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
			Description:	"Description text |||",
			TourUrl:        "http://site.com/tour1",
			RoomName:       "Room | name",
			ReceivingParty: "ReceivingParty text",
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

	key_data := tour.KeyData()
	key_data_expected := "3|2017-01-30|16|10|2|5|0|-1|-1|-1"
	if key_data != key_data_expected {
		t.Error("TourMap KeyData wrong. Expected:\n", key_data_expected, "\ngot:\n", key_data)
	}

	price_data := tour.PriceData()
	price_data_expected := "20000|2017-01-02|18|19|6|7|8|9|Room &#124; name|&#124; Place name|http://site.com/tour1"
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
