package tests

import (
	"testing"
	"strings"
	"github.com/uncleandy/tcache2/tours"
)

func TestStringToTour(t *testing.T) {
	tour := tours.TourBase{}

	tour_data := []string{
		"1",
		"2017-01-01",
		"2",
		"10",
		"2",
		"3",
		"2",
		"3",
		"5",
		"-1",
		"6",
		"2016-12-10",
		"20000",
		"1",
		"34",
		"7",
		"Regular Meal",
		"8",
		"9",
		"10",
		"11",
		"12",
		"13",
		"14",
		"15",
		"16",
		"17",
		"Description text",
		"http://site.com/tour1",
		"Room text",
		"ReceivingParty &#124; text",
		"HtPlaceName &#124; text &#124; test &#124;",
		"40000",
		"500",
		"600",
		"18",
		"19",
		"2017-01-02",
	}
	tour_string := strings.Join(tour_data, tours.TourBaseDataSeparator)

	tour.FromString(tour_string)

	if tour.HotelId != 1 {
		t.Error("HotelId value error. Expected 1, got ", tour.HotelId)
	}
	if tour.Checkin != "2017-01-01" {
		t.Error("Checkin value error. Expected 2017-01-01, got ", tour.Checkin)
	}
	if tour.DptCityId != 2 {
		t.Error("DptCityId value error. Expected 2, got ", tour.DptCityId)
	}
	if tour.Nights != 10 {
		t.Error("Nights value error. Expected 10, got ", tour.Nights)
	}
	if tour.Adults != 2 {
		t.Error("Adults value error. Expected 2, got ", tour.Adults)
	}
	if tour.MealId != 3 {
		t.Error("MealId value error. Expected 3, got ", tour.MealId)
	}
	if tour.Kids != 2 {
		t.Error("Kids value error. Expected 2, got ", tour.Kids)
	}
	if tour.Kid1Age == nil {
		t.Error("Kid1Ade is nil.")
	} else if *tour.Kid1Age != 3 {
		t.Error("Kid1Age value error. Expected 3, got ", *tour.Kid1Age)
	}
	if tour.Kid2Age == nil {
		t.Error("Kid2Ade is nil.")
	} else if *tour.Kid2Age != 5 {
		t.Error("Kid2Age value error. Expected 5, got ", *tour.Kid2Age)
	}
	if tour.Kid3Age == nil {
		t.Error("Kid3Ade is nil.")
	} else if *tour.Kid3Age != -1 {
		t.Error("Kid3Age value error. Expected -1, got ", *tour.Kid3Age)
	}
	if tour.SourceId != 6 {
		t.Error("SourceId value error. Expected 6, got ", tour.SourceId)
	}
	if tour.UpdateDate != "2016-12-10" {
		t.Error("UpdateDate value error. Expected 2017-01-01, got ", tour.UpdateDate)
	}
	if tour.Price != 20000 {
		t.Error("Price value error. Expected 20000, got ", tour.Price)
	}
	if tour.CurrencyId != 1 {
		t.Error("CurrencyId value error. Expected 1, got ", tour.CurrencyId)
	}
	if tour.TownId != 7 {
		t.Error("TownId value error. Expected 7, got ", tour.TownId)
	}
	if tour.MealName != "Regular Meal" {
		t.Error("MealName value error. Expected 'Regular Meal', got ", tour.MealName)
	}
	if tour.TicketsIncluded != 8 {
		t.Error("TicketsIncluded value error. Expected 8, got ", tour.TicketsIncluded)
	}
	if tour.HasEconomTicketsDpt != 9 {
		t.Error("HasEconomTicketsDpt value error. Expected 9, got ", tour.HasEconomTicketsDpt)
	}
	if tour.HasEconomTicketsRtn != 10 {
		t.Error("HasEconomTicketsRtn value error. Expected 10, got ", tour.HasEconomTicketsRtn)
	}
	if tour.HotelIsInStop != 11 {
		t.Error("HotelIsInStop value error. Expected 11, got ", tour.HotelIsInStop)
	}
	if tour.RequestId != 12 {
		t.Error("RequestId value error. Expected 12, got ", tour.RequestId)
	}
	if tour.OfferId != 13 {
		t.Error("OfferId value error. Expected 13, got ", tour.OfferId)
	}
	if tour.FewEconomTicketsDpt != 14 {
		t.Error("FewEconomTicketsDpt value error. Expected 14, got ", tour.FewEconomTicketsDpt)
	}
	if tour.FewEconomTicketsRtn != 15 {
		t.Error("FewEconomTicketsRtn value error. Expected 15, got ", tour.FewEconomTicketsRtn)
	}
	if tour.FewPlacesInHotel != 16 {
		t.Error("FewPlacesInHotel value error. Expected 16, got ", tour.FewPlacesInHotel)
	}
	if tour.Flags != 17 {
		t.Error("Flags value error. Expected 17, got ", tour.Flags)
	}
	if tour.Description != "Description text" {
		t.Error("Description value error. Expected 'Description text', got ", tour.Description)
	}
	if tour.TourUrl != "http://site.com/tour1" {
		t.Error("TourUrl value error. Expected 'http://site.com/tour1', got ", tour.TourUrl)
	}
	if tour.RoomName != "Room text" {
		t.Error("RoomName value error. Expected 'Room text', got ", tour.RoomName)
	}
	if tour.ReceivingParty != "ReceivingParty | text" {
		t.Error("ReceivingParty value error. Expected 'ReceivingParty | text', got ",
			tour.ReceivingParty)
	}
	if tour.HtPlaceName != "HtPlaceName | text | test |" {
		t.Error("HtPlaceName value error. Expected 'HtPlaceName | text | test |', got ",
			tour.HtPlaceName)
	}
	if tour.PriceByr != 40000 {
		t.Error("PriceByr value error. Expected 40000, got ", tour.PriceByr)
	}
	if tour.PriceEur != 500 {
		t.Error("PriceEur value error. Expected 500, got ", tour.PriceEur)
	}
	if tour.PriceUsd != 600 {
		t.Error("PriceUsd value error. Expected 600, got ", tour.PriceUsd)
	}
	if tour.FuelSurchargeMin != 18 {
		t.Error("FuelSurchargeMin value error. Expected 18, got ", tour.FuelSurchargeMin)
	}
	if tour.FuelSurchargeMax != 19 {
		t.Error("FuelSurchargeMax value error. Expected 19, got ", tour.FuelSurchargeMax)
	}
}
