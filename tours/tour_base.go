package tours

import (
	"strconv"
	"strings"
	"fmt"
	"reflect"
)

const (
	TourBaseDataSeparator = "|"
	TourBaseDataSeparatorCode = "&#124;"
	TourBaseDataSize = 36
)

type TourBase struct {
	SourceId   		int    `xml:"sourceId,attr"`
	UpdateDate 		string `xml:"updateDate,attr"`
	Price      		int    `xml:"price,attr"`
	CurrencyId 		int    `xml:"currencyId,attr"`
	Checkin    		string `xml:"checkin,attr"`
	Nights     		int    `xml:"nights,attr"`
	Adults     		int    `xml:"adults,attr"`
	Kids       		int    `xml:"kids,attr"`
	Kid1Age    		*int   `xml:"kid1age,attr"`
	Kid2Age    		*int   `xml:"kid2age,attr"`
	Kid3Age    		*int   `xml:"kid3age,attr"`
	HotelId    		int    `xml:"hotelId,attr"`
	TownId     		int    `xml:"townId,attr"`
	MealId     		int    `xml:"mealId,attr"`
	MealName   		string `xml:"mealName,attr"`
	Hash			string `xml:"hash,attr"`
	TicketsIncluded		int `xml:"ticketsIncluded,attr"`
	HasEconomTicketsDpt	int `xml:"hasEconomTicketsDpt,attr"`
	HasEconomTicketsRtn	int `xml:"hasEconomTicketsRtn,attr"`
	HotelIsInStop		int `xml:"hotelIsInStop,attr"`
	RequestId		int `xml:"requestId,attr"`
	OfferId			int64 `xml:"offerId,attr"`
	FewEconomTicketsDpt	int `xml:"fewEconomTicketsDpt,attr"`
	FewEconomTicketsRtn	int `xml:"fewEconomTicketsRtn,attr"`
	FewPlacesInHotel	int `xml:"fewPlacesInHotel,attr"`
	Flags			int64 `xml:"flags,attr"`
	Description		string `xml:"description,attr"`
	TourUrl			string `xml:"tourUrl,attr"`
	RoomName		string `xml:"roomName,attr"`
	ReceivingParty		string `xml:"receivingParty,attr"`
	HtPlaceName		string `xml:"htplaceName,attr"`

	CreateDate string

	DptCityId int
	CountryId int

	PriceByr int
	PriceEur int
	PriceUsd int

	FuelSurchargeMin	int
	FuelSurchargeMax	int
}

type TourBaseInterface interface {
	ToString() string
	FromString(source string) error
}

type TourInterface interface {
	ToString() string
	FromString(source string) error
	KeyData() string
	PriceData() string
	KeyDataCRC32() uint32
	GenId() int64
}

func (t *TourBase) ToString() string {
	tour_data := make([]string, TourBaseDataSize)

	for field, position := range tourIntFieldsMap() {
		tour_data[position] = strconv.FormatInt(
			reflect.ValueOf(t).Elem().FieldByName(field).Int(), 10,
		)
	}

	for field, position := range tourStringFieldsMap() {
		tour_data[position] = TourEscaped(
			reflect.ValueOf(t).Elem().FieldByName(field).String(),
			TourBaseDataSeparator, TourBaseDataSeparatorCode,
		)
	}

	for field, position := range tourRefIntFieldsMap() {
		elem := reflect.ValueOf(t).Elem().FieldByName(field)
		if elem.IsNil() {
			tour_data[position] = "-1"
		} else {
			tour_data[position] = strconv.FormatInt(elem.Elem().Int(), 10)
		}
	}

	return strings.Join(tour_data, TourBaseDataSeparator)
}

func (t *TourBase) FromString(source string) error {
	tour_data := strings.Split(source, TourBaseDataSeparator)

	if len(tour_data) != TourBaseDataSize {
		return fmt.Errorf(
			"Tour data size is wrong. Expected %d, got %d", TourBaseDataSize,
			len(tour_data),
		)
	}

	for field, position := range tourIntFieldsMap() {
		val, err := strconv.ParseInt(tour_data[position], 10, 64)
		if err != nil {
			return fmt.Errorf("Parse error for int '%s': '%s'", field, tour_data[position])
		}
		reflect.ValueOf(t).Elem().FieldByName(field).SetInt(val)
	}

	for field, position := range tourStringFieldsMap() {
		reflect.ValueOf(t).Elem().FieldByName(field).SetString(
			TourUnEscaped(tour_data[position], TourBaseDataSeparator, TourBaseDataSeparatorCode),
		)
	}

	if t.Kid1Age == nil {
		kidsAge := -1
		t.Kid1Age = &kidsAge
	}
	if t.Kid2Age == nil {
		kidsAge := -1
		t.Kid2Age = &kidsAge
	}
	if t.Kid3Age == nil {
		kidsAge := -1
		t.Kid3Age = &kidsAge
	}

	for field, position := range tourRefIntFieldsMap() {
		val, err := strconv.ParseInt(tour_data[position], 10, 64)
		if err != nil {
			return fmt.Errorf("Parse error for ref int '%s': '%s'", field, tour_data[position])
		}
		reflect.ValueOf(t).Elem().FieldByName(field).Elem().SetInt(val)
	}

	return nil
}

func TourEscaped(source string, symbol string, code string) string {
	return strings.Replace(source, symbol, code, -1)
}

func TourUnEscaped(source string, symbol string, code string) string {
	return strings.Replace(source, code, symbol, -1)
}

func tourIntFieldsMap() map[string]int {
	return map[string]int{
		"HotelId" 	: 0,
		"DptCityId" 	: 2,
		"Nights" 	: 3,
		"Adults" 	: 4,
		"MealId" 	: 5,
		"Kids" 		: 6,
		"SourceId" 	: 10,
		"Price" 	: 12,
		"CurrencyId"	: 13,
		"TownId"	: 14,
		"TicketsIncluded" 	: 16,
		"HasEconomTicketsDpt"	: 17,
		"HasEconomTicketsRtn"	: 18,
		"HotelIsInStop"		: 19,
		"RequestId"		: 20,
		"OfferId"		: 21,
		"FewEconomTicketsDpt"	: 22,
		"FewEconomTicketsRtn"	: 23,
		"FewPlacesInHotel"	: 24,
		"Flags"			: 25,
		"PriceByr"	: 31,
		"PriceEur"	: 32,
		"PriceUsd"	: 33,
		"FuelSurchargeMin"	: 34,
		"FuelSurchargeMax"	: 35,
	}
}

func tourRefIntFieldsMap() map[string]int {
	return map[string]int{
		"Kid1Age"	: 7,
		"Kid2Age"	: 8,
		"Kid3Age"	: 9,
	}
}

func tourStringFieldsMap() map[string]int {
	return map[string]int{
		"Checkin" 	: 1,
		"UpdateDate"	: 11,
		"MealName"	: 15,
		"Description"	: 26,
		"TourUrl"	: 27,
		"RoomName"	: 28,
		"ReceivingParty" : 29,
		"HtPlaceName"	: 30,
	}
}
