package tours

import (
	"strconv"
	"strings"
	"fmt"
	"reflect"
	"github.com/hjr265/redsync.go/redsync"
	"github.com/uncleandy/tcache2/cache"
)

const (
	TourBaseDataSeparator = "|"
	TourBaseDataSeparatorCode = "&#124;"
	TourBaseDataSize = 37
)

var (
	TourBaseDataFields = DataOrderFields{
		StringFields	: map[string]int{
			"Checkin" 	: 1,
			"UpdateDate"	: 11,
			"MealName"	: 16,
			"Description"	: 27,
			"TourUrl"	: 28,
			"RoomName"	: 29,
			"ReceivingParty" : 30,
			"HtPlaceName"	: 31,
		},
		IntFields	: map[string]int{
			"HotelId" 	: 0,
			"DptCityId" 	: 2,
			"Nights" 	: 3,
			"Adults" 	: 4,
			"MealId" 	: 5,
			"Kids" 		: 6,
			"SourceId" 	: 10,
			"Price" 	: 12,
			"CurrencyId"	: 13,
			"CountryId"	: 14,
			"TownId"	: 15,
			"TicketsIncluded" 	: 17,
			"HasEconomTicketsDpt"	: 18,
			"HasEconomTicketsRtn"	: 19,
			"HotelIsInStop"		: 20,
			"RequestId"		: 21,
			"OfferId"		: 22,
			"FewEconomTicketsDpt"	: 23,
			"FewEconomTicketsRtn"	: 24,
			"FewPlacesInHotel"	: 25,
			"Flags"			: 26,
			"PriceByr"	: 32,
			"PriceEur"	: 33,
			"PriceUsd"	: 34,
			"FuelSurchargeMin"	: 35,
			"FuelSurchargeMax"	: 36,
		},
		RefIntFields	: map[string]int{
			"Kid1Age"	: 7,
			"Kid2Age"	: 8,
			"Kid3Age"	: 9,
		},
	}
)

type DataOrderFields struct {
	StringFields 	map[string]int
	IntFields 	map[string]int
	RefIntFields	map[string]int
}

type DataSQLFields struct {
	IdSQLField	string
	StringFields 	map[string]string
	IntFields 	map[string]string
	RefIntFields	map[string]string
}

type TourBase struct {
	Id 			uint64
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
	KeyDataCRC32() uint64
	GenId() int64
}

func (t *TourBase) ToString() string {
	return t.FieldsToString(&TourBaseDataFields)
}

func (t *TourBase) FromString(source string) error {
	return t.FieldsFromString(source, &TourBaseDataFields)
}

func (t *TourBase) FieldsFromString(data_str string, fields_order *DataOrderFields ) error {
	tour_data := strings.Split(data_str, TourBaseDataSeparator)

	for field, position := range fields_order.IntFields {
		val, err := strconv.ParseInt(tour_data[position], 10, 64)
		if err != nil {
			return fmt.Errorf("Parse error for int '%s': '%s'", field, tour_data[position])
		}
		reflect.ValueOf(t).Elem().FieldByName(field).SetInt(val)
	}

	for field, position := range fields_order.StringFields {
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

	for field, position := range fields_order.RefIntFields {
		val, err := strconv.ParseInt(tour_data[position], 10, 64)
		if err != nil {
			return fmt.Errorf("Parse error for ref int '%s': '%s'", field, tour_data[position])
		}
		reflect.ValueOf(t).Elem().FieldByName(field).Elem().SetInt(val)
	}

	return nil
}

func (t *TourBase) FieldsToString(fields_order *DataOrderFields ) string {
	fields_data := make([]string,
		len(fields_order.IntFields) + len(fields_order.StringFields) + len(fields_order.RefIntFields))

	for field, position := range fields_order.IntFields {
		fields_data[position] = strconv.FormatInt(
			reflect.ValueOf(t).Elem().FieldByName(field).Int(), 10,
		)
	}

	for field, position := range fields_order.StringFields {
		fields_data[position] = TourEscaped(
			reflect.ValueOf(t).Elem().FieldByName(field).String(),
			TourBaseDataSeparator, TourBaseDataSeparatorCode,
		)
	}

	for field, position := range fields_order.RefIntFields {
		elem := reflect.ValueOf(t).Elem().FieldByName(field)
		if elem.IsNil() {
			fields_data[position] = "-1"
		} else {
			fields_data[position] = strconv.FormatInt(elem.Elem().Int(), 10)
		}
	}

	return strings.Join(fields_data, TourBaseDataSeparator)
}


func (t *TourBase) InsertSQLFieldsSetBy(fields_set *DataSQLFields) string {
	result := ""
	sep := ""

	if fields_set.IdSQLField != "" {
		result = fields_set.IdSQLField
		sep = ","
	}

	for _, db_field := range fields_set.IntFields {
		result = result + sep + " " + db_field
		sep = ","
	}

	for _, db_field := range fields_set.RefIntFields {
		result = result + sep + " " + db_field
		sep = ","
	}

	for _, db_field := range fields_set.StringFields {
		result = result + sep + " " + db_field
		sep = ","
	}

	return result
}

func (t *TourBase) InsertSQLDataSetBy(fields_set *DataSQLFields) string {
	result := ""
	sep := ""

	if fields_set.IdSQLField != "" {
		result = strconv.FormatUint(t.Id, 10)
		sep = ","
	}

	for field, _ := range fields_set.IntFields {
		value := reflect.ValueOf(t).Elem().FieldByName(field).Int()
		result = result + sep + " " + strconv.FormatInt(value, 10)
		sep = ","
	}

	for field, _ := range fields_set.RefIntFields {
		elem := reflect.ValueOf(t).Elem().FieldByName(field)
		value := "-1"
		if !elem.IsNil() {
			value = strconv.FormatInt(elem.Elem().Int(), 10)
		}
		result = result + sep + " " + value
		sep = ","
	}

	for field, _ := range fields_set.StringFields {
		value := reflect.ValueOf(t).Elem().FieldByName(field).String()
		result = result + sep + " \"" + value + "\""
		sep = ","
	}

	return result
}

func (t *TourBase) UpdateSQLStringBy(fields_set *DataSQLFields) string {
	result := ""
	sep := ""

	for field, db_field := range fields_set.IntFields {
		value := reflect.ValueOf(t).Elem().FieldByName(field).Int()
		result = result + sep + " " + db_field + " = " + strconv.FormatInt(value, 10)
		sep = ","
	}

	for field, db_field := range fields_set.RefIntFields {
		elem := reflect.ValueOf(t).Elem().FieldByName(field)
		value := "-1"
		if !elem.IsNil() {
			value = strconv.FormatInt(elem.Elem().Int(), 10)
		}
		result = result + sep + " " + db_field + " = " + value
		sep = ","
	}

	for field, db_field := range fields_set.StringFields {
		value := reflect.ValueOf(t).Elem().FieldByName(field).String()
		result = result + sep + " " + db_field + " = \"" + value + "\""
		sep = ","
	}

	return result
}

func TourEscaped(source string, symbol string, code string) string {
	return strings.Replace(source, symbol, code, -1)
}

func TourUnEscaped(source string, symbol string, code string) string {
	return strings.Replace(source, code, symbol, -1)
}

func LockTourUpdate(template string, id uint64) *redsync.Mutex {
	mutex, err := cache.NewMutex(fmt.Sprintf(template, id))
	if err != nil {
		return nil
	}
	mutex.Lock()
	return mutex
}
