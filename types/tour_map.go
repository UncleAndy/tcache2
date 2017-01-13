package types

import (
	"strconv"
	"strings"
	"hash/crc32"
	"github.com/uncleandy/tcache2/cache"
)

const (
	TourMapKeyDataSeparator = "|"
	TourMapKeyDataSeparatorCode = "&#124;"
	TourMapRedisGenIdKey = "serial_map_tour"
)

type TourMap struct {
	TourBase
}

func (t TourMap) KeyData() string {
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

func (t TourMap) PriceData() string {
	price_data := []string{
		strconv.Itoa(t.Price),
		t.CreateDate,
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

func (t TourMap) KeyDataCRC32() uint32 {
	key_data := t.KeyData()
	return crc32.ChecksumIEEE([]byte(key_data))
}

func (t TourMap) GenId() (int64, error) {
	return cache.GenID(TourMapRedisGenIdKey)
}
