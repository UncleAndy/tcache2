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

func (t *TourPartners) KeyData() string {
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
		strconv.Itoa(t.SourceId),
		strconv.Itoa(t.CountryId),
		strconv.Itoa(t.TownId),
		strconv.Itoa(t.Adults),
		t.Checkin,
		strconv.Itoa(t.Nights),
		strconv.Itoa(t.Kids),
		strconv.Itoa(kid1age),
		strconv.Itoa(kid2age),
		strconv.Itoa(kid3age),
		strconv.Itoa(t.DptCityId),
	}
	return strings.Join(key_data, TourPartnersKeyDataSeparator)
}

func (t *TourPartners) PriceData() string {
	price_data := []string{
		strconv.Itoa(t.Price),
		t.UpdateDate,
		strconv.Itoa(t.FuelSurchargeMin),
		strconv.Itoa(t.FuelSurchargeMax),
		strconv.Itoa(t.TicketsIncluded),
		strconv.Itoa(t.HasEconomTicketsDpt),
		strconv.Itoa(t.HasEconomTicketsRtn),
		strconv.Itoa(t.HotelIsInStop),

		strconv.Itoa(t.FewEconomTicketsDpt),
		strconv.Itoa(t.FewEconomTicketsRtn),
		strconv.Itoa(t.FewPlacesInHotel),
		strconv.FormatInt(t.Flags, 10),

		strconv.Itoa(t.HotelId),
		strconv.Itoa(t.MealId),
		strconv.Itoa(t.RequestId),
		strconv.FormatInt(t.OfferId, 10),

		TourEscaped(t.MealName,TourPartnersKeyDataSeparator, TourPartnersKeyDataSeparatorCode),
		TourEscaped(t.RoomName, TourPartnersKeyDataSeparator, TourPartnersKeyDataSeparatorCode),
		TourEscaped(t.HtPlaceName, TourPartnersKeyDataSeparator, TourPartnersKeyDataSeparatorCode),
		TourEscaped(t.TourUrl, TourPartnersKeyDataSeparator, TourPartnersKeyDataSeparatorCode),
		TourEscaped(t.Description, TourPartnersKeyDataSeparator, TourPartnersKeyDataSeparatorCode),
		TourEscaped(t.ReceivingParty, TourPartnersKeyDataSeparator, TourPartnersKeyDataSeparatorCode),
	}
	return strings.Join(price_data, TourPartnersKeyDataSeparator)
}

func (t *TourPartners) KeyDataCRC32() uint64 {
	key_data := t.KeyData()
	return uint64(crc32.ChecksumIEEE([]byte(key_data)))
}


func (t *TourPartners) GenId() (uint64, error) {
	return cache.NewID(TourPartnersRedisGenIdKey)
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
	old_update_time, err := time.Parse("2015-03-07 11:06:39", price_data[1])
	if err != nil {
		return false, err
	}

	new_update_time, err := time.Parse("2015-03-07 11:06:39", t.UpdateDate)
	if err != nil {
		return false, err
	}

	return new_update_time.After(old_update_time), nil
}
