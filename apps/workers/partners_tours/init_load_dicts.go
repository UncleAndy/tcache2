package partners_tours

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/log"
)

var (
	activeTownsIds []int
	activeNamePicturesHotelsIds []int
	activeDepartCitiesIds []int
)

func (worker *PartnersToursWorker) LoadDictData() {
	var err error

	activeTownsIds, err = db.QueryCitiesIds("active")
	if err != nil {
		log.Error.Fatal("Can not read SletatCities data.")
	}

	activeNamePicturesHotelsIds, err = db.QueryHotelsIds(
		"active AND images_count > 0 AND name IS NOT NULL AND name != ''",
	)
	if err != nil {
		log.Error.Fatal("Can not read SletatHotels data.")
	}

	activeDepartCitiesIds, err = db.QueryDepartCitiesIds("active OR active_for_partners")
	if err != nil {
		log.Error.Fatal(err)
	}
}

func IsTownGood(id int) bool {
	return db.IsInListInt(activeTownsIds, id)
}

func IsHotelGood(id int) bool {
	return db.IsInListInt(activeNamePicturesHotelsIds, id)
}

func IsDepartCityGood(id int) bool {
	return db.IsInListInt(activeDepartCitiesIds, id)
}
