package map_tours

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/log"
)

var (
	activeTownsIds []int
	activeNamePicturesHotelsIds []int
	activeDepartCitiesIds []int
)

func (worker *MapToursWorker) LoadDictData() {
	log.Info.Println("Start load dict data...")
	var err error

	activeTownsIds, err = db.QueryCitiesIds("active")
	if err != nil {
		log.Error.Fatal("Can not read SletatCities data.", err)
	}

	activeNamePicturesHotelsIds, err = db.QueryHotelsIds(
		"active AND images_count > 0 AND name IS NOT NULL AND name != ''",
	)
	if err != nil {
		log.Error.Fatal("Can not read SletatHotels data.", err)
	}

	activeDepartCitiesIds, err = db.QueryDepartCitiesIds("active OR active_for_partners")
	if err != nil {
		log.Error.Fatal(err)
	}
	log.Info.Println("Finish load dict data.")
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
