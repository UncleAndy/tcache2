package sletat

import (
	"github.com/uncleandy/tcache2/db"
	"github.com/uncleandy/tcache2/log"
)

func PrepareData() {
	loadDepartCities()
	loadOperators()
}

func loadDepartCities() {
	var err error

	departCitiesActiveIds, err = db.QueryDepartCitiesIds("active OR active_for_partners")
	if err != nil {
		log.Error.Fatal(err)
	}
}

func loadOperators() {
	rawOperators, err := db.QueryOperators("")
	if err != nil {
		log.Error.Fatal(err)
	}

	operators = make(map[int]db.SletatOperator)
	for _, rawOperator := range rawOperators {
		operators[rawOperator.Id] = rawOperator
	}
}

func IsDepartCityActive(id int) bool {
	return db.IsInListInt(departCitiesActiveIds, id)
}

func IsOperatorActive(id int) bool {
	operator, present := operators[id]
	if !present {
		return false
	}
	return operator.Active
}
