package db

import (
	"fmt"
	"github.com/uncleandy/tcache2/log"
)

func QueryCitiesIds(where string) ([]int, error) {
	CheckConnect()

	sql := "SELECT sletat_city_id FROM sletat_cities"
	if where != "" {
		sql = fmt.Sprintf("SELECT sletat_city_id FROM sletat_cities WHERE %s", where)
	}

	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	var cityId int
	citiesIds := make([]int, 0)

	for rows.Next() {
		err = rows.Scan(&cityId)
		if err != nil {
			log.Error.Println(err)
		}

		citiesIds = append(citiesIds, cityId)
	}

	return citiesIds, nil
}
