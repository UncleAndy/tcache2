package db

import (
	"github.com/uncleandy/tcache2/log"
	"fmt"
)

func QueryDepartCities(where string) ([]int, error) {
	CheckConnect()

	sql := "SELECT sletat_depart_city_id FROM sletat_depart_cities"
	if where != "" {
		sql = fmt.Sprintf("SELECT sletat_depart_city_id FROM sletat_depart_cities WHERE %s", where)
	}

	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var cityId int
	citiesIds := make([]int, 0)

	for rows.Next() {
		err = rows.Scan(&cityId)
		if err != nil {
			log.Error.Println(err)
		}

		citiesIds = append(citiesIds, cityId)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return citiesIds, nil
}