package db

import (
	"fmt"
	"github.com/uncleandy/tcache2/log"
)

func QueryHotelsIds(where string) ([]int, error) {
	CheckConnect()

	sql := "SELECT sletat_hotel_id FROM sletat_hotels"
	if where != "" {
		sql = fmt.Sprintf("SELECT sletat_hotel_id FROM sletat_hotels WHERE %s", where)
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
	hotelsIds := make([]int, 0)

	for rows.Next() {
		err = rows.Scan(&cityId)
		if err != nil {
			log.Error.Println(err)
		}

		hotelsIds = append(hotelsIds, cityId)
	}

	return hotelsIds, nil
}
