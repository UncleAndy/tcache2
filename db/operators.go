package db

import "github.com/uncleandy/tcache2/log"

type SletatOperator struct {
	Id              int
	ExchangeRateUsd float64
	ExchangeRateEur float64
	ExchangeRateRur float64
	Active		bool
}

func QueryOperators(where string) ([]SletatOperator, error) {
	CheckConnect()

	rows, err := db.Query(`
	SELECT
		sletat_tour_operator_id,
		exchange_rate_usd,
		exchange_rate_eur,
		exchange_rate_rur,
		active
	FROM
	sletat_tour_operators`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	operators := []SletatOperator{}

	for rows.Next() {
		var id int
		var exchangeRateUsd interface{}
		var exchangeRateEur interface{}
		var exchangeRateRur interface{}
		var active bool

		err = rows.Scan(
			&id,
			&exchangeRateUsd,
			&exchangeRateEur,
			&exchangeRateRur,
			&active,
		)
		if err != nil {
			log.Error.Println(err)
		}

		operator := SletatOperator{
			Id:              id,
			ExchangeRateUsd: parseExchangeRateValue(exchangeRateUsd),
			ExchangeRateEur: parseExchangeRateValue(exchangeRateEur),
			ExchangeRateRur: parseExchangeRateValue(exchangeRateRur),
			Active:		 active,
		}

		operators = append(operators, operator)
	}

	return operators, nil
}

func parseExchangeRateValue(v interface{}) float64 {
	switch v.(type) {
	case float64:
		return v.(float64)
	default:
		return 0
	}
}
