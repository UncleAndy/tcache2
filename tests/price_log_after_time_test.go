package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/apps/postprocessor/post_map_tours_price_logs"
)

func TestPriceLogAfterTime(t *testing.T) {
	tour0 := random_tour_map()
	tour1 := random_tour_map()
	tour2 := random_tour_map()
	tour3 := random_tour_map()
	price_log := []string{}

	tour0.UpdateDate = "2017-01-01 10:00:00"
	tour0.Price = 10000
	price_log = append(price_log, tour0.PriceData())

	tour1.UpdateDate = "2017-01-01 12:00:00"
	tour1.Price = 15000
	price_log = append(price_log, tour1.PriceData())

	tour2.UpdateDate = "2017-01-03 08:00:00"
	tour2.Price = 5000
	price_log = append(price_log, tour2.PriceData())

	tour3.UpdateDate = "2017-01-20 01:00:00"
	tour3.Price = 1000
	price_log = append(price_log, tour3.PriceData())

	new_price_log1 := post_map_tours_price_logs.PriceLogAfterTime(price_log, "2017-01-01 08:00:00")
	if len(new_price_log1) != len(price_log) {
		t.Error("Wrong result price log lenght. Expected:", len(price_log),
			", got", len(new_price_log1))
	}

	new_price_log2 := post_map_tours_price_logs.PriceLogAfterTime(price_log, "2017-01-01 10:00:00")
	if len(new_price_log2) != (len(price_log) - 1) {
		t.Error("Wrong result price log lenght. Expected:", (len(price_log) - 1),
			", got", len(new_price_log2))
	} else {
		if new_price_log2[0] != tour1.PriceData() {
			t.Error("Wrong price_log[0]. Expected:", tour1.PriceData(),
				", got", new_price_log2[0])
		}
		if new_price_log2[1] != tour2.PriceData() {
			t.Error("Wrong price_log[1]. Expected:", tour2.PriceData(),
				", got", new_price_log2[1])
		}
		if new_price_log2[2] != tour3.PriceData() {
			t.Error("Wrong price_log[2]. Expected:", tour3.PriceData(),
				", got", new_price_log2[2])
		}
	}

	new_price_log3 := post_map_tours_price_logs.PriceLogAfterTime(price_log, "2017-01-03 08:00:00")
	if len(new_price_log3) != (len(price_log) - 3) {
		t.Error("Wrong result price log lenght. Expected:", (len(price_log) - 3),
			", got", len(new_price_log3))
	} else {
		if new_price_log3[0] != tour3.PriceData() {
			t.Error("Wrong price_log[0]. Expected:", tour3.PriceData(),
				", got", new_price_log3[0])
		}
	}

	new_price_log4 := post_map_tours_price_logs.PriceLogAfterTime(price_log, "2017-01-20 01:00:00")
	if len(new_price_log4) != 0 {
		t.Error("Wrong result price log lenght. Expected: 0",
			", got", len(new_price_log4))
	}
}