package tests

import (
	"testing"
	"strings"
	"github.com/uncleandy/tcache2/tours"
)

func TestTourToString(t *testing.T) {
	tour := tours.TourBase{}

	tour_data := []string{
		"1",
		"2017-01-01",
		"2",
		"10",
		"2",
		"3",
		"2",
		"3",
		"5",
		"-1",
		"6",
		"2016-12-10",
		"20000",
		"1",
		"34",
		"7",
		"Regular Meal",
		"8",
		"9",
		"10",
		"11",
		"12",
		"13",
		"14",
		"15",
		"16",
		"17",
		"Description text",
		"http://site.com/tour1",
		"Room text",
		"ReceivingParty &#124; text",
		"HtPlaceName &#124; text &#124; test &#124;",
		"40000",
		"500",
		"600",
		"18",
		"19",
	}
	tour_string := strings.Join(tour_data, tours.TourBaseDataSeparator)

	tour.FromString(tour_string)

	tour_gen_string := tour.ToString()

	if tour_gen_string != tour_string {
		t.Error("Tour string value error. Expected:\n", tour_string, "\ngot:\n", tour_gen_string)
	}
}
