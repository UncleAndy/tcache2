package map_tours

import "github.com/uncleandy/tcache2/tours"

func IsSkipTour(tour *tours.TourMap) {
	return !IsTownGood(tour.TownId) || !IsDepartCityGood(tour.DptCityId) || !IsHotelGood(tour.HotelId)
}
