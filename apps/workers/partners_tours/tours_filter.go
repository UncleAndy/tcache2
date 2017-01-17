package partners_tours

import "github.com/uncleandy/tcache2/tours"

func IsSkipTour(tour *tours.TourPartners) bool {
	return !IsTownGood(tour.TownId) || !IsDepartCityGood(tour.DptCityId) || !IsHotelGood(tour.HotelId)
}
