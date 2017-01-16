package sletat

import (
	"github.com/uncleandy/tcache2/tours"
)

func IsSkipTour(tour *tours.TourBase) bool {
	if !IsOperatorActive(tour.SourceId) {
		return true
	}

	if !isFullInfo(tour) {
		return true
	}

	if !isKidsValid(tour) {
		return true
	}

	return false
}

func isFullInfo(tour *tours.TourBase) bool {
	return (tour.TicketsIncluded == 1 &&
		(tour.HasEconomTicketsDpt == 1 || tour.HasEconomTicketsDpt == 2) &&
		(tour.HasEconomTicketsRtn == 1 || tour.HasEconomTicketsRtn == 2) &&
		(tour.HotelIsInStop == 0 || tour.HotelIsInStop == 2) &&
		tour.HotelId != 0)
}

func isKidsValid(tour *tours.TourBase) bool {
	kids := 0

	if tour.Kid1Age != nil && *tour.Kid1Age >= 0 {
		kids++
	}

	if tour.Kid2Age != nil && *tour.Kid2Age >= 0 {
		kids++
	}

	if tour.Kid3Age != nil && *tour.Kid3Age >= 0 {
		kids++
	}

	if tour.Kids == kids {
		return true
	}

	return false
}
