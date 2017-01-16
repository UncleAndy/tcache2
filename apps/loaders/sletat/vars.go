package sletat

import "github.com/uncleandy/tcache2/db"

var (
	sletatSettings 		SletatSettings

	departCitiesActiveIds 	[]int
	operators          	map[int]db.SletatOperator
)
