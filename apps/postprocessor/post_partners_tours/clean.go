package post_partners_tours

import (
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/log"
	"time"
	"strconv"
)

const (
	PartnerToursUpdateTimeExpired = 8 * time.Hour
)

var (
	BadToursForAnalyze = 0
	DeletedKeysCount = 0
)

func (post_worker *PostPartnersToursWorker) CheckClean(id uint64) {
	// Clean old tours

	tour := tours.TourPartners{}

	// Load tour data from cache
	key_data, err := cache.Get(id, fmt.Sprintf(partners_tours.PartnersTourKeyDataKeyTemplate, id))
	if err != nil {
		BadToursForAnalyze++
		log.Error.Print("WARNING! Can not read KEY DATA for partners tour id ", id)
	} else {
		tour.FromKeyData(key_data)
	}

	price_data, err := cache.Get(id, fmt.Sprintf(partners_tours.PartnersTourPriceDataKeyTemplate, id))
	if err != nil {
		BadToursForAnalyze++
		log.Error.Print("WARNING! Can not read PRICE DATA for partners tour id ", id)
	} else {
		tour.FromPriceData(price_data)
	}

	if tour.UpdateDate != "" {
		compare_time, err := time.Parse("2006-01-02 15:04:05", tour.UpdateDate)
		if err != nil {
			BadToursForAnalyze++
			log.Error.Print("Wrong time_str param in PriceLogAfterTime: ", tour.UpdateDate, "\n", err)
		}
		compare_time_unix := compare_time.Add(PartnerToursUpdateTimeExpired).UTC().Unix()

		if compare_time_unix <= time.Now().UTC().Unix() {
			WorkerKeysCountMutex.Lock()
			DeletedKeysCount++
			WorkerKeysCountMutex.Unlock()

			cache.AddQueue(partners_tours.PartnersTourDeleteQueue, strconv.FormatUint(id, 10))
		}
	}
}