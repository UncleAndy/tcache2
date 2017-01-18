package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"time"
	"fmt"
	"github.com/uncleandy/tcache2/tours"
	"math/rand"
	"github.com/bouk/monkey"
	"reflect"
)

func init_workers() {
	worker_base.Workers = []worker_base.WorkerBaseInterface{
		&map_tours.MapToursWorker{},
		&partners_tours.PartnersToursWorker{},
	}

	map_tours_settings := worker_base.Workers[0].GetSettings()
	map_tours_settings.AllThreadsCount = 3
	map_tours_settings.WorkerFirstThreadId = 0
	map_tours_settings.WorkerThreadsCount = 3

	partners_tours_settings := worker_base.Workers[1].GetSettings()
	partners_tours_settings.AllThreadsCount = 2
	partners_tours_settings.WorkerFirstThreadId = 0
	partners_tours_settings.WorkerThreadsCount = 2
}

func random_tour_base() *tours.TourBase {
	kid1age := -1
	kid2age := -1
	kid3age := -1
	tour := tours.TourBase{
		SourceId: rand.Int(),
		UpdateDate: "2017-01-01 10:11:12",
		Price: 10000 + rand.Intn(10000),
		CurrencyId: 1,
		Checkin: fmt.Sprintf("2017-01-%0d", 1 + rand.Intn(30)),
		Nights: 5 + rand.Intn(10),
		Adults: 2,
		Kids: 0,
		Kid1Age: &kid1age,
		Kid2Age: &kid2age,
		Kid3Age: &kid3age,
		HotelId: 10000 + rand.Intn(100000),
		TownId:  10000 + rand.Intn(100000),
		MealId: rand.Intn(10),
		MealName: "Meal name",
		Hash: "",
		TicketsIncluded: 1,
		HasEconomTicketsDpt: 1,
		HasEconomTicketsRtn: 1,
		HotelIsInStop: 0,
		RequestId: 0,
		OfferId: 0,
		FewEconomTicketsDpt: 1,
		FewEconomTicketsRtn: 1,
		FewPlacesInHotel: 1,
		Flags: 0,
		Description: "Tour description",
		TourUrl: "http://site.com/tour1",
		RoomName: "Room name",
		ReceivingParty: "Receiving Party name",
		HtPlaceName: "Ht Place Name",

		CreateDate: "2017-01-01 00:00:00",

		DptCityId: 10000 + rand.Intn(100000),
		CountryId: 1 + rand.Intn(100),

		PriceByr: 10000 + rand.Intn(10000),
		PriceEur: 100 + rand.Intn(500),
		PriceUsd: 100 + rand.Intn(500),

		FuelSurchargeMin: 0,
		FuelSurchargeMax: 0,
	}
	return &tour
}

var sequence_map_crc32 uint64
var sequence_partners_crc32 uint64

func TestWorkerManagerLoop(t *testing.T) {
	init_test_redis_single()
	init_workers()

	tour1 := random_tour_base()
	tour2 := random_tour_base()
	tour3 := random_tour_base()
	tour4 := random_tour_base()
	tour5 := random_tour_base()

	cache.AddQueue(sletat.LoaderQueueToursName, tour1.ToString())
	cache.AddQueue(sletat.LoaderQueueToursName, tour2.ToString())
	cache.AddQueue(sletat.LoaderQueueToursName, tour3.ToString())
	cache.AddQueue(sletat.LoaderQueueToursName, tour4.ToString())
	cache.AddQueue(sletat.LoaderQueueToursName, tour5.ToString())

	monkey.PatchInstanceMethod(reflect.TypeOf(&tours.TourMap{}), "KeyDataCRC32",
		func(_ *tours.TourMap) uint64 {
			sequence_map_crc32++
			return sequence_map_crc32
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(&tours.TourPartners{}), "KeyDataCRC32",
		func(_ *tours.TourPartners) uint64 {
			sequence_partners_crc32++
			return sequence_partners_crc32
	})
	monkey.Patch(map_tours.IsSkipTour, func(_ *tours.TourMap) bool {
		return false
	})
	monkey.Patch(partners_tours.IsSkipTour, func(_ *tours.TourPartners) bool {
		return false
	})

	worker_base.ManagerLoop()

	for !cache.IsEmptyQueue(sletat.LoaderQueueToursName) {
		time.Sleep(1 * time.Second)
	}

	map_tours_queue_1 := fmt.Sprintf(map_tours.ThreadMapToursQueueTemplate, 1)
	map_tours_queue_2 := fmt.Sprintf(map_tours.ThreadMapToursQueueTemplate, 2)
	map_tours_queue_3 := fmt.Sprintf(map_tours.ThreadMapToursQueueTemplate, 0)

	len1, err1 := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours_queue_1).Result()
	if err1 != nil {
		t.Error("Error for read", map_tours_queue_1, "queue length:", err1)
	} else if len1 != 2 {
		t.Error("Wrong lenght queue", map_tours_queue_1, ". Expected 2, got", len1)
	}
	len2, err2 := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours_queue_2).Result()
	if err2 != nil {
		t.Error("Error for read", map_tours_queue_2, "queue length:", err2)
	} else if len2 != 2 {
		t.Error("Wrong lenght queue", map_tours_queue_2, ". Expected 2, got", len2)
	}
	len3, err3 := cache.RedisSettings.MainServers[0].Connection.LLen(map_tours_queue_3).Result()
	if err3 != nil {
		t.Error("Error for read", map_tours_queue_3, "queue length:", err3)
	} else if len3 != 1 {
		t.Error("Wrong lenght queue", map_tours_queue_3, ". Expected 1, got", len3)
	}

	partners_tours_queue_1 := fmt.Sprintf(partners_tours.ThreadPartnersToursQueueTemplate, 1)
	partners_tours_queue_2 := fmt.Sprintf(partners_tours.ThreadPartnersToursQueueTemplate, 0)

	plen1, perr1 := cache.RedisSettings.MainServers[0].Connection.LLen(partners_tours_queue_1).Result()
	if perr1 != nil {
		t.Error("Error for read", partners_tours_queue_1, "queue length:", perr1)
	} else if plen1 != 3 {
		t.Error("Wrong lenght queue", partners_tours_queue_1, ". Expected 3, got", plen1)
	}
	plen2, perr2 := cache.RedisSettings.MainServers[0].Connection.LLen(partners_tours_queue_2).Result()
	if perr2 != nil {
		t.Error("Error for read", partners_tours_queue_2, "queue length:", perr2)
	} else if plen2 != 2 {
		t.Error("Wrong lenght queue", partners_tours_queue_2, ". Expected 2, got", plen2)
	}

	cache.CleanQueue(sletat.LoaderQueueToursName)
	cache.CleanQueue(map_tours_queue_1)
	cache.CleanQueue(map_tours_queue_2)
	cache.CleanQueue(map_tours_queue_3)
	cache.CleanQueue(partners_tours_queue_1)
	cache.CleanQueue(partners_tours_queue_2)
}