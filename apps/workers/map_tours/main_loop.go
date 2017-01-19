package map_tours

import (
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
	"time"
	"github.com/uncleandy/tcache2/log"
	"fmt"
	"gopkg.in/redis.v4"
	"strconv"
)

const (
	ThreadMapToursQueueTemplate = "map_tours_download_list_%d"
	MapTourIDKeyTemplate = "mtk:%s"
	MapTourKeyDataKeyTemplate = "mtkk:%d"
	MapTourPriceDataKeyTemplate = "mtp:%d"
	MapTourPriceLogKeyTemplate = "mtl:%d"
	MapTourInsertQueue = "map_tours_insert"
	MapTourUpdateQueue = "map_tours_update"
)

var (
	ForceStopThreads = false
)

func (worker *MapToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	worker.InitThreads()
}

func (worker *MapToursWorker) InitThreads() {
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		worker.Thread(worker.Settings.WorkerFirstThreadId + i)
	}
}

func (worker *MapToursWorker) SendTour(tour_str string) {
	tour := tours.TourMap{}

	err := tour.FromString(tour_str)
	if err != nil {
		log.Error.Print("Load tour from loader queue error:", err)
		return
	}

	if IsSkipTour(&tour) {
		return
	}

	crc := tour.KeyDataCRC32()
	thread_index := crc % uint64(worker.Settings.AllThreadsCount)
	thread_queue := fmt.Sprintf(ThreadMapToursQueueTemplate, thread_index)

	err = cache.AddQueue(thread_queue, tour_str)
	if err != nil {
		log.Error.Print("AddQueue error: ", err)
	}
}

func (worker *MapToursWorker) Thread(thread_index int) {
	go func() {
		thread_queue := fmt.Sprintf(ThreadMapToursQueueTemplate, thread_index)
		tour := tours.TourMap{}
		for !ForceStopThreads {
			tour_str, err := cache.GetQueue(thread_queue)
			if err != nil || tour_str == "" {
				time.Sleep(1 * time.Second)
				continue
			}

			err = tour.FromString(tour_str)
			if err != nil {
				log.Error.Print("Load tour from loader queue error:", err)
				continue
			}

			worker.TourProcess(&tour)
		}
	}()
}

func (worker *MapToursWorker) TourProcess(tour *tours.TourMap) {
	crc := tour.KeyDataCRC32()

	id_tour_str, err := cache.Get(crc, fmt.Sprintf(MapTourIDKeyTemplate, tour.KeyData()))
	if err != nil && err != redis.Nil {
		log.Error.Print(
			"Error read map tour from key ",
			fmt.Sprintf(MapTourIDKeyTemplate, tour.KeyData()),
			":",
			err,
		)
	}

	if err != nil {
		// Add new tour
		id_tour, err := tour.GenId()
		if err != nil {
			log.Error.Fatal("Error GenID for tour:", err)
		}

		cache.Set(crc,
			fmt.Sprintf(MapTourIDKeyTemplate, tour.KeyData()), strconv.FormatUint(id_tour, 10))
		cache.Set(id_tour,
			fmt.Sprintf(MapTourKeyDataKeyTemplate, id_tour), tour.KeyData())
		cache.Set(id_tour,
			fmt.Sprintf(MapTourPriceDataKeyTemplate, id_tour), tour.PriceData())
		worker.ToInsertQueue(id_tour)
	} else {
		// Compare old price with new price
		id_tour, err := strconv.ParseUint(id_tour_str, 10, 64)
		if err != nil {
			log.Error.Print(
				"Error parse map tour id from key ",
				fmt.Sprintf(MapTourIDKeyTemplate, tour.KeyData()),
				":",
				err,
			)
		}

		old_price_data, err_price := cache.Get(id_tour, fmt.Sprintf(MapTourPriceDataKeyTemplate, id_tour))
		if err_price != nil && err_price != redis.Nil {
			log.Error.Fatal("Error read PriceData for tour ", id_tour, ":", err)
		}

		is_bigger, err := tour.PriceBiggerThen(old_price_data)
		if err == nil || err_price == redis.Nil {
			if is_bigger && err_price != redis.Nil {
				cache.RPush(id_tour, fmt.Sprintf(MapTourPriceLogKeyTemplate, id_tour), tour.PriceData())
			} else {
				// Save to price data
				cache.Set(id_tour, fmt.Sprintf(MapTourPriceDataKeyTemplate, id_tour), tour.PriceData())
			}
			worker.ToUpdateQueue(id_tour)
		} else {
			log.Error.Fatal("Error compare prices:", err)
		}
	}
}

func (worker *MapToursWorker) IsPrimary() bool {
	return worker.Settings.WorkerFirstThreadId == 0
}

func (worker *MapToursWorker) ToInsertQueue(id uint64) error {
	return cache.AddQueue(MapTourInsertQueue, strconv.FormatUint(id, 10))
}

func (worker *MapToursWorker) ToUpdateQueue(id uint64) error {
	return cache.AddQueue(MapTourUpdateQueue, strconv.FormatUint(id, 10))
}
