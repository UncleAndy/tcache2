package map_tours

import (
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"time"
	"github.com/uncleandy/tcache2/log"
	"fmt"
	"gopkg.in/redis.v4"
)

const (
	ThreadMapToursQueueTemplate = "tours_download_list_%d"
	TourIDKeyTemplate = "mtk:%s"
	TourKeyDataKeyTemplate = "mtkk:%d"
	TourPriceDataKeyTemplate = "mtp:%d"
	TourPriceLogKeyTemplate = "mtl:%d"
)

func (worker *MapToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	worker.InitThreads()

	// ToursManager run only for first worker
	if worker.Settings.WorkerFirstThreadId == 0 {
		worker.ToursManager()
	}
}

func (worker *MapToursWorker) InitThreads() {
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		worker.Thread(worker.Settings.WorkerFirstThreadId + i)
	}
}

func (worker *MapToursWorker) ToursManager() {
	// Scan Redis tours loader queue & move tours to worker threads Redis queue
	go func() {
		tour := tours.TourMap{}
		for true {
			tour_str, err := cache.GetQueue(sletat.LoaderQueueToursName)
			if err != nil || tour_str == "" {
				time.Sleep(1 * time.Second)
				continue
			}

			err = tour.FromString(tour_str)
			if err != nil {
				log.Error.Print("Load tour from loader queue error:", err)
				continue
			}

			if IsSkipTour(&tour) {
				continue
			}

			crc := tour.KeyDataCRC32()
			thread_index := crc % worker.Settings.AllThreadsCount
			thread_queue := fmt.Sprintf(ThreadMapToursQueueTemplate, thread_index)

			cache.AddQueue(thread_queue, tour_str)
		}
	}()
}

func (worker *MapToursWorker) Thread(thread_index int) {
	go func() {
		thread_queue := fmt.Sprintf(ThreadMapToursQueueTemplate, thread_index)
		tour := tours.TourMap{}
		for true {
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

	id_tour, err := cache.Get(crc, fmt.Sprintf(TourIDKeyTemplate, tour.KeyData()))
	if err != nil && err != redis.Nil {
		log.Error.Print(
			"Error read map tour from key ",
			fmt.Sprintf(TourIDKeyTemplate, tour.KeyData()),
			":",
			err,
		)
	}

	if err != nil {
		// Add new tour
		id_tour, err = tour.GenId()
		if err != nil {
			log.Error.Fatal("Error GenID for tour:", err)
		}

		cache.Set(crc, fmt.Sprintf(TourIDKeyTemplate, tour.KeyData()), id_tour)
		cache.Set(id_tour, fmt.Sprintf(TourKeyDataKeyTemplate, id_tour), tour.KeyData())
		cache.Set(id_tour, fmt.Sprintf(TourPriceDataKeyTemplate, id_tour), tour.PriceData())
	} else {
		// Compare old price with new price
		old_price_data, err := cache.Get(id_tour, fmt.Sprintf(TourPriceDataKeyTemplate, id_tour))
		if err != nil {
			log.Error.Fatal("Error read PriceData for tour ", id_tour, ":", err)
		}

		is_bigger, err := tour.PriceBiggerThen(old_price_data)
		if err == nil {
			if is_bigger {
				cache.RPush(id_tour, fmt.Sprintf(TourPriceLogKeyTemplate, id_tour), tour.PriceData())
			} else {
				// Save to price data
				cache.Set(id_tour, fmt.Sprintf(TourPriceDataKeyTemplate, id_tour), tour.PriceData())
			}
		} else {
			log.Error.Fatal("Error compare prices:", err)
		}
	}
}