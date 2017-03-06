package partners_tours

import (
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
	"time"
	"github.com/uncleandy/tcache2/log"
	"fmt"
	"gopkg.in/redis.v4"
	"strconv"
	"sync"
)

const (
	ThreadPartnersToursQueueTemplate = "partners_tours_download_list_%d"
	PartnersTourIDKeyTemplate = "ptk:%s"
	PartnersTourKeyDataKeyTemplate = "ptkk:%d"
	PartnersTourPriceDataKeyTemplate = "ptp:%d"
	PartnersTourInsertQueue = "partners_tours_insert"
	PartnersTourUpdateQueue = "partners_tours_update"
	PartnersTourDeleteQueue = "partners_tours_delete"
	PartnersTourUpdateMutexTemplate = "partners_update_%d"
	PartnersTourBatchSize = 100
)

var (
	ForceStopThreads = false
)

func (worker *PartnersToursWorker) Stop() {
	ForceStopThreads = true
}

func (worker *PartnersToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	worker.InitThreads()
}

func (worker *PartnersToursWorker) InitThreads() {
	wg := sync.WaitGroup{}
	wg.Add(worker.Settings.WorkerThreadsCount)
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		thread_index := worker.Settings.WorkerFirstThreadId + i
		go func() {
			worker.Thread(thread_index)
			wg.Done()
		}()
	}

	wg.Wait()
	worker.FinishChanel <- true
}

func (worker *PartnersToursWorker) SendTour(tour_str string) {
	tour := tours.TourPartners{}

	err := tour.FromString(tour_str)
	if err != nil {
		log.Error.Print("Load tour from loader queue error:", err)
		return
	}

	crc := tour.KeyDataCRC32()
	thread_index := crc % uint64(worker.Settings.AllThreadsCount)
	thread_queue := fmt.Sprintf(ThreadPartnersToursQueueTemplate, thread_index)

	cache.AddQueue(thread_queue, tour_str)
}

func (worker *PartnersToursWorker) Thread(thread_index int) {
	log.Info.Printf("Start partners tours worker %d\n", thread_index)

	thread_queue := fmt.Sprintf(ThreadPartnersToursQueueTemplate, thread_index)
	tour := tours.TourPartners{}
	for !ForceStopThreads {
		tours, err := cache.GetQueueBatch(thread_queue, PartnersTourBatchSize)
		if err != nil || len(tours) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for _, tour_str := range tours {
			err = tour.FromString(tour_str)
			if err != nil {
				log.Error.Print("Load tour from loader queue error:", err)
				continue
			}

			worker.TourProcess(&tour)
		}
	}

	log.Info.Printf("Finish partners tours worker %d\n", thread_index)
}

func (worker *PartnersToursWorker) TourProcess(tour *tours.TourPartners) {
	InToursCounter++

	if IsSkipTour(tour) {
		return
	}

	crc := tour.KeyDataCRC32()

	id_tour_str, err := cache.Get(crc, fmt.Sprintf(PartnersTourIDKeyTemplate, tour.KeyData()))
	if err != nil && err != redis.Nil {
		log.Error.Print(
			"Error read partners tour from key ",
			fmt.Sprintf(PartnersTourIDKeyTemplate, tour.KeyData()),
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

		mutex := worker.TourUpdateLock(id_tour)
		defer mutex.Unlock()

		cache.Set(crc,
			fmt.Sprintf(PartnersTourIDKeyTemplate, tour.KeyData()),
			strconv.FormatUint(id_tour, 10))
		cache.Set(id_tour,
			fmt.Sprintf(PartnersTourKeyDataKeyTemplate, id_tour),
			tour.KeyData())
		cache.Set(id_tour,
			fmt.Sprintf(PartnersTourPriceDataKeyTemplate, id_tour),
			tour.PriceData())
		worker.ToInsertQueue(id_tour)
	} else {
		id_tour, err := strconv.ParseUint(id_tour_str, 10, 64)
		if err != nil {
			log.Error.Print(
				"Error parse map tour id from key ",
				fmt.Sprintf(PartnersTourIDKeyTemplate, tour.KeyData()),
				":",
				err,
			)
		}

		mutex := worker.TourUpdateLock(id_tour)
		defer mutex.Unlock()

		// Compare old price with new price
		old_price_data, err_price := cache.Get(id_tour, fmt.Sprintf(PartnersTourPriceDataKeyTemplate, id_tour))
		if err_price != nil && err_price != redis.Nil {
			log.Error.Print("Error read PriceData for tour ", id_tour, ":", err)
		}

		is_bigger, err := tour.PriceBiggerThen(old_price_data)
		if err == nil || err_price == redis.Nil {
			if !is_bigger || err_price == redis.Nil {
				// Save to price data
				cache.Set(id_tour, fmt.Sprintf(PartnersTourPriceDataKeyTemplate, id_tour), tour.PriceData())
				worker.ToUpdateQueue(id_tour)
			}
		} else {
			log.Error.Print("Error compare prices:", err)
		}
	}
}

func (worker *PartnersToursWorker) IsPrimary() bool {
	return worker.Settings.WorkerFirstThreadId == 0
}

func (worker *PartnersToursWorker) ToInsertQueue(id uint64) error {
	return cache.AddQueue(PartnersTourInsertQueue, strconv.FormatUint(id, 10))
}

func (worker *PartnersToursWorker) ToUpdateQueue(id uint64) error {
	return cache.AddQueue(PartnersTourUpdateQueue, strconv.FormatUint(id, 10))
}


func (worker *PartnersToursWorker) TourUpdateLock(id uint64) *cache.RedisMutex {
	var locked bool
	var mutex *cache.RedisMutex

	start := true
	counter := 5
	locked = false

	for start || (!locked && counter > 0) {
		if !start && !locked {
			log.Error.Println("Repeat for redis mutex (partners)...")
		}
		mutex = tours.PartnersTourUpdateLocker(id)
		locked = mutex.Lock()

		start = false
		counter--
	}

	if !locked {
		log.Error.Fatalln("Can not lock redis mutex.")
	}

	return mutex
}
