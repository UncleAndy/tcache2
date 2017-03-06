package map_tours

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
	ThreadMapToursQueueTemplate = "map_tours_download_list_%d"
	MapTourIDKeyTemplate = "mtk:%s"
	MapTourKeyDataKeyTemplate = "mtkk:%d"
	MapTourPriceDataKeyTemplate = "mtp:%d"
	MapTourPriceLogKeyTemplate = "mtl:%d"
	MapTourInsertQueue = "map_tours_insert"
	MapTourUpdateQueue = "map_tours_update"
	MapTourDeleteQueue = "map_tours_delete"
	MapTourBatchSize = 100
)

var (
	ForceStopThreads = false
)

func (worker *MapToursWorker) Stop() {
	ForceStopThreads = true
}

func (worker *MapToursWorker) MainLoop() {
	// Create threads & fill threads array of channels
	worker.InitThreads()
}

func (worker *MapToursWorker) InitThreads() {
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

func (worker *MapToursWorker) SendTour(tour_str string) {
	tour := tours.TourMap{}

	err := tour.FromString(tour_str)
	if err != nil {
		log.Error.Print("Load tour from loader queue error:", err)
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
	log.Info.Printf("Start map tours worker %d\n", thread_index)
	thread_queue := fmt.Sprintf(ThreadMapToursQueueTemplate, thread_index)
	tour := tours.TourMap{}
	for !ForceStopThreads {
		tours, err := cache.GetQueueBatch(thread_queue, MapTourBatchSize)
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
	log.Info.Printf("Finish map tours worker %d\n", thread_index)
}

func (worker *MapToursWorker) TourProcess(tour *tours.TourMap) {
	if IsSkipTour(tour) {
		return
	}

	crc := tour.KeyDataCRC32()

	//log.Info.Println("Get id from cache...")
	id_tour_str, err := cache.Get(crc, fmt.Sprintf(MapTourIDKeyTemplate, tour.KeyData()))
	//log.Info.Println("Get id from cache done.")
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
		//log.Info.Println("Gen id...")
		id_tour, err := tour.GenId()
		//log.Info.Println("Gen id done.")
		if err != nil {
			log.Error.Fatal("Error GenID for tour:", err)
		}

		//log.Info.Println("LockTourUpdate...", id_tour)
		mutex := worker.TourUpdateLock(id_tour)
		defer mutex.Unlock()
		//log.Info.Println("LockTourUpdate done.", id_tour)

		//log.Info.Println("Save tour data...")
		cache.Set(crc,
			fmt.Sprintf(MapTourIDKeyTemplate, tour.KeyData()), strconv.FormatUint(id_tour, 10))
		cache.Set(id_tour,
			fmt.Sprintf(MapTourKeyDataKeyTemplate, id_tour), tour.KeyData())
		cache.Set(id_tour,
			fmt.Sprintf(MapTourPriceDataKeyTemplate, id_tour), tour.PriceData())
		worker.ToInsertQueue(id_tour)
		//log.Info.Println("Save tour data done.")
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

		//log.Info.Println("LockTourUpdate...", id_tour)
		mutex := worker.TourUpdateLock(id_tour)
		defer mutex.Unlock()
		//log.Info.Println("LockTourUpdate done.", id_tour)

		//log.Info.Println("Get old price data...")
		old_price_data, err_price := cache.Get(id_tour, fmt.Sprintf(MapTourPriceDataKeyTemplate, id_tour))
		//log.Info.Println("Get old price data done.")
		if err_price != nil && err_price != redis.Nil {
			log.Error.Print("Error read PriceData for tour ", id_tour, ":", err)
		}

		//log.Info.Println("Compare prices...")
		is_bigger, err := tour.PriceBiggerThen(old_price_data)
		//log.Info.Println("Compare prices done.")
		if err == nil || err_price == redis.Nil {
			if is_bigger && err_price != redis.Nil {
				//log.Info.Println("Add data to price log...")
				cache.RPush(id_tour, fmt.Sprintf(MapTourPriceLogKeyTemplate, id_tour), tour.PriceData())
				//log.Info.Println("Add data to price log done.")
			} else {
				// Save to price data
				//log.Info.Println("Set new price data...")
				cache.Set(id_tour, fmt.Sprintf(MapTourPriceDataKeyTemplate, id_tour), tour.PriceData())
				//log.Info.Println("Set new price data done.")
				//log.Info.Println("Add tour to update queue...")
				worker.ToUpdateQueue(id_tour)
				//log.Info.Println("Add tour to update queue done.")
			}
		} else {
			log.Error.Print("Error compare prices:", err)
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

func (worker *MapToursWorker) TourUpdateLock(id uint64) *cache.RedisMutex {
	var locked bool
	var mutex *cache.RedisMutex

	start := true
	counter := 5
	locked = false

	for start || (!locked && counter > 0) {
		if !start && !locked {
			log.Error.Println("Repeat for redis mutex (map)...")
		}
		mutex = tours.MapTourUpdateLocker(id)
		locked = mutex.Lock()

		start = false
		counter--
	}

	if !locked {
		log.Error.Fatalln("Can not lock redis mutex.")
	}

	return mutex
}
