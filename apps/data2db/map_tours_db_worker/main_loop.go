package map_tours_db_worker

import (
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"gopkg.in/redis.v4"
	"strconv"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/db"
)

const (
	MapToursInsertBatchSize = 1000
)

func (worker *MapToursDbWorker) MainLoop() {
	worker.InitThreads()
}

func (worker *MapToursDbWorker) InitThreads() {
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		worker.Thread(worker.Settings.WorkerFirstThreadId + i)
	}
}

func (worker *MapToursDbWorker) Thread(thread_index int) {
	go func() {
		for {
			worker.InsertProcess(thread_index)
			worker.UpdateProcess(thread_index)
			worker.DeleteProcess(thread_index)
		}
	}()
}

func (worker *MapToursDbWorker) InsertProcess(thread_index int) {
	insert_queue := fmt.Sprintf(MapTourInsertThreadQueueTemplate, thread_index)
	insert_tours := make([]tours.TourMap, MapToursInsertBatchSize)
	insert_tours_index := 0
	for {
		id_str, err := cache.GetQueue(insert_queue)

		// Check finish loop
		if err == redis.Nil {
			_, err := cache.Get(0, MapTourInsertThreadDataCounter)
			if err != redis.Nil {
				// Flush data if present
				if insert_tours_index > 0 {
					worker.InsertToursFlush(&insert_tours, insert_tours_index)
				}

				cache.Incr(0, MapTourInsertThreadDataCounter)
				break
			}
		}

		tour, err := worker.ReadTour(id_str)
		if err != nil {
			continue
		}

		insert_tours[insert_tours_index] = tour
		insert_tours_index++
		if insert_tours_index >= MapToursInsertBatchSize {
			worker.InsertToursFlush(&insert_tours, insert_tours_index)
			insert_tours_index = 0
		}
	}
}

func (worker *MapToursDbWorker) UpdateProcess(thread_index int) {
}

func (worker *MapToursDbWorker) DeleteProcess(thread_index int) {
}

func (worker *MapToursDbWorker) ReadTour(id_str string) (tours.TourMap, error) {
	id, err := strconv.ParseUint(id_str, 10, 64)
	if err != nil {
		log.Error.Print("Error parse uint64 for id:", id_str)
		return nil, err
	}

	key_data_key := fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, id)
	key_data, err := cache.Get(id, key_data_key)
	if err != nil {
		log.Error.Print("WARNING! Can not read KEY DATA for id:", id)
		return nil, err
	}

	price_data_key := fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, id)
	price_data, err := cache.Get(id, price_data_key)
	if err != nil {
		log.Error.Print("WARNING! Can not read PRICE DATA for id:", id)
		return nil, err
	}

	tour := tours.TourMap{}
	err = tour.FromKeyData(key_data)
	if err != nil {
		log.Error.Print("Can not parse KEY DATA for id:", id, " - ", err)
		return nil, err
	}
	err = tour.FromPriceData(price_data)
	if err != nil {
		log.Error.Print("Can not parse PRICE DATA for id:", id, " - ", err)
		return nil, err
	}

	return tour, nil
}

func (worker *MapToursDbWorker) InsertToursFlush(tours *[]tours.TourMap, size int) {
	// Insert tours to DB
	first_tour := (*tours)[0]
	insert_fields_sql := first_tour.InsertSQLFieldsSet()
	sep := ""
	data_sql := ""
	for _, tour := range *tours {
		data_sql = data_sql + sep + "("+tour.InsertSQLDataSet()+")"
		sep = ","
	}
	sql := "INSERT INTO cached_sletat_tours "+insert_fields_sql+" VALUES "+data_sql+";"

	db.CheckConnect()
	_, err := db.SendQuery(sql)
	if err != nil {
		log.Error.Print("WARNING! Error when insert new map tours to DB: ", err)
	}
}
