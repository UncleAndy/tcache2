package map_tours_db_worker

import (
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"strconv"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/db"
	"strings"
)

const (
	MapToursInsertBatchSize = 1000
	MapToursUpdateBatchSize = 1000
	MapToursDeleteBatchSize = 1000
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
	worker.InsertProcessBy(
		thread_index,
		MapToursInsertBatchSize,
		MapTourInsertThreadQueueTemplate,
		MapTourInsertThreadDataCounter,
	)
}

func (worker *MapToursDbWorker) UpdateProcess(thread_index int) {
	worker.UpdateProcessBy(
		thread_index,
		MapToursUpdateBatchSize,
		MapTourUpdateThreadQueueTemplate,
		MapTourUpdateThreadDataCounter,
	)
}

func (worker *MapToursDbWorker) DeleteProcess(thread_index int) {
	worker.DeleteProcessBy(
		thread_index,
		MapToursDeleteBatchSize,
		MapTourDeleteThreadQueueTemplate,
		MapTourDeleteThreadDataCounter,
	)
}

func (worker *MapToursDbWorker) ReadTour(id_str string) (tours.TourInterface, error) {
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

	tour.Id = id

	return tour, nil
}

func (worker *MapToursDbWorker) InsertToursFlush(tours *[]tours.TourInterface, size int) {
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

func (worker *MapToursDbWorker) UpdateToursFlush(tours *[]tours.TourInterface, size int) {
	trx, err := db.StartTransaction()
	if err != nil {
		log.Error.Print("WARNING! Error update map tours start transaction: ", err)
	}

	for _, tour := range *tours {
		sql := "UPDATE cached_sletat_tours SET "+tour.UpdateSQLString()+" WHERE id = "+tour.Id
		err := db.SendQueryParamsTrx(trx, sql)
		if err != nil {
			log.Error.Print("WARNING! Error when update map tour "+ tour.Id +" to DB: ", err)
		}
	}

	err = db.CommitTransaction(trx)
	if err != nil {
		log.Error.Print("WARNING! Error update map tours commit transaction: ", err)
	}
}

func (worker *MapToursDbWorker) DeleteToursFlush(tours *[]string, size int) {
	ids := strings.Join(*tours, ",")
	sql := "DELETE FROM cached_sletat_tours WHERE id IN (" + ids + ")"
	db.CheckConnect()
	_, err := db.SendQuery(sql)
	if err != nil {
		log.Error.Print("WARNING! Error delete map tours from DB: ", err)
	}
}
