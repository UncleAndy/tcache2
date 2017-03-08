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
	"github.com/uncleandy/tcache2/apps/data2db/db_worker_base"
)

const (
	MapToursInsertBatchSize = 1000
	MapToursUpdateBatchSize = 1000
	MapToursDeleteBatchSize = 1000
)

func (worker *MapToursDbWorker) MainLoop() {
	log.Info.Println("Run map main loop...")
	worker.InitThreads()
	log.Info.Println("Finish map main loop.")
}

func (worker *MapToursDbWorker) InitThreads() {
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		worker.Thread(worker.Settings.WorkerFirstThreadId + i)
	}
}

func (worker *MapToursDbWorker) Thread(thread_index int) {
	log.Info.Println("Run map thread ", thread_index)
	go func() {
		for {
			worker.InsertProcess(thread_index)
			worker.UpdateProcess(thread_index)
			worker.DeleteProcess(thread_index)
		}
	}()
}

func (worker *MapToursDbWorker) InsertProcess(thread_index int) {
	db_worker_base.DbWorkerBaseInterface(worker).InsertProcessBy(
		thread_index,
		MapToursInsertBatchSize,
		MapTourInsertThreadQueueTemplate,
		MapTourInsertThreadDataCounter,
		worker.DbConnectionByThread(thread_index),
	)
}

func (worker *MapToursDbWorker) UpdateProcess(thread_index int) {
	db_worker_base.DbWorkerBaseInterface(worker).UpdateProcessBy(
		thread_index,
		MapToursUpdateBatchSize,
		MapTourUpdateThreadQueueTemplate,
		MapTourUpdateThreadDataCounter,
		worker.DbConnectionByThread(thread_index),
	)
}

func (worker *MapToursDbWorker) DeleteProcess(thread_index int) {
	db_worker_base.DbWorkerBaseInterface(worker).DeleteProcessBy(
		thread_index,
		MapToursDeleteBatchSize,
		MapTourDeleteThreadQueueTemplate,
		MapTourDeleteThreadDataCounter,
		worker.DbConnectionByThread(thread_index),
	)
}

func (i MapTourRedisReader) ReadTour(id_str string) (tours.TourInterface, error) {
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

	return tours.TourInterface(&tour), nil
}

func (i MapTourDbSQLAction) InsertToursFlush(tours *[]tours.TourInterface, size int, db_conn *db.DbConnection) {
	// Insert tours to DB
	log.Info.Println("InsertToursFlush: Map start...")
	first_tour := (*tours)[0]
	insert_fields_sql := first_tour.InsertSQLFieldsSet()
	sep := ""
	data_sql := ""
	for i := 0; i < size; i++ {
		tour := (*tours)[i]
		data_sql = data_sql + sep + "("+tour.InsertSQLDataSet()+")"
		sep = ","
	}
	sql := "INSERT INTO cached_sletat_tours ("+insert_fields_sql+") VALUES "+data_sql+";"

	log.Info.Println("InsertToursFlush: Check connect...")
	db_conn.CheckConnect()
	log.Info.Println("InsertToursFlush: Run query...")
	_, err := db_conn.SendQuery(sql)
	log.Info.Println("InsertToursFlush: Query done.")
	if err != nil {
		log.Error.Print("WARNING! Error when insert new map tours to DB: ", err)
	}
	log.Info.Println("InsertToursFlush: Map finish...")
}

func (i MapTourDbSQLAction) UpdateToursFlush(tours *[]tours.TourInterface, size int, db_conn *db.DbConnection) {
	err := db_conn.StartTransaction()
	if err != nil {
		log.Error.Print("WARNING! Error update map tours start transaction: ", err)
	}

	for i := 0; i < size; i++ {
		tour := (*tours)[i]
		id_str := strconv.FormatUint(tour.GetId(), 10)
		sql := "UPDATE cached_sletat_tours SET "+tour.UpdateSQLString()+" WHERE id = "+id_str
		err := db_conn.SendQueryParamsTrx(sql)
		if err != nil {
			log.Error.Print("WARNING! Error when update map tour ", id_str, " to DB: ", err)
		}
	}

	err = db_conn.CommitTransaction()
	if err != nil {
		log.Error.Print("WARNING! Error update map tours commit transaction: ", err)
	}
}

func (i MapTourDbSQLAction) DeleteToursFlush(tours *[]string, size int, db_conn *db.DbConnection) {
	actual := (*tours)[0:size]
	ids := strings.Join(actual, ",")
	sql := "DELETE FROM cached_sletat_tours WHERE id IN (" + ids + ")"
	db_conn.CheckConnect()
	_, err := db_conn.SendQuery(sql)
	if err != nil {
		log.Error.Print("WARNING! Error delete map tours from DB: ", err)
	}
}
