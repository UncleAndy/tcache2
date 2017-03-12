package partners_tours_db_worker

import (
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"strconv"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/db"
	"strings"
	"github.com/uncleandy/tcache2/apps/workers/partners_tours"
	"github.com/uncleandy/tcache2/apps/data2db/db_worker_base"
)

const (
	PartnersToursInsertBatchSize = 1000
	PartnersToursUpdateBatchSize = 1000
	PartnersToursDeleteBatchSize = 1000
)

func (worker *PartnersToursDbWorker) MainLoop() {
	worker.InitThreads()
}

func (worker *PartnersToursDbWorker) InitThreads() {
	for i := 0; i < worker.Settings.WorkerThreadsCount; i++ {
		thread := worker.Settings.WorkerFirstThreadId + i
		go worker.Thread(thread)
	}
}

func (worker *PartnersToursDbWorker) Thread(thread_index int) {
	thread := thread_index - worker.Settings.WorkerFirstThreadId
	worker.DbPool[thread].Init(db.CurrentDbSettings)
	worker.DbPool[thread].CheckConnect()
	for {
		worker.InsertProcess(thread_index)
		worker.UpdateProcess(thread_index)
		worker.DeleteProcess(thread_index)
	}
}

// TODO: Tests process like map tours
func (worker *PartnersToursDbWorker) InsertProcess(thread_index int) {
	db_worker_base.DbWorkerBaseInterface(worker).InsertProcessBy(
		thread_index,
		PartnersToursInsertBatchSize,
		PartnersTourInsertThreadQueueTemplate,
		PartnersTourInsertThreadDataCounter,
		worker.DbConnectionByThread(thread_index),
	)
}

func (worker *PartnersToursDbWorker) UpdateProcess(thread_index int) {
	db_worker_base.DbWorkerBaseInterface(worker).UpdateProcessBy(
		thread_index,
		PartnersToursUpdateBatchSize,
		PartnersTourUpdateThreadQueueTemplate,
		PartnersTourUpdateThreadDataCounter,
		worker.DbConnectionByThread(thread_index),
	)
}

func (worker *PartnersToursDbWorker) DeleteProcess(thread_index int) {
	db_worker_base.DbWorkerBaseInterface(worker).DeleteProcessBy(
		thread_index,
		PartnersToursDeleteBatchSize,
		PartnersTourDeleteThreadQueueTemplate,
		PartnersTourDeleteThreadDataCounter,
		worker.DbConnectionByThread(thread_index),
	)
}

func (i PartnersTourRedisReader) ReadTour(id_str string) (tours.TourInterface, error) {
	id, err := strconv.ParseUint(id_str, 10, 64)
	if err != nil {
		log.Error.Print("Error parse uint64 for id:", id_str)
		return nil, err
	}

	key_data_key := fmt.Sprintf(partners_tours.PartnersTourKeyDataKeyTemplate, id)
	key_data, err := cache.Get(id, key_data_key)
	if err != nil {
		log.Error.Print("WARNING! Can not read KEY DATA for id:", id)
		return nil, err
	}

	price_data_key := fmt.Sprintf(partners_tours.PartnersTourPriceDataKeyTemplate, id)
	price_data, err := cache.Get(id, price_data_key)
	if err != nil {
		log.Error.Print("WARNING! Can not read PRICE DATA for id:", id)
		return nil, err
	}

	tour := tours.TourPartners{}
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

func (i PartnersTourDbSQLAction) InsertToursFlush(tours *[]tours.TourInterface, size int, db_conn *db.DbConnection) {
	// Insert tours to DB
	first_tour := (*tours)[0]
	insert_fields_sql := first_tour.InsertSQLFieldsSet()
	sep := ""
	data_sql := ""
	for i := 0; i < size; i++ {
		tour := (*tours)[i]
		data_sql = data_sql + sep + "("+tour.InsertSQLDataSet()+")"
		sep = ","
	}
	sql := "INSERT INTO partners_tours ("+insert_fields_sql+") VALUES "+data_sql+";"

	db_conn.CheckConnect()
	rows, err := db_conn.SendQuery(sql)
	if err != nil {
		log.Error.Print("WARNING! Error when insert new partners tours to DB: ", err)
	}
	if rows != nil {
		rows.Close()
	}
}

func (i PartnersTourDbSQLAction) UpdateToursFlush(tours *[]tours.TourInterface, size int, db_conn *db.DbConnection) {
	err := db_conn.StartTransaction()
	if err != nil {
		log.Error.Print("WARNING! Error update partners tours start transaction: ", err)
	}

	for i := 0; i < size; i++ {
		tour := (*tours)[i]
		id_str := strconv.FormatUint(tour.GetId(), 10)
		sql := "UPDATE partners_tours SET "+tour.UpdateSQLString()+" WHERE id = "+id_str
		err := db_conn.SendQueryParamsTrx(sql)
		if err != nil {
			log.Error.Print("WARNING! Error when update partners tour "+ id_str +" to DB: ", err)
		}
	}

	err = db_conn.CommitTransaction()
	if err != nil {
		log.Error.Print("WARNING! Error update partners tours commit transaction: ", err)
	}
}

func (i PartnersTourDbSQLAction) DeleteToursFlush(tours *[]string, size int, db_conn *db.DbConnection) {
	actual := (*tours)[0:size]
	ids := strings.Join(actual, ",")
	sql := "DELETE FROM partners_tours WHERE id IN (" + ids + ")"
	db_conn.CheckConnect()
	rows, err := db_conn.SendQuery(sql)
	if err != nil {
		log.Error.Print("WARNING! Error delete partners tours from DB: ", err)
	}
	if rows != nil {
		rows.Close()
	}
}
