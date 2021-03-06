package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"fmt"
	"github.com/uncleandy/tcache2/apps/workers/map_tours"
	"strconv"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/apps/data2db/db_worker_base"
	"github.com/uncleandy/tcache2/apps/workers/worker_base"
	"github.com/uncleandy/tcache2/apps/data2db/map_tours_db_worker"
	"os"
	"github.com/uncleandy/tcache2/db"
	"time"
)

// Save fixture tours to Redis
func save_map_tours_data_to_redis(tours []tours.TourMap) {
	var key_data string
	var price_data string

	for _, tour := range tours {
		key_data = tour.KeyData()
		price_data = tour.PriceData()
		cache.Set(tour.Id,
			fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour.Id),
			key_data)
		cache.Set(tour.KeyDataCRC32(),
			fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data),
			strconv.FormatUint(tour.Id, 10))
		cache.Set(tour.Id,
			fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour.Id),
			price_data)
	}
}

func clean_map_tours_data_in_redis(tours []tours.TourMap) {
	for _, tour := range tours {
		key_data := tour.KeyData()
		cache.Del(tour.Id,
			fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour.Id))
		cache.Del(tour.KeyDataCRC32(),
			fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
		cache.Del(tour.Id,
			fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour.Id))
	}
}

func init_test_db() {
	os.Setenv(db.EnvDbFileConfig, "test_db_config.yaml")
	db.Init()
}

func insert_test_tours_to_redis_and_db(tours []tours.TourMap) {
	save_map_tours_data_to_redis(tours)

	insert_counter_key := map_tours_db_worker.MapTourInsertThreadDataCounter
	insert_template_0 := fmt.Sprintf(map_tours_db_worker.MapTourInsertThreadQueueTemplate, 0)

	for _, tour := range tours {
		cache.AddQueue(insert_template_0, strconv.FormatUint(tour.Id, 10))
	}

	worker := map_tours_db_worker.MapToursDbWorker{
		db_worker_base.DbWorkerBase{
			Settings : worker_base.WorkerSettings{
				WorkerFirstThreadId:        0,
				WorkerThreadsCount:        3,
				AllThreadsCount:        3,
			},
			FinishChanel: make(chan bool),
			RedisTourReader: map_tours_db_worker.MapTourRedisReader{},
			DbSQLAction: map_tours_db_worker.MapTourDbSQLAction{},
		},
	}

	go worker.InsertProcess(0)

	cache.Set(0, insert_counter_key, "0")
	for true {
		counter_str, err := cache.Get(0, insert_counter_key)
		if err != nil {
			println("Error read flush counter in manager:", err)
		}
		counter, err := strconv.ParseUint(counter_str, 10, 64)
		if err != nil {
			println("Error parse flush counter in manager:", err)
		}

		if counter >= uint64(1) {
			break
		}

		time.Sleep(1 * time.Second)
	}
}

func TestDbWorkerInsert(t *testing.T) {
	init_test_redis_multi()
	init_test_db()
	db.CheckConnect()

	db.SendQuery("DELETE FROM cached_sletat_tours;")

	tour1 := TourMapFixture()
	tour2 := TourMapFixture()
	tour3 := TourMapFixture()
	tour1.Id = 1
	tour1.Checkin = "2017-01-11"
	tour2.Id = 2
	tour2.Checkin = "2017-01-12"
	tour3.Id = 3
	tour3.Checkin = "2017-01-13"

	save_map_tours_data_to_redis([]tours.TourMap{tour1, tour2, tour3})

	insert_counter_key := map_tours_db_worker.MapTourInsertThreadDataCounter
	insert_template_0 := fmt.Sprintf(map_tours_db_worker.MapTourInsertThreadQueueTemplate, 0)
	insert_template_1 := fmt.Sprintf(map_tours_db_worker.MapTourInsertThreadQueueTemplate, 1)
	insert_template_2 := fmt.Sprintf(map_tours_db_worker.MapTourInsertThreadQueueTemplate, 2)

	cache.AddQueue(insert_template_0, "1")
	cache.AddQueue(insert_template_1, "2")
	cache.AddQueue(insert_template_2, "3")

	worker := map_tours_db_worker.MapToursDbWorker{
		db_worker_base.DbWorkerBase{
			Settings : worker_base.WorkerSettings{
				WorkerFirstThreadId:        0,
				WorkerThreadsCount:        3,
				AllThreadsCount:        3,
			},
			FinishChanel: make(chan bool),
			RedisTourReader: map_tours_db_worker.MapTourRedisReader{},
			DbSQLAction: map_tours_db_worker.MapTourDbSQLAction{},
		},
	}

	go worker.InsertProcess(0)
	go worker.InsertProcess(1)
	go worker.InsertProcess(2)

	cache.Set(0, insert_counter_key, "0")
	for true {
		counter_str, err := cache.Get(0, insert_counter_key)
		if err != nil {
			t.Error("Error read flush counter in manager:", err)
		}
		counter, err := strconv.ParseUint(counter_str, 10, 64)
		if err != nil {
			t.Error("Error parse flush counter in manager:", err)
		}

		if counter >= uint64(3) {
			break
		}

		time.Sleep(1 * time.Second)
	}

	rows, err := db.SendQuery("SELECT COUNT(*) FROM cached_sletat_tours;")
	if err != nil {
		t.Error("Error select count of tour from DB:", err)
	} else if rows.Err() != nil {
		t.Error("Error select count of tour from DB (rows):", rows.Err())
	} else {
		rows.Next()
		count := 0
		err = rows.Scan(&count)
		if err != nil {
			t.Error("Error select count of tour from DB (scan):", err)
		}
		if count != 3 {
			t.Error("Wrong count of tours in DB. Expected 3, got:", count)
		}
	}
	rows.Close()

	//====================================================
	// Other module test
	// Clean redis & run LoadToursData from main worker
	clean_map_tours_data_in_redis([]tours.TourMap{tour1, tour2, tour3})
	map_tours_worker := map_tours.MapToursWorker{}
	map_tours_worker.LoadToursData()

	// Check tour1
	key_data, err := cache.Get(tour1.Id,
		fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour1.Id))
	if err != nil {
		t.Error("Can not read key data after LoadToursData:", err)
		return
	}

	price_data, err := cache.Get(tour1.Id,
		fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour1.Id))
	if err != nil {
		t.Error("Can not read price data after LoadToursData:", err)
		return
	}

	tour := tours.TourMap{}

	tour.FromKeyData(key_data)
	tour.FromPriceData(price_data)
	db_id, err := cache.Get(tour.KeyDataCRC32(),
		fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
	if err != nil {
		t.Error("Can not read id data after LoadToursData:", err)
	} else if db_id == "" {
		t.Error("Can not read id data after LoadToursData: is empty string")
	} else {
		id, err := strconv.ParseUint(db_id, 10, 64)
		if err != nil {
			t.Error("Can not parse id data after LoadToursData:", err)
		} else {
			if id != tour1.Id {
				t.Error("Wrong ID value after LoadToursData. Expected:", tour1.Id, ", got:", id)
			}
		}
	}
	if tour1.KeyData() != tour.KeyData() {
		t.Error("Wrong KEY data after LoadToursData. Expected:\n", tour1.KeyData(),
		"\n, got:\n", tour.KeyData())
	}
	if tour1.PriceData() != tour.PriceData() {
		t.Error("Wrong PRICE data after LoadToursData. Expected:\n", tour1.PriceData(),
		"\n, got:\n", tour.PriceData())
	}

	// Check tour2
	key_data, err = cache.Get(tour2.Id,
		fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour2.Id))
	if err != nil {
		t.Error("Can not read key data after LoadToursData:", err)
		return
	}

	price_data, err = cache.Get(tour2.Id,
		fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour2.Id))
	if err != nil {
		t.Error("Can not read price data after LoadToursData:", err)
		return
	}

	tour = tours.TourMap{}

	tour.FromKeyData(key_data)
	tour.FromPriceData(price_data)
	db_id, err = cache.Get(tour.KeyDataCRC32(),
		fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
	if err != nil {
		t.Error("Can not read id data after LoadToursData:", err)
	} else if db_id == "" {
		t.Error("Can not read id data after LoadToursData: is empty string")
	} else {
		id, err := strconv.ParseUint(db_id, 10, 64)
		if err != nil {
			t.Error("Can not parse id data after LoadToursData:", err)
		} else {
			if id != tour2.Id {
				t.Error("Wrong ID value after LoadToursData. Expected:", tour2.Id, ", got:", id)
			}
		}
	}
	if tour2.KeyData() != tour.KeyData() {
		t.Error("Wrong KEY data after LoadToursData. Expected:\n", tour2.KeyData(),
			"\n, got:\n", tour.KeyData())
	}
	if tour2.PriceData() != tour.PriceData() {
		t.Error("Wrong PRICE data after LoadToursData. Expected:\n", tour2.PriceData(),
			"\n, got:\n", tour.PriceData())
	}

	// Check tour3
	key_data, err = cache.Get(tour3.Id,
		fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour3.Id))
	if err != nil {
		t.Error("Can not read key data after LoadToursData:", err)
		return
	}

	price_data, err = cache.Get(tour3.Id,
		fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour3.Id))
	if err != nil {
		t.Error("Can not read price data after LoadToursData:", err)
		return
	}

	tour = tours.TourMap{}

	tour.FromKeyData(key_data)
	tour.FromPriceData(price_data)
	db_id, err = cache.Get(tour.KeyDataCRC32(),
		fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
	if err != nil {
		t.Error("Can not read id data after LoadToursData:", err)
	} else if db_id == "" {
		t.Error("Can not read id data after LoadToursData: is empty string")
	} else {
		id, err := strconv.ParseUint(db_id, 10, 64)
		if err != nil {
			t.Error("Can not parse id data after LoadToursData:", err)
		} else {
			if id != tour3.Id {
				t.Error("Wrong ID value after LoadToursData. Expected:", tour3.Id, ", got:", id)
			}
		}
	}
	if tour3.KeyData() != tour.KeyData() {
		t.Error("Wrong KEY data after LoadToursData. Expected:\n", tour3.KeyData(),
			"\n, got:\n", tour.KeyData())
	}
	if tour3.PriceData() != tour.PriceData() {
		t.Error("Wrong PRICE data after LoadToursData. Expected:\n", tour3.PriceData(),
			"\n, got:\n", tour.PriceData())
	}
	//====================================================

	clean_map_tours_data_in_redis([]tours.TourMap{tour1, tour2, tour3})
	db.SendQuery("DELETE FROM cached_sletat_tours;")
}

func TestDbWorkerUpdate(t *testing.T) {
	init_test_redis_multi()
	init_test_db()
	db.CheckConnect()

	db.SendQuery("DELETE FROM cached_sletat_tours;")

	tour1 := TourMapFixture()
	tour2 := TourMapFixture()
	tour3 := TourMapFixture()
	tour1.Id = 1
	tour1.Checkin = "2017-01-11"
	tour2.Id = 2
	tour2.Checkin = "2017-01-12"
	tour3.Id = 3
	tour3.Checkin = "2017-01-13"

	insert_test_tours_to_redis_and_db([]tours.TourMap{tour1, tour2, tour3})

	tour1.Price = tour1.Price + 1000
	tour2.Price = tour2.Price + 5000
	tour3.Price = tour3.Price + 10000

	save_map_tours_data_to_redis([]tours.TourMap{tour1, tour2, tour3})

	update_counter_key := map_tours_db_worker.MapTourUpdateThreadDataCounter
	update_template_0 := fmt.Sprintf(map_tours_db_worker.MapTourUpdateThreadQueueTemplate, 0)
	update_template_1 := fmt.Sprintf(map_tours_db_worker.MapTourUpdateThreadQueueTemplate, 1)
	update_template_2 := fmt.Sprintf(map_tours_db_worker.MapTourUpdateThreadQueueTemplate, 2)

	cache.AddQueue(update_template_0, "1")
	cache.AddQueue(update_template_1, "2")
	cache.AddQueue(update_template_2, "3")

	worker := map_tours_db_worker.MapToursDbWorker{
		db_worker_base.DbWorkerBase{
			Settings : worker_base.WorkerSettings{
				WorkerFirstThreadId:        0,
				WorkerThreadsCount:        3,
				AllThreadsCount:        3,
			},
			FinishChanel: make(chan bool),
			RedisTourReader: map_tours_db_worker.MapTourRedisReader{},
			DbSQLAction: map_tours_db_worker.MapTourDbSQLAction{},
		},
	}

	go worker.UpdateProcess(0)
	go worker.UpdateProcess(1)
	go worker.UpdateProcess(2)

	cache.Set(0, update_counter_key, "0")
	for true {
		counter_str, err := cache.Get(0, update_counter_key)
		if err != nil {
			t.Error("Error read flush counter in manager:", err)
		}
		counter, err := strconv.ParseUint(counter_str, 10, 64)
		if err != nil {
			t.Error("Error parse flush counter in manager:", err)
		}

		if counter >= uint64(3) {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// Load data from DB over LoadToursData
	clean_map_tours_data_in_redis([]tours.TourMap{tour1, tour2, tour3})
	map_tours_worker := map_tours.MapToursWorker{}
	map_tours_worker.LoadToursData()

	// Check tour1
	key_data, err := cache.Get(tour1.Id,
		fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour1.Id))
	if err != nil {
		t.Error("Can not read key data after LoadToursData:", err)
		return
	}

	price_data, err := cache.Get(tour1.Id,
		fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour1.Id))
	if err != nil {
		t.Error("Can not read price data after LoadToursData:", err)
		return
	}

	tour := tours.TourMap{}

	tour.FromKeyData(key_data)
	tour.FromPriceData(price_data)
	db_id, err := cache.Get(tour.KeyDataCRC32(),
		fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
	if err != nil {
		t.Error("Can not read id data after LoadToursData:", err)
	} else if db_id == "" {
		t.Error("Can not read id data after LoadToursData: is empty string")
	} else {
		id, err := strconv.ParseUint(db_id, 10, 64)
		if err != nil {
			t.Error("Can not parse id data after LoadToursData:", err)
		} else {
			if id != tour1.Id {
				t.Error("Wrong ID value after LoadToursData. Expected:", tour1.Id, ", got:", id)
			}
		}
	}
	if tour1.KeyData() != tour.KeyData() {
		t.Error("Wrong KEY data after LoadToursData. Expected:\n", tour1.KeyData(),
			"\n, got:\n", tour.KeyData())
	}
	if tour1.PriceData() != tour.PriceData() {
		t.Error("Wrong PRICE data after LoadToursData. Expected:\n", tour1.PriceData(),
			"\n, got:\n", tour.PriceData())
	}

	// Check tour2
	key_data, err = cache.Get(tour2.Id,
		fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour2.Id))
	if err != nil {
		t.Error("Can not read key data after LoadToursData:", err)
		return
	}

	price_data, err = cache.Get(tour2.Id,
		fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour2.Id))
	if err != nil {
		t.Error("Can not read price data after LoadToursData:", err)
		return
	}

	tour = tours.TourMap{}

	tour.FromKeyData(key_data)
	tour.FromPriceData(price_data)
	db_id, err = cache.Get(tour.KeyDataCRC32(),
		fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
	if err != nil {
		t.Error("Can not read id data after LoadToursData:", err)
	} else if db_id == "" {
		t.Error("Can not read id data after LoadToursData: is empty string")
	} else {
		id, err := strconv.ParseUint(db_id, 10, 64)
		if err != nil {
			t.Error("Can not parse id data after LoadToursData:", err)
		} else {
			if id != tour2.Id {
				t.Error("Wrong ID value after LoadToursData. Expected:", tour2.Id, ", got:", id)
			}
		}
	}
	if tour2.KeyData() != tour.KeyData() {
		t.Error("Wrong KEY data after LoadToursData. Expected:\n", tour2.KeyData(),
			"\n, got:\n", tour.KeyData())
	}
	if tour2.PriceData() != tour.PriceData() {
		t.Error("Wrong PRICE data after LoadToursData. Expected:\n", tour2.PriceData(),
			"\n, got:\n", tour.PriceData())
	}

	// Check tour3
	key_data, err = cache.Get(tour3.Id,
		fmt.Sprintf(map_tours.MapTourKeyDataKeyTemplate, tour3.Id))
	if err != nil {
		t.Error("Can not read key data after LoadToursData:", err)
		return
	}

	price_data, err = cache.Get(tour3.Id,
		fmt.Sprintf(map_tours.MapTourPriceDataKeyTemplate, tour3.Id))
	if err != nil {
		t.Error("Can not read price data after LoadToursData:", err)
		return
	}

	tour = tours.TourMap{}

	tour.FromKeyData(key_data)
	tour.FromPriceData(price_data)
	db_id, err = cache.Get(tour.KeyDataCRC32(),
		fmt.Sprintf(map_tours.MapTourIDKeyTemplate, key_data))
	if err != nil {
		t.Error("Can not read id data after LoadToursData:", err)
	} else if db_id == "" {
		t.Error("Can not read id data after LoadToursData: is empty string")
	} else {
		id, err := strconv.ParseUint(db_id, 10, 64)
		if err != nil {
			t.Error("Can not parse id data after LoadToursData:", err)
		} else {
			if id != tour3.Id {
				t.Error("Wrong ID value after LoadToursData. Expected:", tour3.Id, ", got:", id)
			}
		}
	}
	if tour3.KeyData() != tour.KeyData() {
		t.Error("Wrong KEY data after LoadToursData. Expected:\n", tour3.KeyData(),
			"\n, got:\n", tour.KeyData())
	}
	if tour3.PriceData() != tour.PriceData() {
		t.Error("Wrong PRICE data after LoadToursData. Expected:\n", tour3.PriceData(),
			"\n, got:\n", tour.PriceData())
	}

	clean_map_tours_data_in_redis([]tours.TourMap{tour1, tour2, tour3})
	db.SendQuery("DELETE FROM cached_sletat_tours;")
}

func TestDbWorkerDelete(t *testing.T) {
	init_test_redis_multi()
	init_test_db()
	db.CheckConnect()

	db.SendQuery("DELETE FROM cached_sletat_tours;")

	tour1 := TourMapFixture()
	tour2 := TourMapFixture()
	tour3 := TourMapFixture()
	tour1.Id = 1
	tour1.Checkin = "2017-01-11"
	tour2.Id = 2
	tour2.Checkin = "2017-01-12"
	tour3.Id = 3
	tour3.Checkin = "2017-01-13"

	insert_test_tours_to_redis_and_db([]tours.TourMap{tour1, tour2, tour3})
	clean_map_tours_data_in_redis([]tours.TourMap{tour1, tour2, tour3})

	delete_counter_key := map_tours_db_worker.MapTourDeleteThreadDataCounter
	delete_template_0 := fmt.Sprintf(map_tours_db_worker.MapTourDeleteThreadQueueTemplate, 0)
	delete_template_1 := fmt.Sprintf(map_tours_db_worker.MapTourDeleteThreadQueueTemplate, 1)
	delete_template_2 := fmt.Sprintf(map_tours_db_worker.MapTourDeleteThreadQueueTemplate, 2)

	cache.AddQueue(delete_template_0, "1")
	cache.AddQueue(delete_template_1, "2")
	cache.AddQueue(delete_template_2, "3")

	worker := map_tours_db_worker.MapToursDbWorker{
		db_worker_base.DbWorkerBase{
			Settings : worker_base.WorkerSettings{
				WorkerFirstThreadId:        0,
				WorkerThreadsCount:        3,
				AllThreadsCount:        3,
			},
			FinishChanel: make(chan bool),
			RedisTourReader: map_tours_db_worker.MapTourRedisReader{},
			DbSQLAction: map_tours_db_worker.MapTourDbSQLAction{},
		},
	}

	go worker.DeleteProcess(0)
	go worker.DeleteProcess(1)
	go worker.DeleteProcess(2)

	cache.Set(0, delete_counter_key, "0")
	for true {
		counter_str, err := cache.Get(0, delete_counter_key)
		if err != nil {
			t.Error("Error read flush counter in manager:", err)
		}
		counter, err := strconv.ParseUint(counter_str, 10, 64)
		if err != nil {
			t.Error("Error parse flush counter in manager:", err)
		}

		if counter >= uint64(3) {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// Check tours count in DB. Must be = 0
	rows, err := db.SendQuery("SELECT COUNT(*) FROM cached_sletat_tours;")
	if err != nil {
		t.Error("Error select count of tour from DB:", err)
	} else if rows.Err() != nil {
		t.Error("Error select count of tour from DB (rows):", rows.Err())
	} else {
		rows.Next()
		count := 0
		err = rows.Scan(&count)
		if err != nil {
			t.Error("Error select count of tour from DB (scan):", err)
		}
		if count != 0 {
			t.Error("Wrong count of tours in DB. Expected 0, got:", count)
		}
	}
	rows.Close()

	clean_map_tours_data_in_redis([]tours.TourMap{tour1, tour2, tour3})
	db.SendQuery("DELETE FROM cached_sletat_tours;")
}