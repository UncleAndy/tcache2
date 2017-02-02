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
func SaveMapToursDataToRedis(tours []tours.TourMap) {
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

func CleanMapToursDataInRedis(tours []tours.TourMap) {
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

	SaveMapToursDataToRedis([]tours.TourMap{tour1, tour2, tour3})

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
			t.Error("Wrong count of tours in DB. Expected 1, got:", count)
		}
	}
	rows.Close()

	//====================================================
	// Other module test
	// Clean redis & run LoadToursData from main worker
	CleanMapToursDataInRedis([]tours.TourMap{tour1})
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







	db.SendQuery("DELETE FROM cached_sletat_tours;")
}

func TestDbWorkerUpdate(t *testing.T) {
}