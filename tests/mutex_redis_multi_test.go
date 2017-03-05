package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
	"time"
)

func TestMultiMutexLock(t *testing.T) {
	init_test_redis_multi()

	redis_mutex := cache.NewMutex("test_mutex")
	if redis_mutex == nil {
		t.Error("Can not create redis mutex: is nil")
		return
	}

	redis_mutex.Delay = 1 * time.Second
	redis_mutex.Expiry = 10 * time.Second

	// Simple lock
	check_pause_duration := 500 * time.Millisecond
	start_test := make(chan bool)
	go func() {
		locked := redis_mutex.Lock()
		if !locked {
			println("Can not lock.")
		}
		defer redis_mutex.Unlock()
		start_test <- true
		time.Sleep(check_pause_duration)
	}()
	<- start_test

	redis_mutex_dup := cache.NewMutex("test_mutex")
	if redis_mutex_dup == nil {
		t.Error("Can not create redis mutex: is nil")
		return
	}

	start_time := time.Now()
	redis_mutex_dup.Lock()
	lock_duration := time.Since(start_time)
	redis_mutex_dup.Unlock()

	if lock_duration < (check_pause_duration/2) {
		t.Error("Wrong Lock time. Expexted > ", (check_pause_duration/2), ", got ", lock_duration)
	}
}