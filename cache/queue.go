package cache

import (
	"gopkg.in/redis.v4"
	"fmt"
)

const (
	MaxOperationsBeforeQueueUpdates = 100
)

var (
	updateQueueSizesCounter int
)

// Load queue sizes for servers
func QueueSizesUpdate(queue string, servers *[]RedisServer) {
	updateQueueSizesCounter = MaxOperationsBeforeQueueUpdates
	for i, server := range *servers {
		len, err := server.Connection.LLen(queue).Result()
		if err != nil {
			len = 0
		}

		(*servers)[i].QueueSizesMutex.Lock()
		(*servers)[i].QueueSizes[queue] = len
		(*servers)[i].QueueSizesMutex.Unlock()
	}
}

func QueueSizesUpdateAll(queue string) {
	QueueSizesUpdate(queue, &RedisSettings.MainServers)
	if RedisSettings.ReconfigureMode {
		QueueSizesUpdate(queue, &RedisSettings.OldServers)
	}
}

func CheckUpdateQueueSizes(queue string) {
	updateQueueSizesCounter--
	if updateQueueSizesCounter <= 0 {
		QueueSizesUpdateAll(queue)
	}
}

func MinQueueServerSearchBy(queue string, servers *[]RedisServer) RedisServer {
	var min int64
	var res RedisServer
	min = -1
	for _, server := range *servers {
		server.QueueSizesMutex.Lock()
		if min == -1 || server.QueueSizes[queue] <= min {
			res = server
			min = server.QueueSizes[queue]
		}
		server.QueueSizesMutex.Unlock()
	}
	return res
}

func MinQueueMainServerSearch(queue string) RedisServer {
	CheckUpdateQueueSizes(queue)
	return MinQueueServerSearchBy(queue, &RedisSettings.MainServers)
}

func MinQueueOldServerSearch(queue string) RedisServer {
	CheckUpdateQueueSizes(queue)
	return MinQueueServerSearchBy(queue, &RedisSettings.OldServers)
}

func MaxQueueServerSearchBy(queue string, servers *[]RedisServer) (RedisServer, error) {
	var max int64
	var res RedisServer
	max = 0
	for _, server := range *servers {
		server.QueueSizesMutex.Lock()
		if server.QueueSizes[queue] > max {
			res = server
			max = server.QueueSizes[queue]
		}
		server.QueueSizesMutex.Unlock()
	}
	var err error
	if max == 0 {
		err = redis.Nil
	}
	return res, err
}

func MaxQueueMainServerSearch(queue string) (RedisServer, error) {
	CheckUpdateQueueSizes(queue)
	return MaxQueueServerSearchBy(queue, &RedisSettings.MainServers)
}

func MaxQueueOldServerSearch(queue string) (RedisServer, error) {
	CheckUpdateQueueSizes(queue)
	return MaxQueueServerSearchBy(queue, &RedisSettings.OldServers)
}

func AddQueue(queue string, val string) error {
	minQueueServer := MinQueueMainServerSearch(queue)
	inc_queue_size(queue, &minQueueServer)
	return minQueueServer.Connection.RPush(queue, val).Err()
}

func GetQueue(queue string) (string, error) {
	QueueSizesUpdate(queue, &RedisSettings.MainServers)
	maxQueueServer, err := MaxQueueMainServerSearch(queue)
	var val string
	if err == nil {
		val, err = maxQueueServer.Connection.LPop(queue).Result()
	}

	if err != nil {
		QueueSizesUpdate(queue, &RedisSettings.OldServers)
		maxQueueOldServer, s_err := MaxQueueOldServerSearch(queue)
		if s_err == nil {
			val, err = maxQueueOldServer.Connection.LPop(queue).Result()

			if err == nil {
				dec_queue_size(queue, &maxQueueOldServer)
			}
		}
	} else {
		dec_queue_size(queue, &maxQueueServer)
	}

	return val, err
}

func GetQueueBatch(queue string, batch_size int64) ([]string, error) {
	mutex := NewMutex(fmt.Sprintf("lock_queue_batch:%s", queue))
	mutex.Lock()
	defer mutex.Unlock()

	QueueSizesUpdate(queue, &RedisSettings.MainServers)
	maxQueueServer, err := MaxQueueMainServerSearch(queue)
	var val []string
	if err == nil {
		val, err = maxQueueServer.Connection.LRange(queue, 0, batch_size - 1).Result()
	}

	if err != nil {
		QueueSizesUpdate(queue, &RedisSettings.OldServers)
		maxQueueOldServer, s_err := MaxQueueOldServerSearch(queue)
		if s_err == nil {
			val, err = maxQueueOldServer.Connection.LRange(queue, 0, batch_size - 1).Result()

			if err == nil {
				maxQueueOldServer.Connection.LTrim(queue, int64(len(val)), -1)
				dec_queue_size_by(queue, &maxQueueOldServer, int64(len(val)))
			}
		}
	} else {
		maxQueueServer.Connection.LTrim(queue, int64(len(val)), -1)
		dec_queue_size_by(queue, &maxQueueServer, int64(len(val)))
	}

	return val, err
}

func IsEmptyQueue(queue string) bool {
	QueueSizesUpdate(queue, &RedisSettings.MainServers)
	maxQueueServer, err := MaxQueueMainServerSearch(queue)
	lt_zero := true
	if err == nil {
		maxQueueServer.QueueSizesMutex.Lock()
		lt_zero = (maxQueueServer.QueueSizes[queue] <= 0)
		maxQueueServer.QueueSizesMutex.Unlock()
	}
	return err != nil || lt_zero
}

func QueueSize(queue string) int64 {
	QueueSizesUpdate(queue, &RedisSettings.MainServers)
	size := int64(0)
	for _, server := range RedisSettings.MainServers {
		server.QueueSizesMutex.Lock()
		size = size + server.QueueSizes[queue]
		server.QueueSizesMutex.Unlock()
	}
	return size
}

func CleanQueueBy(queue string, servers *[]RedisServer) {
	for _, server := range *servers {
		server.Connection.Del(queue)
		server.QueueSizesMutex.Lock()
		server.QueueSizes[queue] = 0
		server.QueueSizesMutex.Unlock()
	}
}

func CleanQueue(queue string) {
	CleanQueueBy(queue, &RedisSettings.MainServers)
	if RedisSettings.ReconfigureMode {
		CleanQueueBy(queue, &RedisSettings.OldServers)
	}
}

func inc_queue_size(queue string, server *RedisServer) {
	(*server).QueueSizesMutex.Lock()
	(*server).QueueSizes[queue] += 1
	(*server).QueueSizesMutex.Unlock()
}

func dec_queue_size(queue string, server *RedisServer) {
	dec_queue_size_by(queue, server, 1)
}

func dec_queue_size_by(queue string, server *RedisServer, size int64) {
	(*server).QueueSizesMutex.Lock()
	if (*server).QueueSizes[queue] <= 0 {
		(*server).QueueSizes[queue] = 0
	} else {
		(*server).QueueSizes[queue]-= size
	}
	(*server).QueueSizesMutex.Unlock()
}
