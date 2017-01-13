package cache

import (
	"github.com/uncleandy/tcache2/types"
	"errors"
)

const (
	MaxOperationsBeforeQueueUpdates = 100
)

var (
	updateQueueSizesCounter int
)

// Load queue sizes for servers
func QueueSizesUpdate(queue string, servers *[]types.RedisServer) {
	updateQueueSizesCounter = MaxOperationsBeforeQueueUpdates
	for i, server := range *servers {
		len, err := server.Connection.LLen(queue).Result()
		if err != nil {
			len = 0
		}
		(*servers)[i].QueueSizes[queue] = len
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

func MinQueueServerSearchBy(queue string, servers *[]types.RedisServer) types.RedisServer {
	var min int64
	var res types.RedisServer
	min = -1
	for _, server := range *servers {
		if min == -1 || server.QueueSizes[queue] <= min {
			res = server
			min = server.QueueSizes[queue]
		}
	}
	return res
}

func MinQueueMainServerSearch(queue string) types.RedisServer {
	CheckUpdateQueueSizes(queue)
	return MinQueueServerSearchBy(queue, &RedisSettings.MainServers)
}

func MinQueueOldServerSearch(queue string) types.RedisServer {
	CheckUpdateQueueSizes(queue)
	return MinQueueServerSearchBy(queue, &RedisSettings.OldServers)
}

func MaxQueueServerSearchBy(queue string, servers *[]types.RedisServer) (types.RedisServer, error) {
	var max int64
	var res types.RedisServer
	max = 0
	for _, server := range *servers {
		if server.QueueSizes[queue] > max {
			res = server
			max = server.QueueSizes[queue]
		}
	}
	var err error
	if max == 0 {
		err = errors.New("No data in queue")
	}
	return res, err
}

func MaxQueueMainServerSearch(queue string) (types.RedisServer, error) {
	CheckUpdateQueueSizes(queue)
	return MaxQueueServerSearchBy(queue, &RedisSettings.MainServers)
}

func MaxQueueOldServerSearch(queue string) (types.RedisServer, error) {
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
				maxQueueOldServer.QueueSizes[queue]--
			}
		}
	} else {
		dec_queue_size(queue, &maxQueueServer)
	}

	return val, err
}

func CleanQueueBy(queue string, servers *[]types.RedisServer) {
	for _, server := range *servers {
		server.Connection.Del(queue)
		server.QueueSizes[queue] = 0
	}
}

func CleanQueue(queue string) {
	CleanQueueBy(queue, &RedisSettings.MainServers)
	if RedisSettings.ReconfigureMode {
		CleanQueueBy(queue, &RedisSettings.OldServers)
	}
}

func inc_queue_size(queue string, server *types.RedisServer) {
	(*server).QueueSizes[queue] += 1
}

func dec_queue_size(queue string, server *types.RedisServer) {
	if (*server).QueueSizes[queue] <= 0 {
		(*server).QueueSizes[queue] = 0
	} else {
		(*server).QueueSizes[queue]--
	}
}
