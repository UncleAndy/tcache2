package cache

import "github.com/uncleandy/tcache2/types"

// Load queue sizes for servers
func QueueSizesUpdate(queue string, servers *[]types.RedisServer) {
	for i, server := range *servers {
		len, err := server.Connection.LLen(queue).Result()
		if err != nil {
			len = 0
		}
		if (*servers)[i].QueueSizes == nil {
			(*servers)[i].QueueSizes = make(map[string]int64)
		}
		(*servers)[i].QueueSizes[queue] = len
	}
}

func QueueSizesUpdateAll(queue string) {
	QueueSizesUpdate(queue, &RedisSettings.MainServers)
	QueueSizesUpdate(queue, &RedisSettings.OldServers)
}
