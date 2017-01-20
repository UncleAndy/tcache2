package cache

import (
	"net"
	"github.com/hjr265/redsync.go/redsync"
	"github.com/uncleandy/tcache2/log"
	"errors"
)

func NewMutex(name string) (*redsync.Mutex, error) {
	pools := []net.Addr{}
	addr, err := net.ResolveTCPAddr("tcp", RedisSettings.MainServers[0].Addr)
	if err != nil {
		log.Error.Print("Error resolve Redis server address", RedisSettings.MainServers[0].Addr, ":", err)
	} else {
		pools = append(pools, addr)
	}

	if len(pools) <= 0 {
		log.Error.Fatal("Can not create Redis mutex!")
		return nil, errors.New("Can not create Redis mutex!")
	}

	return redsync.NewMutex(name, pools)
}
