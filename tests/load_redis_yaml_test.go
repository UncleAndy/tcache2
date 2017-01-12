package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/cache"
)

func TestLoadRedisYaml(t *testing.T) {
	cache.ReadSettings("redis_reconfigure.yaml")

	if len(cache.RedisSettings.MainServers) != 2 {
		t.Error("MainServers expected 2, got ", len(cache.RedisSettings.MainServers))
	}
	if len(cache.RedisSettings.OldServers) != 3 {
		t.Error("MainServers expected 3, got ", len(cache.RedisSettings.OldServers))
	}
	server := cache.RedisSettings.MainServers[0]
	if server.Addr != "127.0.0.1:6379" ||
		server.Db != 0 ||
		server.Password != "" ||
		server.Priority != 1.5 {
		t.Errorf("Wrong data for MainServer[0]: %+v", server)
	}
	server = cache.RedisSettings.MainServers[1]
	if server.Addr != "127.0.0.2:1234" ||
		server.Db != 1 ||
		server.Password != "abc" ||
		server.Priority != 2.7 {
		t.Errorf("Wrong data for MainServer[1]: %+v", server)
	}
	server = cache.RedisSettings.OldServers[0]
	if server.Addr != "127.0.0.1:1234" ||
		server.Db != 0 ||
		server.Password != "" ||
		server.Priority != 1.0 {
		t.Errorf("Wrong data for OldServer[0]: %+v", server)
	}
	server = cache.RedisSettings.OldServers[1]
	if server.Addr != "127.0.0.2:1234" ||
		server.Db != 0 ||
		server.Password != "" ||
		server.Priority != 1.0 {
		t.Errorf("Wrong data for OldServer[1]: %+v", server)
	}
	server = cache.RedisSettings.OldServers[2]
	if server.Addr != "127.0.0.3:1234" ||
		server.Db != 0 ||
		server.Password != "" ||
		server.Priority != 1.0 {
		t.Errorf("Wrong data for OldServer[2]: %+v", server)
	}
}
