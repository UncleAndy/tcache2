package cache

import "gopkg.in/redis.v4"

type RedisServer struct {
	Addr 		string	`yaml:"addr"`
	Db 		int	`yaml:"db"`
	Password 	string	`yaml:"password"`
	Priority 	float32	`yaml:"priority"`
	Connection 	*redis.Client
	QueueSizes	map[string]int64
}
