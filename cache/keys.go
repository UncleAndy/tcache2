package cache

import (
)
import "github.com/uncleandy/tcache2/log"

// Возвращает порцию ключей подходящую под шаблон
// Вызов должен быть итерационным - current_cursor, возвращенный на текущем шаге нужно передавать на следующий
// Инициирующий курсор должен быть "0"
// Если возвращается курсор "0", значит текущая порция данных последняя

type KeysScanner struct {
	Template string
	Count int64
	ServerIndex int
	Cursor uint64
	Finished bool
}

func (k *KeysScanner) Init(template string, count int64) {
	k.Template = template
	k.Count = count - 1
	k.ServerIndex = 0
	k.Finished = false
}

func (k *KeysScanner) Next() []string {
	var err error
	var result []string

	for len(result) <= 0 {
		server := RedisSettings.MainServers[k.ServerIndex]
		result, k.Cursor, err =  server.Connection.Scan(k.Cursor, k.Template, k.Count).Result()
		if err != nil {
			log.Error.Print("Error Redis Scan keys: ", err)
		}

		if k.Cursor == 0 {
			k.ServerIndex++
			if k.ServerIndex >= len(RedisSettings.MainServers) {
				k.Finished = true
				return result
			}
		}
	}

	return result
}
