package cache

type RedisMode struct {
	ReconfigureMode 	bool		`yaml:"reconfigure_mode"`
	MainServers		[]RedisServer	`yaml:"main_servers,flow"`

	// Used only if ReconfigureMode is true
	OldServers 		[]RedisServer	`yaml:"old_servers,flow"`
}
