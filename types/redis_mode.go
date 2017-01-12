package types

type RedisMode struct {
	ReconfigureMode 	bool		`yaml:"reconfigure_mode"`
	MainServers		[]RedisServer	`yaml:"main_servers,flow"`

	// Used only if ReconfigureMode is true
	NewServers 		[]RedisServer	`yaml:"new_servers,flow"`
}
