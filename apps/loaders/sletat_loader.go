package main

import (
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"github.com/uncleandy/tcache2/cache"
	"os"
	"syscall"
	"os/signal"
)

func SignalsInit() (chan os.Signal) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	return sigChan
}

func SignalsProcess(signals chan os.Signal) {
	<- signals

	println("Detect stop command. Please, wait...")

	sletat.ForceStopFlag = true
}

func main() {
	signals := SignalsInit()
	go SignalsProcess(signals)

	cache.InitFromEnv()
	cache.RedisInit()
	sletat.Init()
	sletat.MainLoop()
}