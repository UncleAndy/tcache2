package main

import (
	"github.com/uncleandy/tcache2/apps/loaders/sletat"
	"github.com/uncleandy/tcache2/cache"
	"os"
	"syscall"
	"os/signal"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/apps_libs"
)

const (
	PidFileName = "/var/tmp/tcache2_sletat_loader.pid"
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

	log.Info.Println("Detect stop command. Please, wait...")

	sletat.ForceStopFlag = true
}

func main() {
	apps_libs.PidProcess(PidFileName)
	defer os.Remove(PidFileName)

	signals := SignalsInit()
	go SignalsProcess(signals)

	cache.InitFromEnv()
	cache.RedisInit()

	sletat.RunStatisticLoop()

	sletat.Init()
	sletat.MainLoop()

	log.Info.Println("Finished")
}