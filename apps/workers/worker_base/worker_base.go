package worker_base

import "github.com/uncleandy/tcache2/tours"

type WorkerBaseInterface interface {
	Init()
	MainLoop()
	WaitFinish()
	SendTour(string)
	IsPrimary() bool
}
