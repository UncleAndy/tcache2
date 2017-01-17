package worker_base

type WorkerBaseInterface interface {
	Init()
	MainLoop()
	WaitFinish()
	SendTour(string)
	IsPrimary() bool
}
