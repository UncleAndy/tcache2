package sletat

import (
	"time"
	"github.com/uncleandy/tcache2/log"
)

var (
	ForceStopFlag = false
)

func MainLoop() {
	finish_channel := make(chan bool)

	t, err := makeDownloadTime()
	if err != nil {
		log.Error.Println(err)
		return
	}

	LoadTours(LoadPackets(t), finish_channel)

	finish_wait(finish_channel)
}

func makeDownloadTime() (string, error) {
	location, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		return "", err
	}

	t := time.Now().In(location).Add(-2 * time.Hour)
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())

	return t.Format(time.RFC3339), nil
}

func finish_wait(finish_channel chan bool) {
	<-finish_channel
	close(finish_channel)

	log.Info.Println("END")
}
