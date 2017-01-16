package sletat

import (
	"sync"
	"github.com/uncleandy/tcache2/log"
	"encoding/xml"
	"io"
	"compress/gzip"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
)

const (
	workersNum = 16
)

func LoadTours(packets chan SletatPacket, finish_channel chan bool) {
	wg := new(sync.WaitGroup)
	wg.Add(workersNum)

	// Run multiply workers to read concurrently from one channel.
	for i := 0; i < workersNum; i++ {
		go func() {
			for packet := range packets {
				log.Info.Println("fetchTours Run ...")
				tours, err := FetchTours(packet.Id)

				if err != nil {
					log.Error.Println(err)
					continue
				}

				// Process tours before send the to the database.
				log.Info.Println("fetchTours tours loop Run ...")
				for tour := range tours {
					if IsSkipTour(&tour) {
						continue
					}

					TourToQueue(tour)
				}
				log.Info.Println("fetchTours tours loop FINISH ...")
			}

			log.Info.Println("fetchTours gorotine FINISH")
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		log.Info.Println("fetchTours FINISH ...")

		finish_channel <- true
	}()

}

func FetchTours(packetId string) (chan tours.TourBase, error) {
	url := bulkCacheUrl + packetId
	log.Info.Println("Download:", url)

	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}

	tours_channel := make(chan tours.TourBase)
	go func() {
		defer resp.Body.Close()
		defer gzipReader.Close()

		decoder := xml.NewDecoder(gzipReader)
		for {
			t, err := decoder.Token()
			if err != nil && err != io.EOF {
				log.Error.Println(err)
				break
			}

			if err == io.EOF {
				break
			}

			switch se := t.(type) {
			case xml.StartElement:
				if se.Name.Local == "tour" {
					tour := tours.TourBase{}
					decoder.DecodeElement(&tour, &se)

					tours_channel <- tour
				}
			}
		}

		log.Info.Println("FetchTours FINISH")
		close(tours_channel)
	}()

	return tours_channel, nil
}

func TourToQueue(tour *tours.TourBase) {
	cache.AddQueue("tours_download_list", tour.ToString())
}
