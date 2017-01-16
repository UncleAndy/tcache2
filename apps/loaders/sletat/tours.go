package sletat

import (
	"sync"
	"github.com/uncleandy/tcache2/log"
	"encoding/xml"
	"io"
	"compress/gzip"
	"github.com/uncleandy/tcache2/tours"
	"github.com/uncleandy/tcache2/cache"
	"sort"
)

const (
	workersNum = 2
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
					PreProcessTour(packet, &tour)

					if IsSkipTour(&tour) {
						continue
					}

					TourToQueue(&tour)
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

func PreProcessTour(packet SletatPacket, tour *tours.TourBase) {
	tour.DptCityId = packet.DptCityId
	tour.SourceId = packet.SourceId
	tour.CreateDate = packet.CreateDate
	tour.CountryId = packet.CountryId

	setPrices(tour)

	processKidsValue(tour)
}

func setPrices(tour *tours.TourBase) {
	if operator, ok := operators[tour.SourceId]; ok {
		// BYN = RUB * exchange rate
		tour.PriceByr = int(float64(tour.Price) * operator.ExchangeRateRur)

		// EUR = BYN / exchange rate
		if tour.PriceEur > 0 && operator.ExchangeRateEur > 0 {
			tour.PriceEur = int(float64(tour.PriceByr) / operator.ExchangeRateEur)
		} else {
			tour.PriceEur = 0
		}

		// USD = BYN / exchange rate
		if tour.PriceByr > 0 && operator.ExchangeRateUsd > 0 {
			tour.PriceUsd = int(float64(tour.PriceByr) / operator.ExchangeRateUsd)
		} else {
			tour.PriceUsd = 0
		}
	}
}

func processKidsValue(tour *tours.TourBase) {
	var kids int

	if tour.Kid1Age != nil {
		kids++
	} else {
		kidsAge := -1
		tour.Kid1Age = &kidsAge
	}

	if tour.Kid2Age != nil {
		kids++
	} else {
		kidsAge := -1
		tour.Kid2Age = &kidsAge
	}

	if tour.Kid3Age != nil {
		kids++
	} else {
		kidsAge := -1
		tour.Kid3Age = &kidsAge
	}

	if kids != tour.Kids {
		switch tour.Kids {
		case 0:
			*tour.Kid1Age, *tour.Kid2Age, *tour.Kid3Age = -1, -1, -1
		case 1:
			*tour.Kid2Age, *tour.Kid3Age = -1, -1
		case 2:
			*tour.Kid3Age = -1
		}
	}

	kidsSlice := make([]int, 3)

	kidsSlice[0] = *tour.Kid1Age
	kidsSlice[1] = *tour.Kid2Age
	kidsSlice[2] = *tour.Kid3Age

	sort.Ints(kidsSlice)

	tour.Kid1Age = &kidsSlice[0]
	tour.Kid2Age = &kidsSlice[1]
	tour.Kid3Age = &kidsSlice[2]
}
