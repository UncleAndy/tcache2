package sletat

import (
	"encoding/xml"
	"github.com/uncleandy/tcache2/log"
	"net/http"
	"bytes"
)

const (
	URL          = "http://bulk.sletat.ru/Main.svc"
	bulkCacheUrl = "http://bulk.sletat.ru/BulkCacheDownload?packetId="
	EnvLoaderFileConfig = "SLETAT_LOADER_CONFIG"
)

type SletatPacketList struct {
	XMLName         xml.Name `xml:"urn:SletatRu:Contracts:Bulk:Soap11Gate:v1 GetPacketList"`
	CreateDatePoint string   `xml:"createDatePoint"`
}

type SletatPacket struct {
	CountryId    int    `xml:"CountryId"`
	CreateDate   string `xml:"CreateDate"`
	DateTimeFrom string `xml:"DateTimeFrom"`
	DateTimeTo   string `xml:"DateTimeTo"`
	DptCityId    int    `xml:"DptCityId"`
	Id           string `xml:"Id"`
	SourceId     int    `xml:"SourceId"`
}

type SletatRequest struct {
	XMLName xml.Name `xml:"http://schemas.xmlsoap.org/soap/envelope/ Envelope"`
	Header  SletatRequestHeader
	Body    SletatRequestBody
}

type SletatRequestHeader struct {
	XMLName  xml.Name `xml:"Header"`
	AuthInfo SletatAuthInfo
}

type SletatAuthInfo struct {
	XMLName  xml.Name `xml:"urn:SletatRu:DataTypes:AuthData:v1 AuthInfo"`
	Login    string   `xml:"Login"`
	Password string   `xml:"Password"`
}

type SletatRequestBody struct {
	XMLName    xml.Name `xml:"Body"`
	SOAPAction interface{}
}

var (
	request = SletatRequest{
		Header: SletatRequestHeader{
			AuthInfo: SletatAuthInfo{
				Login:    sletatSettings.Login,
				Password: sletatSettings.Password,
			},
		},
	}
	client   = http.Client{}
)

func LoadPackets(t string) (chan SletatPacket) {
	packets := make(chan SletatPacket)

	go func() {
		log.Info.Println("Download packets from", t)
		packetsList, err := GetPacketsList(t)
		if err != nil {
			log.Error.Println(err)
		}

		log.Info.Println("fetchPackets list...")
		for _, packet := range packetsList {
			if IsSkipPacket(&packet) {
				log.Info.Println("fetchPackets packet skip...")
				continue
			}

			log.Info.Println("fetchPackets packet to work")
			packets <- packet
		}

		close(packets)
		log.Info.Println("fetchPackets done")
	}()

	return packets
}

func GetPacketsList(date string) ([]SletatPacket, error) {
	var buf bytes.Buffer

	log.Info.Println("FetchPacketsList...")

	request.Body.SOAPAction = SletatPacketList{
		CreateDatePoint: date,
	}

	enc := xml.NewEncoder(&buf)
	if err := enc.Encode(request); err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, URL, &buf)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "text/xml;charset=UTF-8")
	req.Header.Add("SOAPAction", "urn:SletatRu:Contracts:Bulk:Soap11Gate:v1/Soap11Gate/GetPacketList")

	log.Info.Println("FetchPacketsList request for packets data...")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	log.Info.Println("FetchPacketsList request for packets data done")

	log.Info.Println("FetchPacketsList packets data XML decode...")
	envelope := struct {
		XMLName xml.Name `xml:"Envelope"`
		Body    struct {
			XMLName               xml.Name `xml:"Body"`
			GetPacketListResponse struct {
			      XMLName             xml.Name `xml:"GetPacketListResponse"`
				GetPacketListResult struct {
					XMLName    xml.Name `xml:"GetPacketListResult"`
					PacketInfo []SletatPacket
				}
			}
		}
	}{}
	log.Info.Println("FetchPacketsList packet data:\n", resp.Status)
	if err = xml.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	log.Info.Println("FetchPacketsList packets data XML decode done")

	return envelope.Body.GetPacketListResponse.GetPacketListResult.PacketInfo, nil
}
