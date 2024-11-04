package syncutils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"recommendation-service/model"
	"strings"
)

const (
	ServicePort        = 9000
	SyncronizationPort = 9001
	RecommendationPort = 9002
)

type MasterSyncRequest struct {
	MasterIp      string            `json:"masterIp"`
	MovieGenreIds [][]string        `json:"movieGenreIds"`
	ModelConfig   model.ModelConfig `json:"modelConfig"`
}

type SlaveSyncResponse struct {
	Status int `json:"status"`
}

// Recommendation Communication
type ClientRecRequest struct {
	UserId   int   `json:"userId"`
	Quantity int   `json:"quantity"`
	GenreIds []int `json:"genreIds"`
}

type MasterRecRequest struct {
	UserId       int   `json:"userId"`
	StartMovieId int   `json:"startMovieId"`
	EndMovieId   int   `json:"endMovieId"`
	Quantity     int   `json:"quantity"`
	GenreIds     []int `json:"genreIds"`
}

type MasterRecResponse struct {
	UserId          int              `json:"userId"`
	Recommendations []Recommendation `json:"recommendations"`
}

type Recommendation struct {
	Id      int      `json:"id"`
	Title   string   `json:"title"`
	Genres  []string `json:"genres"`
	Rating  float64  `json:"rating"`
	Comment string   `json:"comment"`
}

type SlaveRecResponse struct {
	Predictions []Prediction `json:"predictions"`
	Sum         float64      `json:"sum"`
	Min         float64      `json:"min"`
	Max         float64      `json:"max"`
	Count       int          `json:"count"`
}

type Prediction struct {
	MovieId int     `json:"movieId"`
	Rating  float64 `json:"rating"`
}

func ReceiveJsonMessageAsObject(object any, conn *net.Conn) error {
	reader := bufio.NewReader(*conn)
	bytes, err := reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("jsonMssgReceive: Error reading bytes: %v", err)
	}
	err = json.Unmarshal(bytes, object)
	if err != nil {
		return fmt.Errorf("jsonMssgReceive: Error unmarshalling JSON: %v", err)
	}
	return nil
}

func SendObjectAsJsonMessage(object any, conn *net.Conn) error {
	bytes, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("jsonMssgSend: Error marshalling JSON: %v", err)
	}
	writer := bufio.NewWriter(*conn)
	_, err = writer.Write(bytes)
	if err != nil {
		return fmt.Errorf("jsonMssgSend: Error writing bytes: %v", err)
	}
	err = writer.WriteByte('\n')
	if err != nil {
		return fmt.Errorf("jsonMssgSend: Error writing newline: %v", err)
	}
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("jsonMssgSend: Error flushing writer: %v", err)
	}
	return nil
}
func LoadJsonFile(filename string, object interface{}) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("jsonLoad: Error opening json file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(object)
	if err != nil {
		return fmt.Errorf("jsonLoad: Error decoding json file: %v", err)
	}
	return nil
}

func GetOwnIp() string {
	interfaces, _ := net.Interfaces()
	for _, iInterface := range interfaces {
		if strings.HasPrefix(iInterface.Name, "eth0") {
			addresses, _ := iInterface.Addrs()
			for _, addr := range addresses {
				switch expression := addr.(type) {
				case *net.IPNet:
					ipv4 := expression.IP.To4()
					if ipv4 != nil {
						return ipv4.String()
					}
				}
			}
		}
	}
	return "127.0.0.1"
}
