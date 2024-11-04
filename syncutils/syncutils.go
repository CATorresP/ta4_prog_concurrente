package syncutils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"recommendation-service/model"
)

type MasterSyncRequest struct {
	MasterAddress string            `json:"address"`
	ModelConfig   model.ModelConfig `json:"modelConfig"`
}

type SlaveSyncResponse struct {
	Status int `json:"status"`
}

// Recommendation Communication
type ClientRecRequest struct {
	UserId   int `json:"userId"`
	Quantity int `json:"quantity"`
}

type MasterRecRequest struct {
	UserId       int `json:"userId"`
	StartMovieId int `json:"startMovieId"`
	EndMovieId   int `json:"endMovieId"`
	Quantity     int `json:"quantity"`
}

type MasterRecResponse struct {
	UserId          int              `json:"userId"`
	Recommendations []Recommendation `json:"recommendations"`
}

type Recommendation struct {
	MovieId    int     `json:"movieId"`
	MovieTitle string  `json:"movieTitle"`
	Rating     float64 `json:"rating"`
}

type SlaveRecResponse struct {
	Predictions []Prediction `json:"predictions"`
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
