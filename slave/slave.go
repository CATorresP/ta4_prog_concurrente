package slave

import (
	"fmt"
	"log"
	"math"
	"net"
	"recommendation-service/model"
	"recommendation-service/syncutils"
	"sort"
)

type Slave struct {
	ip            string
	masterIp      string
	model         model.Model
	movieGenreIds [][]int
}

func (slave *Slave) Init() error {
	slave.ip = syncutils.GetOwnIp()
	return nil
}

func (slave *Slave) Run() {
	for {
		slave.handleSynchronization()
		slave.handleRecommendations()
	}
}

// Proceso de sincronización
func (slave *Slave) handleSynchronization() {
	syncLstn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", slave.ip, syncutils.SyncronizationPort))
	log.Println("Slave listening for syncronization on", fmt.Sprintf("%s:%d", slave.ip, syncutils.SyncronizationPort))
	if err != nil {
		log.Println("ERROR: syncErr: Error setting local listener:", err)
		return
	}
	defer syncLstn.Close()

	success := false
	for !success {
		conn, err := syncLstn.Accept()
		if err != nil {
			log.Printf("ERROR: syncErr: Connection error: %v", err)
			continue
		}
		err = slave.handleSyncRequest(&conn)
		if err != nil {
			log.Printf("ERROR: syncErr: Error handling sync request: %v", err)
			continue
		}
		log.Println("INFO: Synchronization successful")
		break
	}
}

// Procesamiento de solicitud de sincronización
func (slave *Slave) handleSyncRequest(conn *net.Conn) error {
	defer (*conn).Close()
	log.Println("INFO: Start handling sync request")

	var syncRequest syncutils.MasterSyncRequest
	slave.receiveSyncRequest(conn, &syncRequest)

	slave.masterIp = syncRequest.MasterIp
	slave.movieGenreIds = syncRequest.MovieGenreIds
	slave.model = model.LoadModel(&syncRequest.ModelConfig)

	log.Println("IP: ", slave.ip)
	log.Println("Master IP: ", slave.masterIp)
	log.Println("Model Syncronized")
	if len(syncRequest.MovieGenreIds) > 0 {
		log.Println("Movie genres loaded: ", len(syncRequest.MovieGenreIds))
	} else {
		log.Println("No movie genres loaded")
	}
	if len(slave.model.R) > 0 {
		log.Println("R: ", len(slave.model.R), ", ", len(slave.model.R[0]))
	} else {
		log.Println("R: ", len(slave.model.R), ", ", 0)
	}
	if len(slave.model.P) > 0 {
		log.Println("P: ", len(slave.model.P), ", ", len(slave.model.P[0]))
	} else {
		log.Println("P: ", len(slave.model.P), ", ", 0)
	}
	if len(slave.model.Q) > 0 {
		log.Println("Q: ", len(slave.model.Q), ", ", len(slave.model.Q[0]))
	} else {
		log.Println("Q: ", len(slave.model.Q), ", ", 0)
	}
	log.Println("test: ", slave.model.Predict(1, 1))
	return slave.repondSyncRequest(conn)
}

// Recibir solicitud de sincronización
func (slave *Slave) receiveSyncRequest(conn *net.Conn, syncRequest *syncutils.MasterSyncRequest) error {
	err := syncutils.ReceiveJsonMessageAsObject(&syncRequest, conn)
	if err != nil {
		return fmt.Errorf("syncRequestErr. Error receiving request: %v", err)
	}
	return nil
}

// Responder solicitud de sincronización
func (slave *Slave) repondSyncRequest(conn *net.Conn) error {
	err := syncutils.SendObjectAsJsonMessage(syncutils.SlaveSyncResponse{Status: 0}, conn)
	if err != nil {
		return fmt.Errorf("syncResponseErr. Error sending response: %v", err)
	}
	return nil
}

func (slave *Slave) handleRecommendations() {
	log.Println("INFO: Start handling recs")
	recLstn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", slave.ip, syncutils.RecommendationPort))
	log.Println("Slave listening for recommendation requests on", fmt.Sprintf("%s:%d", slave.ip, syncutils.RecommendationPort))
	if err != nil {
		log.Printf("ERROR: recErr: Error setting local listener: %v", err)
		return
	}
	defer recLstn.Close()
	for {
		conn, err := recLstn.Accept()
		if err != nil {
			log.Printf("ERROR: recError: Incoming connection error: %v", err)
			continue
		}
		go slave.handleRecommendation(&conn)
	}
}

func (slave *Slave) handleRecommendation(conn *net.Conn) {
	defer (*conn).Close()
	log.Println("INFO: Start handling recommendation")

	var request syncutils.MasterRecRequest
	err := receiveRecRequest(&request, conn)
	if err != nil {
		log.Printf("ERROR: recHandleErr: Error handling recommendation: %v", err)
		return
	}
	log.Println("INFO: Recommendation request received")
	log.Println("TEST: Sending response: ", request)

	var response syncutils.SlaveRecResponse
	err = slave.processRecommendation(&response, &request)
	if err != nil {
		log.Printf("ERROR: recHandleErr: Error handling recommendation: %v", err)
		return
	}
	log.Println("INFO: Recommendations obtained")

	log.Println("TEST: Sending response: ", response)

	err = respondRecRequest(&response, conn)
	if err != nil {
		log.Printf("ERROR: recHandleErr: Error handling recommendation: %v", err)
		return
	}
	log.Println("INFO: Recommendation handled successfully")
}

func receiveRecRequest(recRequest *syncutils.MasterRecRequest, conn *net.Conn) error {
	err := syncutils.ReceiveJsonMessageAsObject(recRequest, conn)
	if err != nil {
		return fmt.Errorf("recReceiveRequestErr: Error receiving request: %v", err)
	}
	return nil
}

func (slave *Slave) processRecommendation(response *syncutils.SlaveRecResponse, request *syncutils.MasterRecRequest) error {

	sum := 0.0
	max := math.Inf(-1)
	min := math.Inf(1)
	count := 0

	pred := make([]syncutils.Prediction, request.EndMovieId-request.StartMovieId)
	for movieId := request.StartMovieId; movieId < request.EndMovieId; movieId++ {
		if slave.model.R[request.UserId][movieId] == 0 {
			if len(request.GenreIds) > 0 {
				if !containsAll(slave.movieGenreIds[movieId], request.GenreIds) {
					continue
				}
			}
			rating := slave.model.Predict(request.UserId, movieId)
			pred[count] = syncutils.Prediction{
				MovieId: movieId,
				Rating:  rating,
			}
			if max < rating {
				max = rating
			}
			if min > rating {
				min = rating
			}
			sum += rating
			count++
		}
	}
	pred = pred[:count]
	if count > request.Quantity {
		sort.Slice(pred, func(i, j int) bool {
			return pred[i].Rating > pred[j].Rating
		})
		response.Predictions = pred[:request.Quantity]
	}
	response.Sum = sum
	response.Max = max
	response.Min = min
	response.Count = count
	return nil
}

func containsAll(movieGenres, requestGenres []int) bool {
	genreMap := make(map[int]bool)
	for _, genre := range movieGenres {
		genreMap[genre] = true
	}
	for _, genre := range requestGenres {
		if !genreMap[genre] {
			return false
		}
	}
	return true
}

func respondRecRequest(response *syncutils.SlaveRecResponse, conn *net.Conn) error {
	err := syncutils.SendObjectAsJsonMessage(response, conn)
	if err != nil {
		return fmt.Errorf("recRespondRequestErr: Error sending response: %v", err)
	}
	return nil
}
