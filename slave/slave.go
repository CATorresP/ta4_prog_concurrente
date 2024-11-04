package slave

import (
	"fmt"
	"log"
	"net"
	"recommendation-service/model"
	"recommendation-service/syncutils"
	"sort"
)

type Slave struct {
	ip       string
	masterIp string
	model    model.Model
}

/*
	type SlaveConfig struct {
		SlaveAddress string `json:"slaveAddress"`
	}

	func (slave *Slave) loadConfig(filename string) error {
		var slaveConfig SlaveConfig
		err := syncutils.LoadJsonFile(filename, &slaveConfig)
		if err != nil {
			return fmt.Errorf("loadConfig: Error loading config file: %v", err)
		}

		return nil
	}
*/
func (slave *Slave) Init() error {
	slave.ip = syncutils.GetOwnIp()
	/*
		err := slave.loadConfig("config/slave.json")
		if err != nil {
			return fmt.Errorf("initError: Error loading config: %v", err)
		}
	*/

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
	slave.model = model.LoadModel(&syncRequest.ModelConfig)
	log.Println("IP: ", slave.ip)
	log.Println("Master IP: ", slave.masterIp)
	log.Println("Model loaded")
	log.Println("R: ", len(slave.model.R), ", ", len(slave.model.R[0]))
	log.Println("P: ", len(slave.model.P), ", ", len(slave.model.P[0]))
	log.Println("Q: ", len(slave.model.Q), ", ", len(slave.model.Q[0]))
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

	var recRequest syncutils.MasterRecRequest
	err := receiveRecRequest(&recRequest, conn)
	if err != nil {
		log.Printf("ERROR: recHandleErr: Error handling recommendation: %v", err)
		return
	}
	log.Println("INFO: Recommendation request received")

	var response syncutils.SlaveRecResponse
	err = slave.getModelRecommendation(&response, recRequest.UserId, recRequest.StartMovieId, recRequest.EndMovieId, recRequest.Quantity)
	if err != nil {
		log.Printf("ERROR: recHandleErr: Error handling recommendation: %v", err)
		return
	}
	log.Println("INFO: Recommendations obtained")

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

func (slave *Slave) getModelRecommendation(response *syncutils.SlaveRecResponse, userId, startMovieId, endMovieId, quantity int) error {
	pred := make([]syncutils.Prediction, endMovieId-startMovieId)
	size := 0
	for movieId := startMovieId; movieId < endMovieId; movieId++ {
		if slave.model.R[userId][movieId] == 0 {
			pred[size] = syncutils.Prediction{
				MovieId: movieId,
				Rating:  slave.model.Predict(userId, movieId),
			}
			size++
		}
	}
	pred = pred[:size]
	if size > quantity {
		sort.Slice(pred, func(i, j int) bool {
			return pred[i].Rating > pred[j].Rating
		})
		response.Predictions = pred[:quantity]
	}
	return nil
}

func respondRecRequest(response *syncutils.SlaveRecResponse, conn *net.Conn) error {
	err := syncutils.SendObjectAsJsonMessage(response, conn)
	if err != nil {
		return fmt.Errorf("recRespondRequestErr: Error sending response: %v", err)
	}
	return nil
}
