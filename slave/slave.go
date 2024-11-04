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
	lstn          net.Listener
	slaveAddress  string
	masterAddress string
	model         model.Model
}

// Creación de nodo esclavo
func NewSlave(address string) Slave {
	return Slave{slaveAddress: address}
}

type SlaveConfig struct {
	SlaveAddress string `json:"slaveAddress"`
}

func (slave *Slave) loadConfig(filename string) error {
	var slaveConfig SlaveConfig
	err := syncutils.LoadJsonFile(filename, &slaveConfig)
	if err != nil {
		return fmt.Errorf("loadConfig: Error loading config file: %v", err)
	}

	slave.slaveAddress = slaveConfig.SlaveAddress

	return nil
}

func (slave *Slave) Init() error {
	err := slave.loadConfig("config/slave.json")
	if err != nil {
		return fmt.Errorf("initError: Error loading config: %v", err)
	}

	return nil
}

func (slave *Slave) Run() {
	var err error
	slave.lstn, err = net.Listen("tcp", slave.slaveAddress)
	log.Println("Slave listening on", slave.slaveAddress)
	if err != nil {
		fmt.Println("runError: Error setting local listener:", err)
	}
	defer slave.lstn.Close()

	for {
		slave.handleSynchronization()
		slave.handleRecs()
	}
}

// Proceso de sincronización
func (slave *Slave) handleSynchronization() {
	success := false
	for !success {
		conn, err := slave.lstn.Accept()
		if err != nil {
			log.Printf("syncError: Connection error: %v", err)
			continue
		}
		err = slave.handleSyncRequest(&conn)
		if err != nil {
			log.Printf("syncError: Error handling sync request: %v", err)
			continue
		}
		log.Println("Synchronization successful")
		break
	}
}

// Procesamiento de solicitud de sincronización
func (slave *Slave) handleSyncRequest(conn *net.Conn) error {
	defer (*conn).Close()
	log.Println("Start handling sync request")

	var syncRequest syncutils.MasterSyncRequest
	slave.receiveSyncRequest(conn, &syncRequest)

	slave.masterAddress = syncRequest.MasterAddress
	slave.model = model.LoadModel(&syncRequest.ModelConfig)
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

func (slave *Slave) handleRecs() {
	for {
		conn, err := slave.lstn.Accept()
		if err != nil {
			fmt.Println("recError. Incoming connection error:", err)
			continue
		}
		go slave.handleRecommendationRequest(&conn)
	}
}

func (slave *Slave) handleRecommendationRequest(conn *net.Conn) {
	defer (*conn).Close()
	log.Println("Start handling rec request")
	var recRequest syncutils.MasterRecRequest
	receiveRecRequest(&recRequest, conn)
	log.Println("INFO: Rec request received: ", recRequest)

	var response syncutils.SlaveRecResponse
	err := slave.getModelRecommendation(&response, recRequest.UserId, recRequest.StartMovieId, recRequest.EndMovieId, recRequest.Quantity)
	if err != nil {
		log.Printf("recError: Error getting recommendations: %v", err)
	}
	err = syncutils.SendObjectAsJsonMessage(&response, conn)
	if err != nil {
		log.Printf("recError: Error sending recommendations: %v", err)
	}
}

func receiveRecRequest(recRequest *syncutils.MasterRecRequest, conn *net.Conn) error {
	err := syncutils.ReceiveJsonMessageAsObject(recRequest, conn)
	if err != nil {
		return fmt.Errorf("recRequestErr. Error receiving request: %v", err)
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
