package master

import (
	"fmt"
	"log"
	"net"
	"recommendation-service/model"
	"recommendation-service/syncutils"
	"sort"
	"sync"
	"time"
)

type Master struct {
	// statics
	serviceAddress string
	masterAddress  string
	movieTitles    []string
	modelConfig    model.ModelConfig

	// dynamics
	syncLstn       net.Listener
	serviceLstn    net.Listener
	slaveAddresses []string
	slavesInfo     SafeCounts
}

/*
slaveIsActive          []bool
slaveIsActiveMutex     sync.RWMutex
slaveRequestCount      []int
slaveRequestCountMutex sync.RWMutex
*/

type MasterConfig struct {
	MasterAddress  string            `json:"masterAddress"`
	ServiceAddress string            `json:"serviceAddress"`
	SlaveAddress   []string          `json:"slaveAddress"`
	MovieTitles    []string          `json:"movieTitles"`
	ModelConfig    model.ModelConfig `json:"modelConfig"`
}

type SafeCounts struct {
	Counts   []int
	CountsMu sync.RWMutex
	Status   []bool
	StatusMu sync.RWMutex
}

func (sd *SafeCounts) ReadCounts() []int {
	sd.CountsMu.RLock()
	defer sd.CountsMu.RUnlock()
	copiedCounts := make([]int, len(sd.Counts))
	copy(copiedCounts, sd.Counts)
	return copiedCounts
}

func (sd *SafeCounts) CompareCounts(counts []int) bool {
	sd.CountsMu.RLock()
	defer sd.CountsMu.RUnlock()
	for i, count := range counts {
		if count != sd.Counts[i] {
			return false
		}
	}
	return true
}

func (sd *SafeCounts) ReadCountByIndex(index int) int {
	sd.CountsMu.RLock()
	defer sd.CountsMu.RUnlock()
	return sd.Counts[index]
}
func (sd *SafeCounts) WriteCountByIndex(value int, index int) {
	sd.CountsMu.Lock()
	defer sd.CountsMu.Unlock()
	sd.Counts[index] = value
}
func (sd *SafeCounts) ReadStatus() []bool {
	sd.StatusMu.RLock()
	defer sd.StatusMu.RUnlock()
	copiedStatus := make([]bool, len(sd.Status))
	copy(copiedStatus, sd.Status)
	return copiedStatus
}

func (sd *SafeCounts) ReadStatustByIndex(index int) bool {
	sd.StatusMu.RLock()
	defer sd.StatusMu.RUnlock()
	return sd.Status[index]
}
func (sd *SafeCounts) WriteStatusByIndex(value bool, index int) {
	sd.StatusMu.Lock()
	defer sd.StatusMu.Unlock()
	sd.Status[index] = value
}
func (sd *SafeCounts) GetMinCountIdByStatus(status bool) int {
	sd.CountsMu.RLock()
	defer sd.CountsMu.RUnlock()
	min := sd.Counts[0]
	minIndex := -1
	for i, count := range sd.Counts {
		if (minIndex == -1 || count < min) && sd.Status[i] == status {
			min = count
			minIndex = i
		}
	}
	return minIndex
}

func (sd *SafeCounts) GetActiveCountNum() int {
	sd.StatusMu.RLock()
	defer sd.StatusMu.RUnlock()
	total := 0
	for _, status := range sd.Status {
		if status {
			total++
		}
	}
	return total
}

func (sd *SafeCounts) GetActiveCountNumByStatus(status bool) int {
	sd.StatusMu.RLock()
	defer sd.StatusMu.RUnlock()
	total := 0
	for _, iStatus := range sd.Status {
		if iStatus == status {
			total++
		}
	}
	return total
}

func (sd *SafeCounts) GetActiveIdsByStatus(status bool) []int {
	sd.StatusMu.RLock()
	defer sd.StatusMu.RUnlock()
	ids := make([]int, 0)
	for i, iStatus := range sd.Status {
		if iStatus == status {
			ids = append(ids, i)
		}
	}
	return ids
}

func (master *Master) handleSyncronization() {
	log.Println("Handling syncronization")

	var wg sync.WaitGroup
	for i, address := range master.slaveAddresses {
		if !master.slavesInfo.ReadStatustByIndex(i) {
			wg.Add(1)
			go func(slaveId int, address string) {
				defer wg.Done()
				err := master.handleSlaveSync(i, address)
				if err != nil {
					log.Println(err)
				}
			}(i, address)
		}
	}
	wg.Wait()

	log.Println("Slaves synchronized", master.slavesInfo.ReadStatus())

	for i, address := range master.slaveAddresses {
		if !master.slavesInfo.ReadStatustByIndex(i) {
			go func(slaveId int, address string) {
				for {
					err := master.handleSlaveSync(slaveId, address)
					if err != nil {
						log.Println(err)
						time.Sleep(time.Second * 60)
						continue
					}
					break
				}
			}(i, address)
		}
	}
}

func (master *Master) handleSlaveSync(slaveId int, address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		master.slavesInfo.WriteStatusByIndex(false, slaveId)
		return fmt.Errorf("syncError: Slave %d connection error: %v", slaveId, err)
	}
	defer conn.Close()
	err = master.sendSyncRequest(&conn)
	if err != nil {
		master.slavesInfo.WriteStatusByIndex(false, slaveId)
		return fmt.Errorf("syncError: Slave %d sync request error: %v", slaveId, err)
	}
	var response syncutils.SlaveSyncResponse
	master.receiveSyncResponse(&conn, &response)
	if response.Status == 0 {
		master.slavesInfo.WriteStatusByIndex(true, slaveId)
	}
	log.Println("Slave", slaveId, "synchronized")
	return nil
}

func (master *Master) sendSyncRequest(conn *net.Conn) error {
	request := syncutils.MasterSyncRequest{
		MasterAddress: master.masterAddress,
		ModelConfig:   master.modelConfig,
	}
	err := syncutils.SendObjectAsJsonMessage(&request, conn)
	if err != nil {
		return fmt.Errorf("syncRequestErr: Error sending request object as json: %v", err)
	}
	return nil
}

func (master *Master) receiveSyncResponse(conn *net.Conn, response *syncutils.SlaveSyncResponse) error {
	err := syncutils.ReceiveJsonMessageAsObject(response, conn)
	if err != nil {
		return fmt.Errorf("syncResponseError: Error reading data: %v", err)
	}
	return nil
}

func (master *Master) loadConfig(filename string) error {
	var config MasterConfig
	err := syncutils.LoadJsonFile(filename, &config)
	if err != nil {
		return fmt.Errorf("loadConfig: Error loading config file: %v", err)
	}

	master.serviceAddress = config.ServiceAddress
	master.masterAddress = config.MasterAddress
	master.slaveAddresses = config.SlaveAddress
	master.movieTitles = config.MovieTitles
	master.modelConfig = config.ModelConfig

	return nil
}

func (master *Master) Init() error {
	err := master.loadConfig("config/master.json")
	if err != nil {
		return fmt.Errorf("initError: Error loading config: %v", err)
	}

	numSlaves := len(master.slaveAddresses)
	master.slavesInfo.Counts = make([]int, numSlaves)
	master.slavesInfo.Status = make([]bool, numSlaves)

	return nil
}

func (master *Master) Run() error {
	log.Println("Master running")
	var err error
	master.syncLstn, err = net.Listen("tcp", master.masterAddress)
	if err != nil {
		return fmt.Errorf("initError: Error setting local listener: %v", err)
	}
	defer master.syncLstn.Close()

	log.Println("Master running")
	master.handleSyncronization()
	master.handleService()

	return nil
}

func (master *Master) handleService() {
	var err error
	for {
		master.serviceLstn, err = net.Listen("tcp", master.serviceAddress)
		if err != nil {
			log.Println("serviceError: Error setting service listener:", err)
			time.Sleep(time.Second * 5)
			continue
		}
		break
	}
	defer master.serviceLstn.Close()
	for {
		conn, err := master.serviceLstn.Accept()
		if err != nil {
			fmt.Println("serviceError: Error accepting connection:", err)
		}
		go master.handleRecommendation(&conn)
	}
}

func (master *Master) handleRecommendation(conn *net.Conn) {
	defer (*conn).Close()
	log.Printf("INFO: handleRec: Connection accepted from %v\n", (*conn).RemoteAddr())
	var request syncutils.ClientRecRequest
	err := receiveRecommendationRequest(conn, &request)
	if err != nil {
		log.Printf("ERROR: handleRecErr: %v", err)
	}
	log.Printf("INFO: handleRec: Request received\n")
	var predictions []syncutils.Prediction
	master.handleModelRecommendation(&predictions, &request)
	var response syncutils.MasterRecResponse

	response.UserId = request.UserId
	response.Recommendations = make([]syncutils.Recommendation, len(predictions))
	for i, prediction := range predictions {
		response.Recommendations[i].MovieId = prediction.MovieId
		response.Recommendations[i].MovieTitle = master.movieTitles[prediction.MovieId]
		response.Recommendations[i].Rating = prediction.Rating
	}
	err = sendRecommendationResponse(conn, &response)
	if err != nil {
		log.Printf("ERROR: handleRecErr: %v", err)
	}
	log.Printf("INFO: handleRec: Response sent\n")
}

func receiveRecommendationRequest(conn *net.Conn, request *syncutils.ClientRecRequest) error {
	err := syncutils.ReceiveJsonMessageAsObject(request, conn)
	if err != nil {
		return fmt.Errorf("receiveRecRequestErr: Error reading data: %v", err)
	}
	return nil
}

func (master *Master) handleModelRecommendation(predictions *[]syncutils.Prediction, request *syncutils.ClientRecRequest) error {
	beginStatus := master.slavesInfo.ReadStatus()
	log.Println("Start handling recommendation request. Begin status:", beginStatus)
	defer log.Println("End handling recommendation request.")
	nBatches := 0
	for _, active := range beginStatus {
		if active {
			nBatches++
		}
	}
	if nBatches == 0 {
		return fmt.Errorf("RecRequestErr: No active slaves")
	}
	log.Println("Number of batches:", nBatches)
	batches := master.createBatches(nBatches, request.UserId, request.Quantity)
	log.Println("Batches created:", batches)
	ch := make(chan *syncutils.SlaveRecResponse, nBatches)
	activeSlaveIds := master.slavesInfo.GetActiveIdsByStatus(true)
	go func() {
		var wg sync.WaitGroup
		for batchId, batch := range batches {
			wg.Add(1)
			go func(batchId int) {
				defer wg.Done()
				var slaveId int
				if batchId < len(activeSlaveIds) {
					slaveId = activeSlaveIds[batchId]
				} else {
					slaveId = master.slavesInfo.GetMinCountIdByStatus(true)
				}
				err := master.handleRecommendationRequestBatch(batchId, &batch, slaveId, ch)
				if err != nil {
					log.Println(err)
				}
			}(batchId)
		}
		wg.Wait()
		close(ch)
	}()
	var tempPredictions []syncutils.Prediction
	for partialResponse := range ch {
		tempPredictions = append(tempPredictions, partialResponse.Predictions...)
		if len(tempPredictions) > request.Quantity {
			sort.Slice(tempPredictions, func(i, j int) bool {
				return tempPredictions[i].Rating > tempPredictions[j].Rating
			})
			tempPredictions = tempPredictions[:request.Quantity]
		}
	}
	*predictions = tempPredictions
	return nil
}

func (master *Master) handleRecommendationRequestBatch(batchId int, batch *syncutils.MasterRecRequest, slaveId int, ch chan *syncutils.SlaveRecResponse) error {
	var err error
	var conn net.Conn
	var response syncutils.SlaveRecResponse
	log.Printf("INFO: RequestBatch: Handling batch (%d).\n", batchId)
	defer log.Printf("INFO: RequestBatch: Batch (%d) handled.\n", batchId)
	for {
		for {
			fmt.Println("SlaveId", slaveId)
			conn, err = net.Dial("tcp", master.slaveAddresses[slaveId])
			if err != nil {
				master.slavesInfo.WriteStatusByIndex(false, slaveId)
				log.Printf("RequestBatchErr: Error connecting to slave node %d for batch %d: %v", slaveId, batchId, err)
				log.Printf("Status: %v", master.slavesInfo.ReadStatus())
				slaveId = master.slavesInfo.GetMinCountIdByStatus(true)
				continue
			}
			err = syncutils.SendObjectAsJsonMessage(batch, &conn)
			if err != nil {
				master.slavesInfo.WriteStatusByIndex(false, slaveId)
				log.Printf("RequestBatchErr: Error sending batch to slave node %d for batch %d: %v", slaveId, batchId, err)
				slaveId = master.slavesInfo.GetMinCountIdByStatus(true)
				continue
			}
			break
		}
		err = syncutils.ReceiveJsonMessageAsObject(&response, &conn)
		if err != nil {
			master.slavesInfo.WriteStatusByIndex(false, slaveId)
			log.Printf("RequestBatchErr: Error receiving response from slave node (%d): %v", slaveId, err)
		}
		ch <- &response
		log.Printf("INFO: RequestBatch: Response received from slave node (%d).", slaveId)
		break
	}
	return nil
}
func (master *Master) createBatches(nBatches, userId int, quantity int) []syncutils.MasterRecRequest {
	batches := make([]syncutils.MasterRecRequest, nBatches)
	var rangeSize int = len(master.movieTitles) / nBatches
	var startMovieId int = 0
	for i := 0; i < nBatches; i++ {
		var endMovieId int = startMovieId + rangeSize
		if i == nBatches-1 {
			endMovieId = len(master.movieTitles)
		}
		batches[i] = syncutils.MasterRecRequest{
			UserId:       userId,
			StartMovieId: startMovieId,
			EndMovieId:   endMovieId,
			Quantity:     quantity,
		}
		startMovieId = endMovieId
	}
	return batches
}

func sendRecommendationResponse(conn *net.Conn, response *syncutils.MasterRecResponse) error {
	err := syncutils.SendObjectAsJsonMessage(response, conn)
	if err != nil {
		return fmt.Errorf("sendRecResponseErr: Error sending response object as json: %v", err)
	}
	return nil
}
