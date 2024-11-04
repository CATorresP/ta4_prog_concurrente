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
	ip              string
	movieTitles     []string
	movieGenreNames []string
	movieGenreIds   [][]int
	modelConfig     model.ModelConfig
	slaveIps        []string
	slavesInfo      SafeCounts
}

// Considerar poner las información de las peliculas en una clase
type MasterConfig struct {
	SlaveIps        []string          `json:"slaveIps"`
	MovieTitles     []string          `json:"movieTitles"`
	MovieGenreNames []string          `json:"movieGenreNames"`
	MovieGenreIds   [][]int           `json:"movieGenreIds"`
	ModelConfig     model.ModelConfig `json:"modelConfig"`
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
	log.Println("INFO: Start synchronization")
	var wg sync.WaitGroup
	for i, ip := range master.slaveIps {
		if !master.slavesInfo.ReadStatustByIndex(i) {
			wg.Add(1)
			go func(slaveId int, ip string) {
				defer wg.Done()
				err := master.handleSlaveSync(i, ip)
				if err != nil {
					log.Println(err)
				}
			}(i, ip)
		}
	}
	wg.Wait()
	for i, status := range master.slavesInfo.ReadStatus() {
		if !status {
			log.Printf("INFO: Slave %d: Not responding.\n", i)
		} else {
			log.Printf("INFO: Slave %d: Syncronized.\n", i)
		}

	}

	for i, ip := range master.slaveIps {
		if !master.slavesInfo.ReadStatustByIndex(i) {
			go func(slaveId int, ip string) {
				for {
					time.Sleep(time.Second * 60)
					err := master.handleSlaveSync(slaveId, ip)
					if err != nil {
						log.Println(err)
						continue
					}
					break
				}
			}(i, ip)
		}
	}
}

func (master *Master) handleSlaveSync(slaveId int, ip string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ip, syncutils.SyncronizationPort))
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
		MasterIp:    master.ip,
		ModelConfig: master.modelConfig,
	}
	request.ModelConfig.P = nil

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

	master.slaveIps = config.SlaveIps
	master.movieTitles = config.MovieTitles
	master.movieGenreNames = config.MovieGenreNames
	master.movieGenreIds = config.MovieGenreIds
	master.modelConfig = config.ModelConfig

	return nil
}

func (master *Master) Init() error {
	master.ip = syncutils.GetOwnIp()
	err := master.loadConfig("config/master.json")
	if err != nil {
		return fmt.Errorf("initError: Error loading config: %v", err)
	}
	numSlaves := len(master.slaveIps)
	master.slavesInfo.Counts = make([]int, numSlaves)
	master.slavesInfo.Status = make([]bool, numSlaves)

	return nil
}

func (master *Master) Run() error {
	log.Println("INFO: Master running")
	master.handleSyncronization()
	master.handleService()

	return nil
}

func (master *Master) handleService() {
	log.Println("INFO: Start service")
	defer log.Println("INFO: End service")
	serviceLstn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", master.ip, syncutils.ServicePort))
	if err != nil {
		log.Println("ERROR: serviceError: Error setting service listener:", err)
		return
	}
	defer serviceLstn.Close()

	for {
		conn, err := serviceLstn.Accept()
		if err != nil {
			log.Println("ERROR: serviceError: Error accepting connection:", err)
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
		return
	}

	log.Printf("INFO: handleRec: Request received\n")
	var predictions []syncutils.Prediction
	var sum float64
	var max float64
	var min float64
	var count int
	master.handleModelRecommendation(&predictions, &sum, &max, &min, &count, &request)

	var response syncutils.MasterRecResponse
	var mean float64
	if count > 0 {
		mean = sum / float64(count)
	}

	response.UserId = request.UserId
	response.Recommendations = make([]syncutils.Recommendation, len(predictions))
	for i, prediction := range predictions {
		response.Recommendations[i].Id = prediction.MovieId
		response.Recommendations[i].Title = master.movieTitles[prediction.MovieId]
		response.Recommendations[i].Rating = prediction.Rating
		response.Recommendations[i].Genres = make([]string, len(master.movieGenreIds[prediction.MovieId]))
		for j, genreId := range master.movieGenreIds[prediction.MovieId] {
			response.Recommendations[i].Genres[j] = master.movieGenreNames[genreId]
		}
		response.Recommendations[i].Comment = getComment(prediction.Rating, max, min, mean)
	}
	err = sendRecommendationResponse(conn, &response)
	if err != nil {
		log.Printf("ERROR: handleRecErr: %v", err)
		return
	}
	log.Printf("INFO: handleRec: Response sent\n")
}
func getComment(rating, max, min, mean float64) string {
	if rating > (max+mean)/2 {
		return "Altamente Recomendado. Muy por encima de la media"
	} else if rating > mean {
		return "Recomendado. Por encima de la media"
	} else if rating > (mean+min)/2 {
		return "Poco Recomendado. Por debajo de la media"
	} else {
		return "Muy poco recomendado. Muy por debajo de la media"
	}
}

func receiveRecommendationRequest(conn *net.Conn, request *syncutils.ClientRecRequest) error {
	err := syncutils.ReceiveJsonMessageAsObject(request, conn)
	if err != nil {
		return fmt.Errorf("receiveRecRequestErr: Error reading data: %v", err)
	}
	return nil
}

func (master *Master) handleModelRecommendation(predictions *[]syncutils.Prediction, sum, max, min *float64, count *int, request *syncutils.ClientRecRequest) error {
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
	batches := master.createBatches(nBatches, request.UserId, request.Quantity, request.GenreIds)
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
		*sum += partialResponse.Sum
		*count += partialResponse.Count
		if partialResponse.Max > *max {
			*max = partialResponse.Max
		}
		if partialResponse.Min < *min {
			*min = partialResponse.Min
		}
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
			conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", master.slaveIps[slaveId], syncutils.RecommendationPort))
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
func (master *Master) createBatches(nBatches, userId int, quantity int, genreIds []int) []syncutils.MasterRecRequest {
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
			GenreIds:     genreIds,
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
