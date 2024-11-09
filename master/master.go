package master

import (
	"fmt"
	"log"
	"math/rand"
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

	timeout := 20 * time.Second
	conn.SetDeadline(time.Now().Add(timeout))

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
	log.Println("INFO: Slave", slaveId, "synchronized")
	return nil
}

func (master *Master) sendSyncRequest(conn *net.Conn) error {
	request := syncutils.MasterSyncRequest{
		MasterIp:      master.ip,
		MovieGenreIds: master.movieGenreIds,
		ModelConfig:   master.modelConfig,
	}
	request.ModelConfig.R = nil
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

	log.Println("Master config loaded")
	log.Println("Slaves:", master.slaveIps)
	log.Println("Movies:", len(master.movieTitles))
	log.Println("Genres:", len(master.movieGenreNames))
	log.Println("MovieGenreIds:", len(master.movieGenreIds))
	log.Println("-ModelConfig-")
	log.Println("NumFeatures:", master.modelConfig.NumFeatures)
	log.Println("Epochs:", master.modelConfig.Epochs)
	log.Println("LearningRate:", master.modelConfig.LearningRate)
	log.Println("Regularization:", master.modelConfig.Regularization)
	log.Println("R:", len(master.modelConfig.R), "x", len(master.modelConfig.R[0]))
	log.Println("P:", len(master.modelConfig.P), "x", len(master.modelConfig.P[0]))
	log.Println("Q:", len(master.modelConfig.Q), "x", len(master.modelConfig.Q[0]))

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

	log.Printf("INFO: Service listening on %s:%d\n", master.ip, syncutils.ServicePort)
	for {
		conn, err := serviceLstn.Accept()
		if err != nil {
			log.Println("ERROR: serviceError: Error accepting connection:", err)
		}
		timeout := 20 * time.Second
		conn.SetDeadline(time.Now().Add(timeout))
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
		response.Recommendations[i].Genres = []string{}
		movieGenreIds := master.movieGenreIds[prediction.MovieId]
		response.Recommendations[i].Genres = make([]string, len(movieGenreIds))
		for j, genreId := range movieGenreIds {
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
	} else if rating > mean+(max-mean)/3 {
		return "Recomendado. Por encima de la media"
	} else if rating > mean {
		return "Ligeramente Recomendado. Justo por encima de la media"
	} else if rating > mean-(mean-min)/3 {
		return "Ligeramente No Recomendado. Justo por debajo de la media"
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
	log.Println("INFO: Handling recommendation request.")
	defer log.Println("INFO: RecommendationHandled.")
	beginStatus := master.slavesInfo.ReadStatus()
	log.Printf("INFO: Slaves status: %v", beginStatus)

	nBatches := 0
	for _, active := range beginStatus {
		if active {
			nBatches++
		}
	}
	if nBatches == 0 {
		return fmt.Errorf("RecRequestErr: No active slaves")
	}
	log.Printf("INFO: Created (%d) batches:", nBatches)

	cond := sync.NewCond(&sync.Mutex{})
	partialUserFactorsCh := make(chan *syncutils.SlavePartialUserFactors, nBatches)
	partialRecommendationCh := make(chan *syncutils.SlaveRecResponse, nBatches)
	masterUserFactors := syncutils.MasterUserFactors{
		UserId:      request.UserId,
		UserFactors: initializeUserFactors(master.modelConfig.NumFeatures),
	}

	batches := master.createBatches(nBatches, request.UserId, request.Quantity, request.GenreIds, masterUserFactors.UserFactors)

	activeSlaveIds := master.slavesInfo.GetActiveIdsByStatus(true)

	go func() {
		for batchId, batch := range batches {
			go func(batchId int) {
				var slaveId int
				if batchId < len(activeSlaveIds) {
					slaveId = activeSlaveIds[batchId]
				} else {
					slaveId = master.slavesInfo.GetMinCountIdByStatus(true)
				}
				master.handleRecommendationRequestBatch(cond, partialUserFactorsCh, partialRecommendationCh, batchId, slaveId, &batch, &masterUserFactors)
			}(batchId)
		}
	}()

	userFactorsGrads := make([]float64, master.modelConfig.NumFeatures)
	weightCount := 0
	// Sumatoria de los gradientes de los factores latentes de usuario obtenido de cada batch
	for i := 0; i < nBatches; i++ {
		partialUserFactors := <-partialUserFactorsCh
		for j := range partialUserFactors.WeightedGrad {
			userFactorsGrads[j] += partialUserFactors.WeightedGrad[j]
		}
		weightCount += partialUserFactors.Count
	}

	if weightCount != 0 {
		for i := range userFactorsGrads {
			userFactorsGrads[i] /= float64(weightCount)
		}
	}

	masterUserFactors.UserId = request.UserId
	masterUserFactors.UserFactors = userFactorsGrads

	log.Println("INFO: User factors updated.")
	cond.L.Lock()
	cond.Broadcast()
	cond.L.Unlock()
	for i := 0; i < nBatches; i++ {
		partialRecommendation := <-partialRecommendationCh
		if partialRecommendation.Count > 0 {
			*predictions = append(*predictions, partialRecommendation.Predictions...)
			*sum += partialRecommendation.Sum
			*count += partialRecommendation.Count
			if partialRecommendation.Max > *max {
				*max = partialRecommendation.Max
			}
			if partialRecommendation.Min < *min {
				*min = partialRecommendation.Min
			}
			if i > 0 {
				sort.Slice(*predictions, func(i, j int) bool {
					return (*predictions)[i].Rating > (*predictions)[j].Rating
				})
				if len(*predictions) > request.Quantity {
					*predictions = (*predictions)[:request.Quantity]
				}
			}
		}
	}

	return nil
}

func (master *Master) handleRecommendationRequestBatch(cond *sync.Cond, partialUserFactorsCh chan *syncutils.SlavePartialUserFactors, partialRecommendationCh chan *syncutils.SlaveRecResponse, batchId, slaveId int, batch *syncutils.MasterRecRequest, masterUserFactors *syncutils.MasterUserFactors) {
	var err error
	var conn net.Conn
	log.Printf("INFO: RequestBatch: Handling batch (%d).\n", batchId)
	defer log.Printf("INFO: RequestBatch: Batch (%d) handled.\n", batchId)
	for {
		log.Printf("INFO: Trying to connect batch to slaveId (%d)\n", slaveId)
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", master.slaveIps[slaveId], syncutils.RecommendationPort))
		if err != nil {
			master.slavesInfo.WriteStatusByIndex(false, slaveId)
			log.Printf("ERROR: RequestBatchErr: Error connecting to slave node %d for batch %d: %v", slaveId, batchId, err)
			log.Printf("Slaves status: %v", master.slavesInfo.ReadStatus())
			slaveId = master.slavesInfo.GetMinCountIdByStatus(true)
			continue
		}
		timeout := 20 * time.Second
		conn.SetDeadline(time.Now().Add(timeout))

		err = master.handlePartialRecommendation(&conn, cond, partialUserFactorsCh, partialRecommendationCh, slaveId, batchId, batch, masterUserFactors)
		if err != nil {
			log.Println("ERROR: RequestBatchErr: ", err)
			master.slavesInfo.WriteStatusByIndex(false, slaveId)
			slaveId = master.slavesInfo.GetMinCountIdByStatus(true)
			continue
		}
		break
	}
}

func (master *Master) handlePartialRecommendation(conn *net.Conn, cond *sync.Cond, partialUserFactorsCh chan *syncutils.SlavePartialUserFactors, partialRecommendationCh chan *syncutils.SlaveRecResponse, slaveId, batchId int, batch *syncutils.MasterRecRequest, masterUserFactors *syncutils.MasterUserFactors) error {
	// sendRequest
	err := syncutils.SendObjectAsJsonMessage(batch, conn)
	if err != nil {
		return fmt.Errorf("partialRecommendErr: Error sending batch to slave node (%d) for batch (%d): %v", slaveId, batchId, err)
	}
	// ReceivePartialUserFactors
	var partialUserFactors syncutils.SlavePartialUserFactors
	err = syncutils.ReceiveJsonMessageAsObject(&partialUserFactors, conn)
	if err != nil {
		return fmt.Errorf("partialRecommendErr: Error receiving partial user factors from slave node (%d): %v", slaveId, err)
	}

	partialUserFactorsCh <- &partialUserFactors

	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()

	// SendUserFactors
	err = syncutils.SendObjectAsJsonMessage(masterUserFactors, conn)
	if err != nil {
		return fmt.Errorf("partialRecommendErr: Error sending user factors to slave node (%d): %v", slaveId, err)
	}

	// ReceivePartialRecommendation
	var response syncutils.SlaveRecResponse
	err = syncutils.ReceiveJsonMessageAsObject(&response, conn)
	if err != nil {
		return fmt.Errorf("partialRecommendErr: Error receiving response from slave node (%d): %v", slaveId, err)
	}
	partialRecommendationCh <- &response
	return nil
}

func (master *Master) createBatches(nBatches, userId int, quantity int, genreIds []int, userFactors []float64) []syncutils.MasterRecRequest {
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
			UserRatings:  master.modelConfig.R[userId][startMovieId:endMovieId],
			StartMovieId: startMovieId,
			EndMovieId:   endMovieId,
			Quantity:     quantity,
			GenreIds:     genreIds,
			UserFactors:  userFactors,
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

func initializeUserFactors(numFeatures int) []float64 {
	userFactors := make([]float64, numFeatures)

	for i := 0; i < numFeatures; i++ {
		userFactors[i] = rand.Float64()*0.02 - 0.01 // Valores en el rango [-0.01, 0.01]
	}

	return userFactors
}

func FedAvg(gradients [][]float64, weights []float64) []float64 {
	numFeatures := len(gradients[0]) // Asumimos que todos los vectores tienen la misma longitud

	avgGrad := make([]float64, numFeatures)

	for i := 0; i < numFeatures; i++ {
		for j, grad := range gradients {
			avgGrad[i] += grad[i] * weights[j] // Pondera cada gradiente
		}
	}

	sumWeights := 0.0
	for _, w := range weights {
		sumWeights += w
	}

	for i := range avgGrad {
		avgGrad[i] /= sumWeights
	}

	return avgGrad // Retorna el gradiente promedio para actualizar el modelo global
}
