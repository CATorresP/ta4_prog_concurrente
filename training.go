package main

import (
	"fmt"
	"log"
	"recommendation-service/model"
)

func main() {
	R, err := model.LoadTrainData("./dataset/clean_ratings.csv")
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Println(len(R[0]))
	/*
		grids := model.ModelGrid{
			NumFeatures:    []int{100},
			Epochs:         []int{100, 500},
			LearningRate:   []float64{0.01, 0.001, 0.0001},
			Regularization: []float64{0.01, 0.001, 0.0001},
		}
		model.SearchGrid(grids, R)
	*/
	log.Println("Model")
	model := model.NewModel(100, 500, 0.001, 0.0001, R, 1)
	model.Train()
	log.Println(model.CalculateRMSE())
	model.ParamsToJson("./model/model.json")
}
