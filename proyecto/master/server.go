package main

import "recommendation-service/master"

func main() {
	var node master.Master
	err := node.Init()
	if err != nil {
		panic(err)
	}
	err = node.Run()
	if err != nil {
		panic(err)
	}
}
