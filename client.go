package main

import "recommendation-service/slave"

func main() {
	var node slave.Slave
	err := node.Init()
	if err != nil {
		panic(err)
	}
	node.Run()
}
