package main

import (
	"fmt"
	"net"
	"recommendation-service/syncutils"
)

func displayRecommendations(response *syncutils.MasterRecResponse) {
	fmt.Println("************************************")
	fmt.Println("Recommendations for user you")
	fmt.Println("************************************")
	if (len(response.Recommendations)) == 0 {
		fmt.Println("No se encontrarón recomendaciones que cumplieran con las categorías indicadas entre nuestras películas disponibles.")
		return
	}
	fmt.Printf("Se encontraron las siguiente %d películas.", len(response.Recommendations))
	fmt.Println("------------------------------------")
	for _, rec := range response.Recommendations {
		fmt.Printf("%s\n[%v]\nSe recomendo la película por tener un rating estimado de %f\n", rec.Title, rec.Genres, rec.Rating)
		fmt.Println("------------------------------------")
	}
}

func main() {
	request := syncutils.ClientRecRequest{
		UserId:   4,
		Quantity: 10,
		GenreIds: []int{},
	}
	ip := syncutils.GetOwnIp()
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ip, syncutils.SyncronizationPort))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	err = syncutils.SendObjectAsJsonMessage(&request, &conn)
	if err != nil {
		panic(err)
	}
	var response syncutils.MasterRecResponse
	err = syncutils.ReceiveJsonMessageAsObject(&response, &conn)
	if err != nil {
		panic(err)
	}
	displayRecommendations(&response)
}
