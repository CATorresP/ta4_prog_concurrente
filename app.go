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
	fmt.Printf("Se encontraron las siguiente %d películas.\n", len(response.Recommendations))
	fmt.Println("------------------------------------")
	for _, rec := range response.Recommendations {
		fmt.Printf("%s\n%v\nSe recomendo la película por tener un rating estimado de %f\n%s.\n", rec.Title, rec.Genres, rec.Rating, rec.Comment)
		fmt.Println("------------------------------------")
	}
}

func main() {
	request := syncutils.ClientRecRequest{
		UserId:   4,
		Quantity: 10,
		GenreIds: []int{},
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", "172.18.0.6", syncutils.ServicePort))
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
