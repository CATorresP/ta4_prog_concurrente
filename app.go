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
		fmt.Printf("%s\nSe recomendo la película por tener un rating estimado de %f\n", rec.MovieTitle, rec.Rating)
		fmt.Println("------------------------------------")
	}
}

func main() {
	request := syncutils.ClientRecRequest{
		UserId:   4,
		Quantity: 10,
	}
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		panic(err)
	}
	syncutils.SendObjectAsJsonMessage(&request, &conn)

	var response syncutils.MasterRecResponse
	err = syncutils.ReceiveJsonMessageAsObject(&response, &conn)
	if err != nil {
		panic(err)
	}
	displayRecommendations(&response)
}
