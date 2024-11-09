package main

import (
	"bufio"
	"fmt"

	"net"
	"os"
	"recommendation-service/syncutils"
	"strconv"
	"strings"
)

var endProgram = false

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
	fmt.Println("Presione 'Y' para salir.")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	if strings.TrimSpace(input) == "Y" {
		endProgram = true
	}

}

type Genres struct {
	Genresname []string `json:"movieGenreNames"`
}

func LoadGenres(genres *Genres) error {
	err := syncutils.LoadJsonFile("config/master.json", &genres)
	if err != nil {
		return fmt.Errorf("Error al cargar el archivo de configuración de géneros: %v", err)
	}
	return nil
}

func displayGenres(genres *Genres) {
	fmt.Println("************************************")
	fmt.Println("Available Genres")
	fmt.Println("************************************")
	for i, genre := range genres.Genresname {
		fmt.Printf("%d. %s\n", i, genre)
	}
	fmt.Println("************************************")
}

func getUserInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func menu() {

}

func Banner() {
	fmt.Println("  ____                 _                            _             _   _             ")
	fmt.Println(" |  _ \\ ___  __ _  ___| |_ ___  _ __ ___   ___  __| | __ _ _ __ | |_(_) ___  _ __  ")
	fmt.Println(" | |_) / _ \\/ _` |/ __| __/ _ \\| '_ ` _ \\ / _ \\/ _` |/ _` | '_ \\| __| |/ _ \\| '_ \\ ")
	fmt.Println(" |  _ <  __/ (_| | (__| || (_) | | | | | |  __/ (_| | (_| | | | | |_| | (_) | | | |")
	fmt.Println(" |_| \\_\\___|\\__,_|\\___|\\__\\___/|_| |_| |_|\\___|\\__,_|\\__,_|_| |_|\\__|_|\\___/|_| |_|")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" __          __  _                            _          __  __                                    ")
	fmt.Println(" \\ \\        / / | |                          | |        |  \\/  |                                   ")
	fmt.Println("  \\ \\  /\\  / /__| | ___ ___  _ __ ___   ___  | |_ ___   | \\  / | __ _ _ __   __ _  __ _  ___ _ __  ")
	fmt.Println("   \\ \\/  \\/ / _ \\ |/ __/ _ \\| '_ ` _ \\ / _ \\ | __/ _ \\  | |\\/| |/ _` | '_ \\ / _` |/ _` |/ _ \\ '__| ")
	fmt.Println("    \\  /\\  /  __/ | (_| (_) | | | | | |  __/ | || (_) | | |  | | (_| | | | | (_| | (_| |  __/ |    ")
	fmt.Println("     \\/  \\/ \\___|_|\\___\\___/|_| |_| |_|\\___|  \\__\\___/  |_|  |_|\\__,_|_| |_|\\__,_|\\__, |\\___|_|    ")
	fmt.Println("                                                                                     __/ |         ")
	fmt.Println("                                                                                    |___/          ")
	fmt.Println("--------------------------------------------------------------------------------")
}

func createClientRecRequest(userId int, quantity int, genreIds []int) syncutils.ClientRecRequest {
	return syncutils.ClientRecRequest{
		UserId:   userId,
		Quantity: quantity,
		GenreIds: genreIds,
	}
}

func idHandler() int {
	userIdInput := getUserInput("Enter your user ID (0-1200): ")
	userId, err := strconv.Atoi(userIdInput)
	if err != nil || userId < 0 || userId > 1200 {
		panic("Invalid user ID")
	}
	return userId
}


func appHandler(){
	
	userId := idHandler()
	Genres := Genres{}
	err := LoadGenres(&Genres)
	if err != nil {
		panic(err)
	}

	displayGenres(&Genres)

	genreInput := getUserInput("Enter the genre indices (comma separated): ")
	genreIndices := strings.Split(genreInput, ",")
	var genreIds []int
	for _, index := range genreIndices {
		id, err := strconv.Atoi(strings.TrimSpace(index))
		if err == nil {
			genreIds = append(genreIds, id)
		}
	}

	quantityInput := getUserInput("Enter the number of recommendations: ")
	quantity, err := strconv.Atoi(quantityInput)
	if err != nil {
		panic("Invalid quantity")
	}
	fmt.Printf("User ID: %d\n", userId)
	fmt.Printf("Genres: %v\n", genreIds)
	fmt.Printf("Quantity: %d\n", quantity)
	request := createClientRecRequest(userId, quantity, genreIds)

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", "172.21.0.3", syncutils.ServicePort))
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

func main() {

	Banner()
	appHandler()


}
