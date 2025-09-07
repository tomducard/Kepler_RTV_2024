package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	kafka "github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Quote struct {
	ISIN      string  `json:"ISIN" bson:"ISIN"`
	RIC       string  `json:"RIC" bson:"RIC"`
	BidKepler float64 `json:"BidKepler" bson:"BidKepler"`
	AskKepler float64 `json:"AskKepler" bson:"AskKepler"`
	ScoreAbs  float64
	ScoreRel  float64
	ScoreML   float64
	Valid     int
}

type Asset struct {
	ISIN            string   `json:"ISIN"`
	PercentagePrice *float64 `json:"Percentage Price"`
}

func getMongoCollection(mongoURL, dbName, collectionName string) *mongo.Collection {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB ... !!")

	db := client.Database(dbName)
	collection := db.Collection(collectionName)
	return collection
}

func writeToCSV(quote Quote) error {
	file, err := os.OpenFile("Data.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{quote.ISIN, quote.RIC, strconv.FormatFloat(quote.BidKepler, 'f', -1, 64), strconv.FormatFloat(quote.AskKepler, 'f', -1, 64), strconv.FormatFloat(quote.ScoreAbs, 'f', -1, 64), strconv.FormatFloat(quote.ScoreRel, 'f', -1, 64), strconv.FormatFloat(quote.ScoreML, 'f', -1, 64), fmt.Sprintf("%d", quote.Valid)}
	if err := writer.Write(record); err != nil {
		return err
	}
	return nil
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func parseMessage(msg kafka.Message) []Quote {
	var quotes []Quote

	regex := regexp.MustCompile(`ISIN: ([^\s]+) RIC: ([^\s]+) BidKepler: ([^\s]+) AskKepler: ([^\s]+)`)
	matches := regex.FindAllStringSubmatch(string(msg.Value), -1)

	for _, match := range matches {
		quote := Quote{
			ISIN:      match[1],
			RIC:       match[2],
			BidKepler: convertToFloat(match[3]),
			AskKepler: convertToFloat(match[4]),
		}
		quotes = append(quotes, quote)
	}

	return quotes
}

func convertToFloat(s string) float64 {
	f := strings.Replace(s, ",", ".", -1)
	result, err := strconv.ParseFloat(f, 64)
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func main() {
	filteredAssets := fetchLexify()
	// get Mongo db Collection using environment variables.
	mongoURL := os.Getenv("mongoURL")
	dbName := os.Getenv("dbName")
	collectionName := os.Getenv("collectionName")
	collection := getMongoCollection(mongoURL, dbName, collectionName)

	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := os.Getenv("groupID")
	reader := getKafkaReader(kafkaURL, topic, groupID)

	defer reader.Close()

	fmt.Println("start consuming ... !!")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		if strings.Contains(string(msg.Value), "Update") {

			fmt.Printf("Update recue %s\n", string(msg.Value))
			quote, err := UpdateTraitement(msg)
			quote.Valid = 0
			if err != nil {
				log.Println("Error processing update message:", err)
				continue
			}

			count, mean, errAverage := AverageMean(quote.ISIN)

			if errAverage != nil {
				log.Println("Error processing update message:", err)
				continue
			} else {
				quote.ScoreRel = math.Abs(((quote.BidKepler - mean) / mean) * 100)

				if quote.ScoreRel != 0 && quote.ScoreRel < 3 {
					quote.Valid = 1
				}

			}

			if quote.ScoreAbs != 0 && quote.ScoreAbs < 3 {
				quote.Valid = 1
			}

			updateQuoteWithScore(&quote, filteredAssets)
			log.Println("ISIN ", quote.ISIN, " Moyenne ", mean, " Count ", count, "Score Rel", quote.ScoreRel, "Score Abs", quote.ScoreAbs, " Valid ", quote.Valid)

			if quote.ScoreAbs != 0 || quote.ScoreRel != 0 {
				err = writeToCSV(quote)
				if err != nil {
					log.Println("Error writing to CSV:", err)
				}
			}

			_, err = collection.InsertOne(context.Background(), quote)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Inserted an Update: ", quote)

		} else {
			quotes := parseMessage(msg)

			for _, quote := range quotes {
				//updateQuoteWithScore(&quote, filteredAssets)
				// err := writeToCSV(quote)
				// if err != nil {
				// 	log.Println("Error writing to CSV:", err)
				// }
				if quote.ScoreAbs < 3 {
					_, err = collection.InsertOne(context.Background(), quote)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println("Inserted a single document: ", quote)
				}
			}
		}
	}
}

func fetchLexify() []Asset {

	var filteredAssets []Asset
	url := "https://portfolio.keplercheuvreux.com/app/api/grid_report"
	token := "projets2023"

	payload := []byte(`{"view":"Absolute pricing","parameters":null,"execute_side_effects":false}`)

	req, err := http.NewRequest("PUT", url, ioutil.NopCloser(bytes.NewReader(payload)))
	if err != nil {
		fmt.Println("Erreur lors de la création de la requête:", err)
		return filteredAssets
	}

	req.Header.Set("X-lexifi-token", token)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Erreur lors de l'envoi de la requête:", err)
		return filteredAssets
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Erreur: La requête a retourné un code de statut non OK:", resp.Status)
		return filteredAssets
	}

	var assets []Asset
	err = json.NewDecoder(resp.Body).Decode(&assets)
	if err != nil {
		fmt.Println("Erreur lors du décodage de la réponse JSON:", err)
		return filteredAssets
	}

	for _, asset := range assets {
		if asset.PercentagePrice != nil {
			// Vérification que le pointeur est différent de nil
			// Déférencement du pointeur pour obtenir la valeur float64
			percentage := *asset.PercentagePrice

			// Multiplie par 100
			percentage *= 100

			// Stocke la nouvelle valeur dans la structure Asset
			asset.PercentagePrice = &percentage
			if *asset.PercentagePrice > 1 {
				filteredAssets = append(filteredAssets, asset)
			}
		}
	}

	file, err := os.Create("Lexify.txt")
	if err != nil {
		fmt.Println("Erreur lors de la création du fichier Lexify.txt:", err)
		return filteredAssets
	}
	defer file.Close()

	// Écriture des données dans le fichier
	for _, asset := range filteredAssets {
		line := fmt.Sprintf("ISIN: %s, Percentage Price: %.2f\n", asset.ISIN, *asset.PercentagePrice)
		_, err := file.WriteString(line)
		if err != nil {
			fmt.Println("Erreur lors de l'écriture dans le fichier Lexify.txt:", err)
			return filteredAssets
		}
	}

	fmt.Println("Données écrites avec succès dans le fichier Lexify.txt.")
	return filteredAssets
}

func updateQuoteWithScore(quote *Quote, assets []Asset) {
	for _, asset := range assets {
		if asset.ISIN == quote.ISIN {
			if asset.PercentagePrice != nil {
				percentagePrice := *asset.PercentagePrice
				difference := ((quote.BidKepler - percentagePrice) / percentagePrice) * 100
				quote.ScoreAbs = math.Abs(difference)
				return
			}
		}
	}
	// Si l'ISIN n'est pas trouvé, le score est 0
	quote.ScoreAbs = 0
}

func UpdateTraitement(msg kafka.Message) (Quote, error) {
	// Utilisez une expression régulière pour extraire les données nécessaires du message
	regex := regexp.MustCompile(`ISIN: ([^\s]+) RIC: ([^\s]+) BidKepler: ([^\s]+) AskKepler: ([^\s]+)`)
	matches := regex.FindStringSubmatch(string(msg.Value))

	if len(matches) < 5 {
		fmt.Println("Erreur: Impossible de traiter le message Update.")
		return Quote{}, errors.New("Impossible de traiter le message Update")
	}

	// Extrayez les valeurs pertinentes du message
	isin := matches[1]
	ric := matches[2]
	bidKepler := convertToFloat(matches[3])
	askKepler := convertToFloat(matches[4])

	// Créez une structure Quote avec les données extraites
	quote := Quote{
		ISIN:      isin,
		RIC:       ric,
		BidKepler: bidKepler,
		AskKepler: askKepler,
	}

	return quote, nil
}

func AverageMean(isin string) (int, float64, error) {
	// Récupérer l'URL de la base de données MongoDB depuis les variables d'environnement
	mongoURL := os.Getenv("mongoURL")
	dbName := os.Getenv("dbName")
	collectionName := os.Getenv("collectionName")

	// Se connecter à la base de données MongoDB
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		return 0, 0, err
	}
	defer client.Disconnect(context.Background())

	// Vérifier la connexion à la base de données
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return 0, 0, err
	}

	// Sélectionner la collection dans la base de données
	collection := client.Database(dbName).Collection(collectionName)

	// Définir les options de recherche pour filtrer les produits par ISIN
	findOptions := options.Find()
	findOptions.SetProjection(bson.M{"BidKepler": 1}) // Ne récupère que les valeurs de BidKepler

	// Rechercher tous les documents avec le même ISIN
	cursor, err := collection.Find(context.Background(), bson.M{"ISIN": isin}, findOptions)
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close(context.Background())

	// Carte pour garder une trace des valeurs uniques
	uniqueValues := make(map[float64]bool)

	// Parcourir les résultats de la recherche
	for cursor.Next(context.Background()) {
		var result Quote
		if err := cursor.Decode(&result); err != nil {
			return 0, 0, err
		}

		// Ajouter la valeur de BidKepler à la carte des valeurs uniques
		uniqueValues[result.BidKepler] = true
	}

	// Vérifier les erreurs lors du parcours du curseur
	if err := cursor.Err(); err != nil {
		return 0, 0, err
	}

	// Calculer la moyenne
	total := 0.0
	count := len(uniqueValues)

	for value := range uniqueValues {
		total += value
	}

	average := total / float64(count)

	return count, average, nil
}
