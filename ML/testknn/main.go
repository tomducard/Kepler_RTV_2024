package main

import (
	"fmt"
	"log"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	base "github.com/sjwhitworth/golearn/base"
	evaluation "github.com/sjwhitworth/golearn/evaluation"
	knn "github.com/sjwhitworth/golearn/knn"
)

func main() {
	rawData, err := base.ParseCSVToInstances("dataset.csv", true)
	if err != nil {
		panic(err)
	}

	// Initialiser un classificateur KNN
	cls := knn.NewKnnClassifier("euclidean", "linear", 2)

	// Entraîner le modèle
	trainData, testData := base.InstancesTrainTestSplit(rawData, 0.50)
	cls.Fit(trainData)
	fmt.Println(testData)

	// Faire des prédictions sur les données de test
	predictions, err := cls.Pr

	if err != nil {
		log.Fatal(err)
	}

	// Évaluer les performances du modèle
	confusionMat, err := evaluation.GetConfusionMatrix(testData, predictions)
	if err != nil {
		log.Fatal(err)
	}

	// Afficher la matrice de confusion
	fmt.Println("Resultat entrainement :", confusionMat)

	df := convertToDataFrame(230.45, 231.95)
	classifyProduct(cls, df)

	// Afficher le résultat
	// fmt.Printf("Le produit est valide: %t\n", isValid)
	// fmt.Printf("Score de prédiction: %.2f\n", score)
}

// Fonction pour classifier un produit donné comme valide ou non
func classifyProduct(cls *knn.KNNClassifier, product *dataframe.DataFrame) {
	log.Println("Demarage Verif")

	instances := base.ConvertDataFrameToInstances(product, 1)
	fmt.Println(instances)

	// Faire la prédiction sur l'instance unique
	predictions, err := cls.Predict(instances)
	if err != nil {
		log.Fatal(err)
	}

	// Obtenir les probabilités prédites
	log.Println("Resultat prediction")
	fmt.Println(evaluation.GetConfusionMatrix(instances, predictions))
}

func convertToDataFrame(value1, value2 float64) *dataframe.DataFrame {
	// Création d'une nouvelle série avec les valeurs fournies
	isin := dataframe.NewSeriesString("ISIN", nil, "CH1248352457", "Default")
	valid := dataframe.NewSeriesString("Valid", nil, "valide", "invalide")
	series := dataframe.NewSeriesFloat64("BID", nil, value1, 0)
	defaultSeries := dataframe.NewSeriesFloat64("ASK", nil, value2, 0)

	// Création d'un DataFrame avec les deux séries créées
	df := dataframe.NewDataFrame(isin, valid, series, defaultSeries)
	// Création d'un DataFrame avec la série créée

	return df
}
