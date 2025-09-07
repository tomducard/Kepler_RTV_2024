package main

import (
	"fmt"
	"math/rand"

	base "github.com/sjwhitworth/golearn/base"
	evaluation "github.com/sjwhitworth/golearn/evaluation"
	model "github.com/sjwhitworth/golearn/linear_models"
)

func main() {

	rand.Seed(4402201)

	rawData, err := base.ParseCSVToInstances("dataformate.csv", true)
	if err != nil {
		panic(err)
	}

	//Initialises a new AveragePerceptron classifier
	cls := model.NewLinearRegression()

	//Do a training-test split
	trainData, testData := base.InstancesTrainTestSplit(rawData, 0.20)
	fmt.Println(trainData)
	fmt.Println(testData)
	cls.Fit(trainData)

	predictions, _ := cls.Predict(testData)
	fmt.Println(predictions)

	// Prints precision/recall metrics
	confusionMat, _ := evaluation.GetConfusionMatrix(testData, predictions)
	fmt.Println(evaluation.GetSummary(confusionMat))

}
