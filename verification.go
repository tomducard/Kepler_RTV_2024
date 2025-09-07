package main

import (
	"fmt"
	"math"
)

// VERIF RELATIVE

const (
	facteurEcartMoyenne = 0.5
	facteurVolatilite   = 0.2
	facteurVar          = 0.3
	facteurHistorique   = 0.1
)

func calculerScore(prixActuel, moyenne, volatilite, varInf, varSup float64, historique []float64) float64 {
	score := 100.0

	//On calcule l'écart par rapport à la moyenne
	ecartMoyenne := math.Abs(prixActuel - moyenne)
	score -= facteurEcartMoyenne * ecartMoyenne

	//On calcule la dégradation en fonction de la volatilité
	score -= facteurVolatilite * volatilite

	//On calcule la dégradation en fonction de la VaR
	if prixActuel < varInf || prixActuel > varSup {
		score -= facteurVar * (varSup - varInf)
	}

	//Analyse de l'historique des prix
	score -= facteurHistorique * analyseHistorique(historique)

	//Vérification que le score ne soit pas négatif
	return math.Max(score, 0)
}

func analyseHistorique(historique []float64) float64 {
	//Fonction pour examiner la stabilité des prix dans l'historique à faire
	return 0
}

// VERIF ABSOLUE
func calculatePriceVariation(lastPrice, currentPrice float64) float64 {
	// Calcul de la variation relative en pourcentage
	return ((currentPrice - lastPrice) / lastPrice) * 100
}

func assignScore(variation float64) int {
	// Assignation des scores en fonction de la variation
	switch {
	case variation < -10:
		return 1 // Score bas pour une baisse de plus de 10%
	case variation < 0:
		return 2 // Score moyen pour une baisse
	case variation == 0:
		return 3 // Score neutre pour aucune variation
	case variation <= 5:
		return 4 // Score moyen pour une légère hausse
	default:
		return 5 // Score élevé pour une hausse de plus de 5%
	}
}

func main() {
	//Exemple Verif Relative
	prixActuel := 110.0
	moyenne := 100.0
	volatilite := 10.0
	varInf := 90.0
	varSup := 110.0
	historique := []float64{95.0, 105.0, 98.0, 102.0, 110.0}

	scoreProduit := calculerScore(prixActuel, moyenne, volatilite, varInf, varSup, historique)
	fmt.Printf("Score verification Relative: %.2f\n", scoreProduit)

	//Exemple Verif Absolue
	lastPrice := 100.0
	currentPrice := 90.0

	// Calcul de la variation relative
	variation := calculatePriceVariation(lastPrice, currentPrice)

	// Attribution du score
	score := assignScore(variation)

	// Affichage des résultats
	fmt.Printf("\nVariation absolue : %.2f%%\n", variation)
	fmt.Printf("Score attribué : %d\n", score)
}
