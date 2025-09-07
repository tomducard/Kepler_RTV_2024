package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	kafka "github.com/segmentio/kafka-go"
)

func getCurrentTime() string {
	return time.Now().Format("2006-01-02 15:04:05.000000")
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

type Product struct {
	ISIN       string  `json:"ISIN"`
	CUSIP      string  `json:"CUSIP"`
	PrivateRIC string  `json:"Private RIC"`
	Name       string  `json:"Name"`
	Decenter   float64 `json:"Decenter"`
	Kmargin    float64 `json:"Kech margin"`
	Kspread    float64 `json:"Kech spread"`
	Ispread    float64 `json:"Issuer spread"`
	Type       string  `json:"Contract type"`
	Bid        float64
	Ask        float64
	Mid        float64
	isAMC      bool
	BidKepler  float64
	AskKepler  float64
}

var filteredProducts []Product
var i = 0
var isUpdate = false
var nbProduit = 0

func main() {
	var rics []string
	var wgLex sync.WaitGroup
	var message string

	// Increment the wait group before calling fetchLexifi
	wgLex.Add(1)
	go func() {
		defer wgLex.Done()
		products := fetchLexifi()
		filteredProductsLocal, removedCount := filterEmptyRIC(products)
		filteredProducts = filteredProductsLocal

		for _, product := range filteredProductsLocal {
			//if i >= 50 {
			//	break
			//}
			rics = append(rics, product.PrivateRIC)
		}
		fmt.Println("Number of products removed:", removedCount)
	}()

	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("start producing ... !!")

	log.Println(message)
	wgLex.Wait()

	usage := "Usage: market_price_rdpgw_client_cred_auth.go --clientid clientid --clientsecret client secret " +
		"[--app_id app_id] [--position position] [--auth_url auth_url] " +
		"[--hostname hostname] [--port port] [--standbyhostname hostname] [--standbyport port] " +
		"[--discovery_url discovery_url] [--scope scope] [--service service] " +
		"[--region region] [--ric ric] [--hotstandby] [--help]"

	hostname := flag.String("hostname", "eu-west-1-aws-3-sm.optimized-pricing-api.refinitiv.net", "hostname")
	port := flag.String("port", "", "websocket port")
	appId := flag.String("app_id", "256", "application id")
	service := flag.String("service", "ELEKTRON_DD", "service")
	hotstandby := flag.Bool("hotstandby", false, "hotstandby")
	help := flag.Bool("help", false, "help")
	user := flag.String("user", "GE-A-00995300-3-6655", "user")
	password := flag.String("password", "AYAX~R-meG]DE_B\"YW?`9sa/`uqH^!", "password")

	positionDefault := ""
	host, _ := os.Hostname()
	addrs, _ := net.LookupIP(host)
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			positionDefault = fmt.Sprintf("%s", ipv4)
		}
	}

	position := flag.String("position", positionDefault, "position")

	flag.Usage = func() {
		log.Println(usage)
	}

	flag.Parse()
	log.SetFlags(0)

	flag.Parse()
	log.SetFlags(0)

	//addr := fmt.Sprintf("%s:%s", *hostname, *port)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	if *help {
		log.Println(usage)
		return
	}

	// an interruption for getting auth token and service discovery functions
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for _ = range c {
			os.Exit(0)
		}
	}()

	//number, err := strconv.ParseFloat(authMap["expires_in"].(string), 64)
	token, expired := getAuthToken(*user, *password)

	tokenTS := time.Now()

	// If hostname is specified, use it for the connection
	addr := fmt.Sprintf("%s:%s", *hostname, *port)
	addr2 := fmt.Sprintf("%s:%s", *hostname, *port)

	signal.Stop(c)
	close(c)

	log.Println(addr, addr2)

	interrupt = make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	done := make(chan struct{}, 2)
	connect := make(chan struct{}, 2)
	reconnect := make(chan struct{}, 2)
	newToken := make(chan string, 2)

	var wg sync.WaitGroup

	handler := func(addr string) {
		defer wg.Done()

		closed := make(chan struct{})
		token := ""

		// Main loop
		for {
			select {
			case token = <-newToken:
				break
			case <-done:
				return
			}

			// Start websocket handshake
			u := url.URL{Scheme: "wss", Host: "eu-west-1-aws-3-sm.optimized-pricing-api.refinitiv.net", Path: "/WebSocket"}
			h := http.Header{"Sec-WebSocket-Protocol": {"tr_json2"}}
			log.Printf(getCurrentTime()+" Connecting to WebSocket %s ...\n", u.String())

			dialer := websocket.Dialer{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Ignorer la vérification du certificat
			}

			c, _, err := dialer.Dial(u.String(), h)
			if err != nil {
				log.Println(getCurrentTime(), "WebSocket Connection Failed: ", err)
				connect <- struct{}{}
				continue
			} else {
				log.Println(getCurrentTime(), "WebSocket successfully connected!")
			}

			defer c.Close()

			sendLoginRequest(c, *appId, *position, token)

			go func() {
				// Read loop
				for {
					_, message, err := c.ReadMessage()
					if err != nil {
						log.Println(getCurrentTime(), "read:", err)
						closed <- struct{}{}
						return
					}

					var jsonArray []map[string]interface{}
					//log.Println(getCurrentTime(), "RECEIVED: ")
					json.Unmarshal(message, &jsonArray)
					//log.Println("Refresh recue")

					for _, jsonMessage := range jsonArray {
						// Parse JSON message at a high level
						switch jsonMessage["Type"] {
						case "Refresh":
							if jsonMessage["Domain"] == "Login" {
								sendMarketPriceRequest(c, rics, *service)
							}
							filteredProducts = printJsonBytesResp(message)
							produitsJSON, _ := json.Marshal(MessagePrep(filteredProducts))
							err := ioutil.WriteFile("code.txt", []byte(MessagePrep(filteredProducts)), 0644)
							if err != nil {
								fmt.Println("Erreur lors de l'écriture dans le fichier:", err)
								return
							}
							//fmt.Println("Code écrit dans le fichier code.txt avec succès.")
							if nbProduit != CountNonZeroBidAskKepler(filteredProducts) {
								isUpdate = true
							}

							if CountNonZeroBidAskKepler(filteredProducts) > 1600 && isUpdate {
								ProdKafka(string(produitsJSON), i, writer)
								nbProduit = CountNonZeroBidAskKepler(filteredProducts)
								i++
								isUpdate = false
							}
							continue

						case "Ping":
							sendMessage(c, []byte(`{"Type":"Pong"}`))
						case "Update":
							if jsonMessage["Domain"] == "Login" {
								sendMarketPriceRequest(c, rics, *service)
							}
							log.Println(getCurrentTime(), "RECEIVED: ")
							log.Println(jsonArray)

							re := regexp.MustCompile(`\[([^]]+)\]`)
							matches := re.FindAllStringSubmatch(string(message), -1)
							for _, match := range matches {
								name, ask, bid := decodeUpdate(match[1])
								fmt.Println("Name:", name)
								fmt.Println("BID:", bid)
								fmt.Println("ASK:", ask)
								if ask != 0 && bid != 0 && name != "" {
									ReplaceBidAskByRIC(filteredProducts, name, bid, ask, i, writer)
									i++
								}
								time.Sleep(5 * time.Second)
								fmt.Println("---")

							}
							continue
						default:
						}
					}
				}
			}()

			select {
			case <-done:
				log.Println(getCurrentTime(), "WebSocket Closed")
				c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				c.Close()
				return
			case <-closed:
				reconnect <- struct{}{}
				continue
			}
		}
	}

	defer wg.Wait()

	wg.Add(1)
	go handler(addr)
	newToken <- token
	if *hotstandby && len(addr2) != 0 {
		wg.Add(1)
		go handler(addr2)
		newToken <- token
	}

	for {
		select {
		case <-interrupt:
			done <- struct{}{}
			done <- struct{}{}
			return
		case <-connect:
			// Waiting a few seconds before attempting to reconnect
			time.Sleep(5 * time.Second)
			var deltaTime float64
			if expired < 600 {
				deltaTime = expired * 0.95
			} else {
				deltaTime = expired - 300
			}
			if time.Now().Sub(tokenTS).Seconds() >= deltaTime {
				token, expired = getAuthToken(*user, *password)
				tokenTS = time.Now()
			}
			newToken <- token
		case <-reconnect:
			// Waiting a few seconds before attempting to reconnect
			time.Sleep(5 * time.Second)
			token, expired = getAuthToken(*user, *password)
			tokenTS = time.Now()
			newToken <- token
		}
	}

}

func ProdKafka(message string, i int, writer *kafka.Writer) {
	// get kafka writer using environment variables.

	key := fmt.Sprintf("Key-%d", i)
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(fmt.Sprint(message)),
	}
	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("produced", key)
	}
	time.Sleep(1 * time.Second)
}

func filterEmptyRIC(products []Product) ([]Product, int) {
	var filteredProducts []Product
	removedCount := 0

	for _, product := range products {

		if product.Type == "Actively Manages Certificate" {
			product.isAMC = true
		}
		if product.PrivateRIC != "" {
			filteredProducts = append(filteredProducts, product)
		} else {
			removedCount++
		}
	}

	//Actively Manages Certificate

	return filteredProducts, removedCount
}

func fetchLexifi() []Product {
	url := "https://portfolio.keplercheuvreux.com/app/api/grid_report"
	token := "projets2023"

	payload := []byte(`{"view":"Export Prices Parameters","parameters":null,"execute_side_effects":false}`)

	req, err := http.NewRequest("PUT", url, ioutil.NopCloser(bytes.NewReader(payload)))
	if err != nil {
		fmt.Println("Erreur lors de la création de la requête:", err)
	}

	req.Header.Set("X-lexifi-token", token)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Erreur lors de l'envoi de la requête:", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Erreur: La requête a retourné un code de statut non OK:", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Erreur lors de la lecture du corps de la réponse:", err)
	}

	error := ioutil.WriteFile("Lexifi.txt", []byte(body), 0644)
	if error != nil {
	} else {
		fmt.Println("Code écrit dans le fichier Lexifi.txt avec succès.")
	}

	// Structure pour stocker la réponse JSON
	var products []Product

	// Décoder la réponse JSON dans la structure
	err = json.Unmarshal(body, &products)
	if err != nil {
		fmt.Println("Erreur lors du décodage JSON:", err)
	}

	return products
}

func MessagePrep(products []Product) string {
	var result string
	for _, p := range products {
		// Vérifier si BidKepler ou AskKepler est égal à zéro
		if p.BidKepler != 0 && p.AskKepler != 0 {
			result += fmt.Sprintf("ISIN: %s RIC: %s BidKepler: %.2f AskKepler: %.2f Bid: %.2f Mid: %.2f Ask: %.2f KSpread: %.2f Decenter: %.2f Kmargin: %.2f Ispread: %.2f\n", p.ISIN, p.PrivateRIC, p.BidKepler, p.AskKepler, p.Bid, p.Mid, p.Ask, p.Kspread, p.Decenter, p.Kmargin, p.Ispread)
		}
	}
	return result
}

func MessagePrepUpdate(p Product) string {
	result := fmt.Sprintf("ISIN: %s RIC: %s BidKepler: %.2f AskKepler: %.2f Bid: %.2f Mid: %.2f Ask: %.2f KSpread: %.2f Decenter: %.2f Kmargin: %.2f Ispread: %.2f\n Type : Update", p.ISIN, p.PrivateRIC, p.BidKepler, p.AskKepler, p.Bid, p.Mid, p.Ask, p.Kspread, p.Decenter, p.Kmargin, p.Ispread)
	return result
}

func sendLoginRequest(c *websocket.Conn, appId string, position string, token string) {
	sendMessage(c, []byte(`{"ID":1,"Domain":"Login","Key":{"NameType":"AuthnToken","Elements":{"ApplicationId":"`+appId+`","Position":"`+position+`","AuthenticationToken":"`+token+`"}}}`))
}

// Create and send simple Market Price request
func sendMarketPriceRequest(c *websocket.Conn, rics []string, service string) {
	// Construisez la demande en incluant tous les RICs dans une seule requête
	var request []byte
	request = append(request, `{"ID":2,"Key":{"Name":[`...)

	for i, ric := range rics {
		request = append(request, []byte(`"`+ric+`"`)...)
		if i < len(rics)-1 {
			request = append(request, `,`...)
		}
	}

	request = append(request, []byte(`],"Service":"`+service+`"}, "View":["BID","ASK","PRIMACT_1","SEC_ACT_1"]}`)...)
	//request = append(request, []byte(`],"Service":"`+service+`"}}`)...)
	// Envoyez la demande à l'aide de la fonction sendMessage
	sendMessage(c, request)
}

// Helper to send bytes over WebSocket connection
func sendMessage(c *websocket.Conn, message []byte) {
	log.Println(getCurrentTime(), "SENT:")
	//printJsonBytes(message)
	err := c.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println(getCurrentTime(), "Send Failed:", err)
	}
}

// Output bytes as formatted JSON
func printJsonBytes(bytes []byte) {
	var dat interface{}
	json.Unmarshal(bytes, &dat)
	bytesJson, _ := json.MarshalIndent(dat, "", "  ")
	log.Println(string(bytesJson))
}

func printJsonBytesResp(message []byte) []Product {
	var dat []interface{}
	if err := json.Unmarshal(message, &dat); err != nil {
		log.Println("Erreur lors de l'analyse JSON:", err)
	}

	for _, jsonData := range dat {
		switch item := jsonData.(type) {
		case map[string]interface{}:

			messageType, typeExists := item["Type"]
			if typeExists && messageType == "Status" {
				// Si le type de message est "Status", ignorer ce message
				continue
			}

			nameData, nameExists := item["Key"]
			fieldsData, fieldsExists := item["Fields"]

			if nameExists && fieldsExists {
				nameMap, ok := nameData.(map[string]interface{})
				if !ok {
					log.Println("Type de structure JSON non pris en charge pour Name:", reflect.TypeOf(nameData))
					continue
				}

				name, nameExists := nameMap["Name"]
				if !nameExists {
					log.Println("Champ manquant dans la structure JSON pour Name")
					continue
				}

				fieldsMap, ok := fieldsData.(map[string]interface{})
				if !ok {
					log.Println("Type de structure JSON non pris en charge pour Fields:", reflect.TypeOf(fieldsData))
					continue
				}

				ask, askExists := fieldsMap["ASK"]
				bid, bidExists := fieldsMap["BID"]
				primact, bidExists := fieldsMap["PRIMACT_1"]
				secact, bidExists := fieldsMap["SEC_ACT_1"]

				if askExists && bidExists {

					var produitActuel Product

					for _, produit := range filteredProducts {
						if produit.PrivateRIC == name.(string) {
							produitActuel = produit
							filteredProducts = removeProduct(filteredProducts, name.(string))
							break
						}
					}

					askFloat, ok := ask.(float64)
					if ok {
						produitActuel.Ask = askFloat
					}

					bidFloat, ok := bid.(float64)
					if ok {
						produitActuel.Bid = bidFloat
					}

					primactFloat, ok := primact.(float64)
					if ok {
						produitActuel.Bid = primactFloat
					}

					secactFloat, ok := secact.(float64)
					if ok {
						produitActuel.Ask = secactFloat
					}

					if produitActuel.Bid == 0 && produitActuel.Ask == 0 {
						break
					}

					produitActuel.BidKepler = BBidKepler(produitActuel)
					produitActuel.AskKepler = AAskKepler(produitActuel)

					if ok {
						produitActuel.Mid = (produitActuel.Ask + produitActuel.Bid) / 2
					}

					// Vérifier si le Name existe déjà dans le tableau

					filteredProducts = append(filteredProducts, produitActuel)
				}
			} else {
				//log.Println("Champs manquants dans la structure JSON:", item)
			}
		default:
			log.Println("Type de structure JSON non pris en charge dans la liste:", reflect.TypeOf(jsonData))
		}
	}

	return filteredProducts
}

func getAuthToken(user string, password string) (string, float64) {
	log.Println(getCurrentTime(), "Sending authentication request...")
	// Send login info for authentication token
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transCfg}

	authResp, err := client.PostForm("https://api.refinitiv.com:443/auth/oauth2/v1/token", url.Values{"username": {user}, "password": {password}, "grant_type": {"password"}, "client_id": {"90fdfe49ff5f4f739b934c1ed80d43ca6b8de363"}, "takeExclusiveSignOnControl": {"true"}})

	if err != nil {
		log.Println("Token request failed: ", err)
	}

	authData, _ := ioutil.ReadAll(authResp.Body)

	authResp.Body.Close()

	var authMap map[string]interface{}
	json.Unmarshal(authData, &authMap)

	log.Println("RECEIVED:")
	bytesJson, _ := json.MarshalIndent(authMap, "", "  ")
	log.Println(string(bytesJson))

	// an interruption for getting auth token and service discovery functions
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for _ = range c {
			os.Exit(0)
		}
	}()

	number, err := strconv.ParseFloat(authMap["expires_in"].(string), 64)
	token, expired := authMap["access_token"].(string), number
	log.Printf("Authentication Succeeded. Received AuthToken: %s\n", token)

	return token, expired
}

func decodeUpdate(input string) (string, float64, float64) {
	type Key struct {
		Service string `json:"Service"`
		Name    string `json:"Name"`
	}

	type Update struct {
		ID            int                    `json:"ID"`
		Type          string                 `json:"Type"`
		UpdateType    string                 `json:"UpdateType"`
		DoNotConflate bool                   `json:"DoNotConflate"`
		Key           Key                    `json:"Key"`
		SeqNumber     int                    `json:"SeqNumber"`
		Fields        map[string]interface{} `json:"Fields"`
	}

	var update Update
	err := json.Unmarshal([]byte(input), &update)
	if err != nil {
		fmt.Println("Erreur de décodage JSON:", err)
		return "", 0, 0
	}

	// Accéder aux champs requis
	name := update.Key.Name

	var secAct, primAct float64
	if secActVal, ok := update.Fields["SEC_ACT_1"].(float64); ok {
		secAct = secActVal
	} else {
		fmt.Println("SEC_ACT_1 manquant ou de type incorrect")
	}

	if primActVal, ok := update.Fields["PRIMACT_1"].(float64); ok {
		primAct = primActVal
	} else {
		fmt.Println("PRIMACT_1 manquant ou de type incorrect")
	}

	return name, secAct, primAct
}

func removeProduct(slice []Product, name string) []Product {
	for i, produit := range slice {
		if produit.PrivateRIC == name {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func BBidKepler(produit Product) float64 {
	var DeltaS float64
	if produit.Ask != 0 {
		DeltaS = produit.Ask - produit.Bid
	} else {
		DeltaS = produit.Kspread * 100
	}
	BBidKepler := produit.Bid + produit.Kmargin + DeltaS*produit.Decenter
	return BBidKepler
}

func AAskKepler(produit Product) float64 {
	AAskKepler := produit.BidKepler + (100 * produit.Kspread)
	return AAskKepler
}

func CountNonZeroBidAskKepler(products []Product) int {
	count := 0
	for _, p := range products {
		if p.BidKepler != 0 && p.AskKepler != 0 {
			count++
		}
	}
	return count
}

func ReplaceBidAskByRIC(products []Product, name string, bid float64, ask float64, j int, writer *kafka.Writer) {
	for i, p := range products {
		if p.PrivateRIC == name {
			products[i].Bid = bid
			products[i].Ask = ask
			products[i].BidKepler = BBidKepler(products[i])
			products[i].AskKepler = AAskKepler(products[i])
			jsonMessage := MessagePrepUpdate(products[i])
			ProdKafka(jsonMessage, j, writer)
		}
	}
}
