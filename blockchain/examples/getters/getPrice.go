package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"

	"github.com/sonm-io/core/blockchain"
)

func main() {

	api, err := blockchain.NewAPI()
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}

	p, err := loadSNMPriceUSD()
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}


	log.Println(p)

	price, err := api.OracleUSD().GetCurrentPrice(context.TODO())
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
	log.Println(price)
}

func divideSNM(price float64) *big.Int {
	snmcount := int64(1 / price * 1000000000000000000)
	log.Println(snmcount)
	return big.NewInt(snmcount)
}

func loadSNMPriceUSD() (float64, error) {
	body, err := getJson("https://api.coinmarketcap.com/v1/ticker/sonm/")
	if err != nil {
		return 0, err
	}
	var tickerSnm []*tokenData
	_ = json.Unmarshal(body, &tickerSnm)
	return strconv.ParseFloat(tickerSnm[0].PriceUsd, 64)
}

func getJson(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

type tokenData struct {
	ID       string `json:"id"`
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	PriceUsd string `json:"price_usd"`
}
