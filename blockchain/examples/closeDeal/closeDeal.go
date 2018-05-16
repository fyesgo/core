package main

import (
	"context"
	"log"
	"math/big"

	"github.com/sonm-io/core/blockchain"
)

const (
	hexKey = "a5dd45e0810ca83e21f1063e6bf055bd13544398f280701cbfda1346bcf3ae64"
)

func main() {
	// prv, err := crypto.HexToECDSA(hexKey)
	// if err != nil {
	// 	log.Fatalln(err)
	// 	return
	// }

	api, err := blockchain.NewAPI()
	if err != nil {
		log.Fatalln(err)
		return
	}

	dealId := big.NewInt(7)

	info, err := api.Market().GetDealInfo(context.TODO(), dealId)

	log.Println(info)

	// err = <-api.Market().Bill(context.TODO(), prv, dealId)
	// if err != nil {
	// 	log.Fatalln("via Bill: ", err)
	//
	// }

	// err = <-api.Market().CloseDeal(context.TODO(), prv, dealId, false)
	// if err != nil {
	// 	log.Fatalln("via close:", err)
	//
	// }
	//
	// info, err = api.Market().GetDealInfo(context.TODO(), dealId)

	log.Println(info)

	// log.Println("closed")
}
