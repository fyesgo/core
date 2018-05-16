package main

import (
	"context"
	"log"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	txHash := common.HexToHash("0xe0bc7b7a7ca5e19b8250b8756025147765832144d1d5a01694ced3cc3a7becf0")
	txHash.Hex()

	client, err := ethclient.Dial("https://sidechain-dev.sonm.com")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}

	receipt, err := client.TransactionReceipt(context.TODO(), txHash)
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
	log.Println(receipt)
}
