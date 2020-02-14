package main

import (
	"fmt"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/batch_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/transaction_pb2"
	"github.com/taekion-org/intkey-stress-test/intkey"
	"os"
	flag "github.com/spf13/pflag"
	"time"
)

const DEFAULT_URL = "http://localhost:8008"
const DEFAULT_TRANSPORT = "rest"
const DEFAULT_WAIT_TIME = 30

func handleError(err error) {
	fmt.Println(err)
	os.Exit(-1)
}

func main() {
	var err error
	var url *string = flag.String("url", DEFAULT_URL, "Sawtooth URL")
	var keyFile *string = flag.String("keyfile", "", "Sawtooth Private Key File")
	var transport *string = flag.String("transport", DEFAULT_TRANSPORT, "Sawtooth Transport")
	var intName *string = flag.String("int_name", "testkey", "Name of integer to increment")
	var batchSize *int = flag.Int("batch_size", 100, "Number of transactions per batch")
	var batchCount *int= flag.Int("batch_count", 1, "Number of batches to submit")
	var delay *int = flag.Int("delay", 1000, "Milliseconds between submits")

	flag.Parse()

	var client *intkey.IntkeyClient
	if *transport == "rest" {
		client, err = intkey.NewIntkeyClient(*url, *keyFile)
		if err != nil {
			handleError(err)
		}
	} else if *transport == "zmq" {
		client, err = intkey.NewIntkeyClientZmq(*url, *keyFile)
		if err != nil {
			handleError(err)
		}
	}

	// See if the value exists
	_, err = client.Show(*intName)
	if err == nil {
		handleError(fmt.Errorf("You must specify an unused integer name"))
	}

	batchIds := make([]string, *batchCount + 1)
	var lastTransactionId string

	initPayload := intkey.IntkeyPayload{
		Verb:  intkey.VERB_SET,
		Name:  *intName,
		Value: 0,
	}

	initTransaction, err := client.SawtoothClient.CreateTransaction(&initPayload)
	if err != nil {
		handleError(err)
	}
	lastTransactionId = initTransaction.HeaderSignature

	initBatch, err := client.CreateBatch([]*transaction_pb2.Transaction{initTransaction})
	if err != nil {
		handleError(err)
	}

	initBatchList, err := client.CreateBatchList([]*batch_pb2.Batch{initBatch})
	if err != nil {
		handleError(err)
	}

	batchIds[0] = initBatch.HeaderSignature
	err = client.SawtoothClient.Transport.SubmitBatchList(initBatchList)
	if err != nil {
		handleError(err)
	}

	for i := 1; i < *batchCount+1; i++ {
		fmt.Printf("Batch: %d\n", i)

		transactions := make([]*transaction_pb2.Transaction, *batchSize)
		for j := 0; j < *batchSize; j++ {
			incPayload := intkey.IntkeyPayload{
				Verb:  intkey.VERB_INC,
				Name:  *intName,
				Value: 1,
			}
			incPayload.AddDependency(lastTransactionId)

			transactions[j], err = client.SawtoothClient.CreateTransaction(&incPayload)
			if err != nil {
				handleError(err)
			}
		}

		lastTransactionId = transactions[len(transactions)-1].HeaderSignature

		batch, err := client.CreateBatch(transactions)
		if err != nil {
			handleError(err)
		}

		batchIds[i] = batch.HeaderSignature

		batchList, err := client.CreateBatchList([]*batch_pb2.Batch{batch})
		if err != nil {
			handleError(err)
		}

		for err := client.SawtoothClient.Transport.SubmitBatchList(batchList); err != nil; err = client.SawtoothClient.Transport.SubmitBatchList(batchList) {
			fmt.Printf("Error: %s\n", err)
			time.Sleep(1 * time.Second)
		}
		//if err != nil {
		//	handleError(err)
		//}
		time.Sleep(time.Duration(*delay) * time.Millisecond)
	}

	for i, batchId := range batchIds {
		fmt.Printf("Waiting for batch %d (%s)\n", i, batchId)
		status, err := client.SawtoothClient.Transport.GetBatchStatus(batchId, 60)
		if err != nil {
			handleError(err)
		}
		fmt.Printf("\t%s\n", status)
		if status != "COMMITTED" {
			handleError(fmt.Errorf("Batch not completed..."))
		}
	}

	// Display the result value
	result, err := client.Show(*intName)
	if err != nil {
		handleError(err)
	}

	fmt.Printf("Result: %d\n", result)
}
