package main

import (
	"fmt"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
	"os"
	flag "github.com/spf13/pflag"
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

	// Set the value to zero
	_, err = client.Set(*intName, 0, DEFAULT_WAIT_TIME)
	if err != nil {
		handleError(err)
	}

	payload := intkey.IntkeyPayload{
		Verb:  intkey.VERB_INC,
		Name:  *intName,
		Value: 1,
	}

	payloads := make([]interface{}, *batchSize)
	for i := 0; i < *batchSize; i++ {
		payloads[i] = &payload
	}

	batchIds := make([]string, *batchCount)
	for i := 0; i < *batchCount; i++ {
		fmt.Printf("Batch: %d\n", i)

		batchIds[i], err = client.SawtoothClient.ExecutePayloadBatch(payloads)
		if err != nil {
			handleError(err)
		}
	}

	for i, batchId := range batchIds {
		fmt.Printf("Waiting for batch %d (%s)\n", i, batchId)
		client.SawtoothClient.WaitBatch(batchId, 60, 1)
	}

	// Display the result value
	result, err := client.Show(*intName)
	if err != nil {
		handleError(err)
	}

	fmt.Printf("Result: %d\n", result)
}
