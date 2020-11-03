package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/batch_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/transaction_pb2"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
	flag "github.com/spf13/pflag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const DEFAULT_URL = "http://localhost:8008"
const DEFAULT_TRANSPORT = "rest"
const DEFAULT_WAIT_TIME = 60

var client *intkey.IntkeyClient
var batchIds []string
var lastTransactionId string
var mutex = sync.Mutex{}

var url *string = flag.String("url", DEFAULT_URL, "Sawtooth URL")
var keyFile *string = flag.String("keyfile", "", "Sawtooth private key file")
var transport *string = flag.String("transport", DEFAULT_TRANSPORT, "Sawtooth transport ('rest' or 'zmq')")
var intKey *string = flag.String("int_key", getUUIDKey(), "Name of integer to increment")
var batchSize *int = flag.Int("batch_size", 0, "Number of transactions per batch")
var batchCount *int = flag.Int("batch_count", 0, "Number of batches to submit")
var delay *int64 = flag.Int64("delay", 1000, "Milliseconds between submits")
var noDeps *bool = flag.Bool("nodeps", false, "Disable transaction dependencies")
var fastRetries *int = flag.Int("fast_retries", 0, "Number of times to quickly (~100ms) retry a batch if rejected")

func handleError(err error) {
	fmt.Println(err)
	os.Exit(-1)
}

func getUUIDKey() string {
	key, err := uuid.NewRandom()
	if err != nil {
		handleError(err)
	}

	return key.String()[:18]
}

func main() {
	var err error

	flag.Parse()

	if *batchSize == 0 || *batchCount == 0 {
		fmt.Println("Error: you must set --batch_size and --batch_count")
		flag.PrintDefaults()
		os.Exit(0)
	}

	// Initialize the client (uses a copy of the example intkey client from sawtooth-client-sdk-go).
	for client == nil {
		if *transport == "rest" {
			client, err = intkey.NewIntkeyClient(*url, *keyFile)
			if err != nil {
				fmt.Printf("Error: %s\n", err)
				time.Sleep(1 * time.Second)
			}
		} else if *transport == "zmq" {
			client, err = intkey.NewIntkeyClientZmq(*url, *keyFile)
			if err != nil {
				fmt.Printf("Error: %s\n", err)
				time.Sleep(1 * time.Second)
			}
		}
	}

	// Output the key being used
	fmt.Printf("Using key=%s\n", *intKey)

	// See if the key exists
	_, err = client.Show(*intKey)
	if err == nil {
		handleError(fmt.Errorf("You must specify an unused integer key"))
	}

	// Set up a signal handler for SIGINT.
	// On the first signal, start waiting for transactions.
	// On any subsequent signal, kill the program immediately.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
	sigCount := 0
	go func() {
		for {
			sig := <-sigs
			fmt.Println(sig)
			sigCount++
			if sigCount > 1 {
				os.Exit(-1)
			}
			go finishUp()
		}
	}()

	// Create a slice to store batchIds.
	batchIds = make([]string, 0, *batchCount)

	// Create and submit the initial payload to set the key to 0.
	initPayload := intkey.IntkeyPayload{
		Verb:  intkey.VERB_SET,
		Name:  *intKey,
		Value: 0,
	}
	initTransaction, err := client.SawtoothClient.CreateTransaction(&initPayload)
	if err != nil {
		handleError(err)
	}
	lastTransactionId = initTransaction.HeaderSignature
	submitTransactions([]*transaction_pb2.Transaction{initTransaction})

	// Main loop. Create and submit additional transactions.
	for i := 1; i < *batchCount; i++ {
		transactions := make([]*transaction_pb2.Transaction, *batchSize)
		for j := 0; j < *batchSize; j++ {
			incPayload := intkey.IntkeyPayload{
				Verb:  intkey.VERB_INC,
				Name:  *intKey,
				Value: 1,
			}
			if *noDeps == false {
				incPayload.AddDependency(lastTransactionId)
			}

			transactions[j], err = client.SawtoothClient.CreateTransaction(&incPayload)
			if err != nil {
				handleError(err)
			}

			if incPayload.Name != *intKey {
				handleError(fmt.Errorf("Key dosen't match: %s", incPayload.Name));
			}
		}
		// We save the id of the last transaction in the batch.
		// This is used to create a dependency on the previous batch, causing them to be
		// always committed in order.
		lastTransactionId = transactions[len(transactions)-1].HeaderSignature
		elapsed := submitTransactions(transactions)
		remaining := *delay - elapsed.Milliseconds()
		fmt.Printf(".....elapsed %dms, sleeping %dms\n", elapsed.Milliseconds(), remaining)
		time.Sleep(time.Duration(remaining) * time.Millisecond)
	}

	finishUp()
}

func submitTransactions(transactions []*transaction_pb2.Transaction) time.Duration {
	start := time.Now()

	mutex.Lock()
	defer mutex.Unlock()

	fmt.Printf("Batch: %d\n", len(batchIds))

	batch, err := client.CreateBatch(transactions)
	if err != nil {
		handleError(err)
	}

	batchList, err := client.CreateBatchList([]*batch_pb2.Batch{batch})
	if err != nil {
		handleError(err)
	}

	retries := 0
	for err := client.SawtoothClient.Transport.SubmitBatchList(batchList); err != nil; err = client.SawtoothClient.Transport.SubmitBatchList(batchList) {
		fmt.Printf("Error: %s\n", err)
		if retries < *fastRetries {
			time.Sleep(100 * time.Millisecond)
			retries++
			continue
		}
		mutex.Unlock()
		retries = 0
		fmt.Printf("Backoff for %dms...\n", *delay)
		time.Sleep(time.Duration(*delay) * time.Millisecond)
		mutex.Lock()
	}

	batchIds = append(batchIds, batch.HeaderSignature)

	return time.Now().Sub(start)
}

func finishUp() {
	mutex.Lock()
	for i, batchId := range batchIds {
		fmt.Printf("Waiting for batch %d (%s)\n", i, batchId)
		status, err := client.SawtoothClient.Transport.GetBatchStatus(batchId, DEFAULT_WAIT_TIME)
		if err != nil {
			handleError(fmt.Errorf("Error in Get Batch Status, ", err.Error()))
		}
		fmt.Printf("\t%s\n", status)
		if status != "COMMITTED" {
			handleError(fmt.Errorf("Batch not completed..."))
		}
	}

	// Display the result value
	result, err := client.Show(*intKey)
	if err != nil {
		handleError(err)
	}

	fmt.Printf("Result: %d\n", result)
	os.Exit(0)
}
