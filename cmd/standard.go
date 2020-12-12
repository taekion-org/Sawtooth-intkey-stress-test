package cmd

import (
	"fmt"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/batch_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/transaction_pb2"
	"github.com/spf13/cobra"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var standardCmd = &cobra.Command{
	Use: "standard [flags]",
	Short: "standard runs the traditional intkey-stress-test routine.",
	Run: standardStressTest,
}

type StandardArgs struct {
	batchSize int
	batchCount int
	delay int64
	noDeps bool
	fastRetries int
}
var standardArgs StandardArgs

type StandardState struct {
	client *intkey.IntkeyClient
	batchIds []string
	lastTransactionId string
	mutex sync.Mutex
}
var standardState StandardState

func init() {
	standardCmd.Flags().IntVar(&standardArgs.batchSize, "batch_size", 0, "Number of transactions per batch")
	standardCmd.Flags().IntVar(&standardArgs.batchCount, "batch_count", 0, "Number of batches to submit")
	standardCmd.Flags().Int64Var(&standardArgs.delay, "delay", 1000, "Milliseconds between submits")
	standardCmd.Flags().BoolVar(&standardArgs.noDeps, "nodeps", false, "Disable transaction dependencies")
	standardCmd.Flags().IntVar(&standardArgs.fastRetries, "fast_retries", 0, "Number of times to quickly (~100ms) retry a batch if rejected")
	rootCmd.AddCommand(standardCmd)
}

func standardStressTest(cmd *cobra.Command, args []string) {
	var err error

	// Check for required arguments
	if standardArgs.batchSize == 0 || standardArgs.batchCount == 0 {
		fmt.Println("Error: you must set --batch_size and --batch_count")
		cmd.Flags().PrintDefaults()
		os.Exit(0)
	}

	// Initialize client
	for standardState.client == nil {
		standardState.client, err = getClient()
		if err != nil {
			fmt.Printf("Cannot initialize client: %s\n", err)
			time.Sleep(time.Second * 5)
			continue
		}
		fmt.Printf("Initialized client, connected to %s via %s transport\n", commonArgs.url, commonArgs.transport)
	}

	// Output the key being used
	fmt.Printf("Using key=%s\n", commonArgs.intKey)

	// See if the key exists
	_, err = standardState.client.Show(commonArgs.intKey)
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
			go standardFinishUp()
		}
	}()

	standardMain()
}

func standardMain() {
	// Create a slice to store batchIds.
	standardState.batchIds = make([]string, 0, standardArgs.batchCount)

	// Create and submit the initial payload to set the key to 0.
	initPayload := intkey.IntkeyPayload{
		Verb:  intkey.VERB_SET,
		Name:  commonArgs.intKey,
		Value: 0,
	}
	initTransaction, err := standardState.client.SawtoothClient.CreateTransaction(&initPayload)
	if err != nil {
		handleError(err)
	}
	standardState.lastTransactionId = initTransaction.HeaderSignature
	standardSubmitTransactions([]*transaction_pb2.Transaction{initTransaction})

	// Main loop. Create and submit additional transactions.
	for i := 1; i < standardArgs.batchCount; i++ {
		transactions := make([]*transaction_pb2.Transaction, standardArgs.batchSize)
		for j := 0; j < standardArgs.batchSize; j++ {
			incPayload := intkey.IntkeyPayload{
				Verb:  intkey.VERB_INC,
				Name:  commonArgs.intKey,
				Value: 1,
			}
			if standardArgs.noDeps == false {
				incPayload.AddDependency(standardState.lastTransactionId)
			}

			transactions[j], err = standardState.client.SawtoothClient.CreateTransaction(&incPayload)
			if err != nil {
				handleError(err)
			}

			if incPayload.Name != commonArgs.intKey {
				handleError(fmt.Errorf("Key dosen't match: %s", incPayload.Name));
			}
		}
		// We save the id of the last transaction in the batch.
		// This is used to create a dependency on the previous batch, causing them to be
		// always committed in order.
		standardState.lastTransactionId = transactions[len(transactions)-1].HeaderSignature
		elapsed := standardSubmitTransactions(transactions)
		remaining := standardArgs.delay - elapsed.Milliseconds()
		fmt.Printf(".....elapsed %dms, sleeping %dms\n", elapsed.Milliseconds(), remaining)
		time.Sleep(time.Duration(remaining) * time.Millisecond)
	}

	standardFinishUp()
}

func standardSubmitTransactions(transactions []*transaction_pb2.Transaction) time.Duration {
	start := time.Now()

	standardState.mutex.Lock()
	defer standardState.mutex.Unlock()

	fmt.Printf("Batch: %d\n", len(standardState.batchIds))

	batch, err := standardState.client.CreateBatch(transactions)
	if err != nil {
		handleError(err)
	}

	batchList, err := standardState.client.CreateBatchList([]*batch_pb2.Batch{batch})
	if err != nil {
		handleError(err)
	}

	retries := 0
	for err := standardState.client.SawtoothClient.Transport.SubmitBatchList(batchList); err != nil; err = standardState.client.SawtoothClient.Transport.SubmitBatchList(batchList) {
		fmt.Printf("Error: %s\n", err)
		if retries < standardArgs.fastRetries {
			time.Sleep(100 * time.Millisecond)
			retries++
			continue
		}
		standardState.mutex.Unlock()
		retries = 0
		fmt.Printf("Backoff for %dms...\n", standardArgs.delay)
		time.Sleep(time.Duration(standardArgs.delay) * time.Millisecond)
		standardState.mutex.Lock()
	}

	standardState.batchIds = append(standardState.batchIds, batch.HeaderSignature)

	return time.Now().Sub(start)
}

func standardFinishUp() {
	standardState.mutex.Lock()
	for i, batchId := range standardState.batchIds {
		fmt.Printf("Waiting for batch %d (%s)\n", i, batchId)
		status, err := standardState.client.SawtoothClient.Transport.GetBatchStatus(batchId, DEFAULT_WAIT_TIME)
		if err != nil {
			handleError(fmt.Errorf("Error in Get Batch Status, ", err.Error()))
		}
		fmt.Printf("\t%s\n", status)
		if status != "COMMITTED" {
			handleError(fmt.Errorf("Batch not completed..."))
		}
	}

	// Display the result value
	result, err := standardState.client.Show(commonArgs.intKey)
	if err != nil {
		handleError(err)
	}

	fmt.Printf("Result: %d\n", result)
	os.Exit(0)
}
