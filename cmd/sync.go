package cmd

import (
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/batch_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/transaction_pb2"
	"github.com/spf13/cobra"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
	"github.com/taekion-org/sawtooth-client-sdk-go/transport/errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var syncCmd = &cobra.Command{
	Use: "sync [flags]",
	Short: "sync runs a modified intkey stress routine that synchronously waits for batches.",
	Run: syncStressTest,
}

type SyncArgs struct {
	submitSize int
	submitCount int
	maxPending int
	delay int64
	noDeps bool
	fastRetries int
}
var syncArgs SyncArgs

type SyncState struct {
	client *intkey.IntkeyClient
	batchIds map[string][]byte
	lastTransactionId string
	mutex sync.Mutex
	condLessThanMax *sync.Cond
	condNeedCheck *sync.Cond
	stopFlag bool
	totalSubmitted int
	totalCommitted int
	backoff *backoff.ExponentialBackOff
}
var syncState SyncState

const FAST_RETRY_MS = 100

func init() {
	syncCmd.Flags().IntVar(&syncArgs.submitSize, "submit_size", 0, "Number of transactions/batches per batchlist")
	syncCmd.Flags().IntVar(&syncArgs.maxPending, "max_pending", 100, "Maximum number of pending transactions/batches")
	syncCmd.Flags().Int64Var(&syncArgs.delay, "delay", 1000, "Milliseconds between submits")
	syncCmd.Flags().BoolVar(&syncArgs.noDeps, "nodeps", false, "Disable transaction dependencies")
	rootCmd.AddCommand(syncCmd)

	syncState.condLessThanMax = sync.NewCond(&syncState.mutex)
	syncState.condNeedCheck = sync.NewCond(&syncState.mutex)

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = time.Duration(syncArgs.delay) * time.Millisecond
	backoff.MaxInterval = backoff.InitialInterval * 120
	syncState.backoff = backoff
}

func syncStressTest(cmd *cobra.Command, args []string) {
	var err error

	// Check for required arguments
	if syncArgs.submitSize == 0 {
		fmt.Println("Error: you must set --submit_size")
		cmd.Flags().PrintDefaults()
		os.Exit(0)
	}

	// Initialize client
	for syncState.client == nil {
		syncState.client, err = getClient()
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
	_, err = syncState.client.Show(commonArgs.intKey)
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
			go syncFinishUp()
		}
	}()

	syncMain()
}

func syncMain() {
	syncState.mutex.Lock()

	// Create a slice to store batchId -> status mapping
	syncState.batchIds = make(map[string][]byte, syncArgs.maxPending * 2)

	// Create and submit the initial payload to set the key to 0.
	initPayload := intkey.IntkeyPayload{
		Verb:  intkey.VERB_SET,
		Name:  commonArgs.intKey,
		Value: 0,
	}
	initTransaction, err := syncState.client.SawtoothClient.CreateTransaction(&initPayload)
	if err != nil {
		handleError(err)
	}
	syncState.lastTransactionId = initTransaction.HeaderSignature

	batchIds, _, err := syncSubmitTransactions([]*transaction_pb2.Transaction{initTransaction})
	if err != nil {
		handleError(err)
	}

	statuses, err := syncState.client.SawtoothClient.Transport.GetBatchStatusMultiple(batchIds, DEFAULT_WAIT_TIME)
	if err != nil {
		handleError(err)
	}
	if statuses[batchIds[0]] != "COMMITTED" {
		handleError(fmt.Errorf("Initial transaction not committed"))
	}
	delete(syncState.batchIds, batchIds[0])

	fmt.Println("Initial transaction committed")
	syncState.mutex.Unlock()

	go syncSubmitLoop()
	go syncCheckLoop()

	for {
		if syncState.stopFlag && syncState.totalSubmitted == syncState.totalCommitted {
			fmt.Println("Done, exiting...")
			os.Exit(0)
		}
		time.Sleep(1 * time.Second)
	}
}

func syncSubmitLoop() {
	if syncState.stopFlag {
		fmt.Println("Stop signal received, submit loop stopped...")
		return
	}

	syncState.mutex.Lock()

	for len(syncState.batchIds) >= syncArgs.maxPending {
		fmt.Println("Submit: Max batches pending, sleeping...")
		syncState.condLessThanMax.Wait()
	}

	numToSubmit := syncArgs.maxPending - len(syncState.batchIds)
	if numToSubmit > syncArgs.submitSize {
		numToSubmit = syncArgs.submitSize
	}

	transactions := make([]*transaction_pb2.Transaction, 0, numToSubmit)
	for i := 0; i < numToSubmit; i++ {
		incPayload := intkey.IntkeyPayload{
			Verb:  intkey.VERB_INC,
			Name:  commonArgs.intKey,
			Value: 1,
		}
		if syncArgs.noDeps == false {
			incPayload.AddDependency(syncState.lastTransactionId)
		}

		transaction, err := syncState.client.SawtoothClient.CreateTransaction(&incPayload)
		if err != nil {
			handleError(err)
		}

		transactions = append(transactions, transaction)

		// We keep track of the last transaction id. Each transaction has a dependency on
		// the previous transaction, causing Sawtooth to to execute them in order.
		syncState.lastTransactionId = transaction.HeaderSignature
	}

	_, _, err := syncSubmitTransactions(transactions)
	if err != nil {
		handleError(err)
	}

	syncState.totalSubmitted += numToSubmit
	syncState.condNeedCheck.Signal()
	syncState.mutex.Unlock()

	go syncSubmitLoop()
}

func syncCheckLoop() {
	syncState.mutex.Lock()
	defer syncState.mutex.Unlock()

	for len(syncState.batchIds) == 0 {
		syncState.condNeedCheck.Wait()
	}

	batchIdsToCheck := make([]string, 0, len(syncState.batchIds))
	for batchId, _ := range syncState.batchIds {
		batchIdsToCheck = append(batchIdsToCheck, batchId)
	}

	syncState.mutex.Unlock()
	statuses, err := syncState.client.SawtoothClient.Transport.GetBatchStatusMultiple(batchIdsToCheck, 0)
	syncState.mutex.Lock()

	if err != nil {
		fmt.Printf("Status: Error during status check: %s...backoff for %dms...\n", err, syncArgs.delay)
		time.Sleep(time.Duration(syncArgs.delay) * time.Millisecond)
		go syncCheckLoop()
		return
	}

	batchesCommitted := 0
	for batchId, status := range statuses {
		switch status {
		case "COMMITTED":
			delete(syncState.batchIds, batchId)
			batchesCommitted++

		case "INVALID":
			fmt.Printf("Status: Batch %s is INVALID\n", batchId)
		}
	}

	syncState.totalCommitted += batchesCommitted
	fmt.Printf("Status: Submitted %d - Committed %d - Pending %d - Committed Now %d\n", syncState.totalSubmitted, syncState.totalCommitted, len(syncState.batchIds), batchesCommitted)

	if len(syncState.batchIds) < syncArgs.maxPending {
		syncState.condLessThanMax.Signal()
	}

	if syncState.stopFlag && syncState.totalCommitted == syncState.totalSubmitted {
		return
	}

	time.Sleep(time.Duration(syncArgs.delay) * time.Millisecond)
	go syncCheckLoop()
}

func syncSubmitTransactions(transactions []*transaction_pb2.Transaction) ([]string, time.Duration, error) {
	start := time.Now()

	batches := make([]*batch_pb2.Batch, 0, len(transactions))
	// Create a single batch for each transaction.
	for _, transaction := range transactions {
		batch, err := syncState.client.CreateBatch([]*transaction_pb2.Transaction{transaction})
		if err != nil {
			return nil, 0, err
		}
		batches = append(batches, batch)
	}

	batchList, err := syncState.client.CreateBatchList(batches)
	if err != nil {
		return nil, 0, err
	}

	fmt.Printf("Submit: Prepared %d batches\n", len(transactions))
	syncState.backoff.Reset()
	for err := syncState.client.SawtoothClient.Transport.SubmitBatchList(batchList); err != nil; err = syncState.client.SawtoothClient.Transport.SubmitBatchList(batchList) {
		transportError := err.(*errors.SawtoothClientTransportError)
		if transportError.ErrorCode == errors.BATCH_UNABLE_TO_ACCEPT {
			fmt.Print("Submit: Validator throttling...")
		} else {
			fmt.Printf("Submit: Error in submit: %s...", err)
		}

		backoffInterval := syncState.backoff.NextBackOff()
		fmt.Printf("backoff for %s...\n", backoffInterval)

		syncState.mutex.Unlock()
		time.Sleep(backoffInterval)
		syncState.mutex.Lock()
	}

	newBatchIds := make([]string, 0, len(batches))
	for _, batch := range batches {
		syncState.batchIds[batch.HeaderSignature] = []byte{}
		newBatchIds = append(newBatchIds, batch.HeaderSignature)
	}

	fmt.Printf("Submit: Success\n")

	return newBatchIds, time.Now().Sub(start), nil
}

func syncFinishUp() {
	syncState.mutex.Lock()
	defer syncState.mutex.Unlock()
	syncState.stopFlag = true
}
