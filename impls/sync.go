package impls

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/batch_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/transaction_pb2"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
	"github.com/taekion-org/sawtooth-client-sdk-go/transport/errors"
	"github.com/taekion-org/sawtooth-client-sdk-go/transport/types"
	"fmt"
	"sync"
	"time"
)

const SYNC_INITIAL_WAIT_SECONDS = 600

type SyncArgs struct {
	maxSubmit int
	maxPending int
	delay int64
	noDeps bool
}

type IntkeyTestSync struct {
	*IntkeyTestCommon
	args SyncArgs

	mutex *sync.Mutex
	condNeedSubmit *sync.Cond
	condNeedCheck *sync.Cond
	stopFlag bool

	batchIds map[string][]byte
	lastTransactionId string

	totalSubmit int
	totalCommit int

	backoffSubmit *backoff.ExponentialBackOff
	backoffCheck *backoff.ExponentialBackOff

	logContext *log.Entry
}

func NewIntkeyTestSync(flags *pflag.FlagSet) (*IntkeyTestSync, error) {
	test := &IntkeyTestSync{}

	// Initialize common code
	common, err := NewIntkeyTestBase(flags)
	if err != nil {
		return nil, err
	}
	test.IntkeyTestCommon = common

	// Parse our own configuration flags
	err = test.parseFlags(flags)
	if err != nil {
		return nil, err
	}

	// Initialize log context
	test.logContext = log.WithField("component", "IntkeyTestSync")

	// Initialize synchronization primitives
	test.mutex = &sync.Mutex{}
	test.condNeedSubmit = sync.NewCond(test.mutex)
	test.condNeedCheck = sync.NewCond(test.mutex)

	// Initialize exponential backoff trackers
	initialInterval := time.Duration(test.args.delay) * time.Millisecond
	maxInterval := initialInterval * 120

	backoffSubmit := backoff.NewExponentialBackOff()
	backoffSubmit.InitialInterval = initialInterval
	backoffSubmit.MaxInterval = maxInterval
	test.backoffSubmit = backoffSubmit

	backoffCheck := backoff.NewExponentialBackOff()
	backoffCheck.InitialInterval = initialInterval
	backoffCheck.MaxInterval = maxInterval
	test.backoffCheck = backoffCheck

	return test, nil
}

func (self *IntkeyTestSync) parseFlags(flags *pflag.FlagSet) error {
	maxSubmit, err := flags.GetInt("max_submit")
	if err != nil {
		return err
	}
	self.args.maxSubmit = maxSubmit

	maxPending, err := flags.GetInt("max_pending")
	if err != nil {
		return err
	}
	self.args.maxPending = maxPending

	delay, err := flags.GetInt64("delay")
	if err != nil {
		return err
	}
	self.args.delay = delay

	noDeps, err := flags.GetBool("nodeps")
	if err != nil {
		return err
	}
	self.args.noDeps = noDeps

	return nil
}

func (self *IntkeyTestSync) init() error {
	// Do common initialization
	err := self.IntkeyTestCommon.init()
	if err != nil {
		return err
	}

	self.batchIds = make(map[string][]byte, self.args.maxPending * 2)
	self.totalSubmit = 0
	self.totalCommit = 0

	return nil
}

func (self *IntkeyTestSync) doInitialTransaction() error {
	logContext := self.logContext.WithField("operation", "doInitialTransaction")

	self.mutex.Lock()

	// Create and submit the initial payload to set the key to 0.
	initPayload := intkey.IntkeyPayload{
		Verb:  intkey.VERB_SET,
		Name:  self.IntkeyTestCommon.args.intKey,
		Value: 0,
	}

	initTransaction, err := self.client.SawtoothClient.CreateTransaction(&initPayload)
	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Unable to create initial transaction")
	}

	self.lastTransactionId = initTransaction.HeaderSignature

	batchIds, err := self.submitTransactions([]*transaction_pb2.Transaction{initTransaction})
	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Unable to submit initial transaction")
	}

	statuses, err := self.client.SawtoothClient.Transport.GetBatchStatusMultiple(batchIds, SYNC_INITIAL_WAIT_SECONDS)
	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Unable to get initial transaction status")
	}
	if statuses[batchIds[0]] != "COMMITTED" {
		return NewLoggableError(logContext.WithError(err), "Initial transaction not committed")
	}
	delete(self.batchIds, batchIds[0])

	logContext.Info("Initial transaction committed")

	self.mutex.Unlock()

	return nil
}

func (self *IntkeyTestSync) submitTransactions(transactions []*transaction_pb2.Transaction) ([]string, error) {
	logContext := self.logContext.WithField("operation", "submitTransactions")

	// Create a single batch for each transaction.
	batches := make([]*batch_pb2.Batch, 0, len(transactions))
	for _, transaction := range transactions {
		batch, err := self.client.CreateBatch([]*transaction_pb2.Transaction{transaction})
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}

	// Create a batchlist containing all of the batches.
	batchList, err := self.client.CreateBatchList(batches)
	if err != nil {
		return nil, err
	}

	// Submit the batchlist to the blockchain.
	// Retry if unsuccessful, at intervals corresponding to an exponential backoff.
	logContext.Infof("Prepared %d batch(es)", len(transactions))

	// Reset the exponential backoff tracker.
	self.backoffSubmit.Reset()

	// Create a closure to submit the batchlist (for loop will retry this).
	submitFunc := func () error {
		return self.client.SawtoothClient.Transport.SubmitBatchList(batchList)
	};

	// Loop over trying to submit the batchlist.
	for err := submitFunc(); err != nil; err = submitFunc() {
		// Cast the error to a SawtoothClientTransportError.
		// This will let us check the code.
		transportError := err.(*errors.SawtoothClientTransportError)

		// Get the next backoff interval.
		backoffInterval := self.backoffSubmit.NextBackOff()

		// Check the type of the error and log it accordingly.
		if transportError.ErrorCode == errors.BATCH_UNABLE_TO_ACCEPT {
			logContext.WithField("backoff", backoffInterval).Info("Validator is throttling")
		} else {
			logContext.WithField("backoff", backoffInterval).WithError(err).Error("Error while submitting")
		}

		// If a stop is requested, return a special error to make sure we don't get stuck in this loop.
		if self.stopFlag {
			return nil, fmt.Errorf("Stop requested")
		}

		// Drop the mutex to sleep for the backoff interval.
		self.mutex.Unlock()
		time.Sleep(backoffInterval)
		self.mutex.Lock()
	}

	// Build a list of the new batch ids and simultaneously add them to the batchId map.
	newBatchIds := make([]string, 0, len(batches))
	for _, batch := range batches {
		self.batchIds[batch.HeaderSignature] = []byte{}
		newBatchIds = append(newBatchIds, batch.HeaderSignature)
	}

	// Log a success message.
	logContext.Infof("Sucessfully submitted %d batch(es)", len(newBatchIds))

	return newBatchIds, nil
}

func (self *IntkeyTestSync) submitLoop() bool {
	logContext := self.logContext.WithField("operation", "submitLoop")

	if self.stopFlag {
		logContext.Info("Stop signal received, submit loop stopped")
		return false
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	for len(self.batchIds) >= self.args.maxPending {
		logContext.Info("Max batches pending, sleeping...")
		self.condNeedSubmit.Wait()
	}

	numToSubmit := self.args.maxPending - len(self.batchIds)
	if numToSubmit > self.args.maxSubmit {
		numToSubmit = self.args.maxSubmit
	}

	transactions := make([]*transaction_pb2.Transaction, 0, numToSubmit)
	for i := 0; i < numToSubmit; i++ {
		incPayload := intkey.IntkeyPayload{
			Verb:  intkey.VERB_INC,
			Name:  self.IntkeyTestCommon.args.intKey,
			Value: 1,
		}
		if self.args.noDeps == false {
			incPayload.AddDependency(self.lastTransactionId)
		}

		transaction, err := self.client.SawtoothClient.CreateTransaction(&incPayload)
		if err != nil {
			logContext.WithError(err).Error("Cannot create transaction")
			return true
		}
		transactions = append(transactions, transaction)

		// We keep track of the last transaction id. Each transaction has a dependency on
		// the previous transaction, causing Sawtooth to to execute them in order.
		self.lastTransactionId = transaction.HeaderSignature
	}

	_, err := self.submitTransactions(transactions)
	if err != nil {
		logContext.WithError(err).Error("Cannot submit transaction")
		return true
	}

	self.totalSubmit += numToSubmit
	self.condNeedCheck.Signal()

	return true
}

func (self *IntkeyTestSync) checkLoop() bool {
	logContext := self.logContext.WithField("operation", "checkLoop")

	self.mutex.Lock()
	defer self.mutex.Unlock()

	for len(self.batchIds) == 0 {
		self.condNeedCheck.Wait()
	}

	batchIdsToCheck := make([]string, 0, len(self.batchIds))
	for batchId, _ := range self.batchIds {
		batchIdsToCheck = append(batchIdsToCheck, batchId)
	}

	var statuses map[string]types.BatchStatus
	var err error

	self.backoffCheck.Reset()
	for {
		self.mutex.Unlock()
		statuses, err = self.client.SawtoothClient.Transport.GetBatchStatusMultiple(batchIdsToCheck, 0)
		self.mutex.Lock()

		if err != nil {
			backoffInterval := self.backoffCheck.NextBackOff()
			logContext.WithError(err).Errorf("Error during status check, backoff for %s", backoffInterval)
			time.Sleep(backoffInterval)
			continue
		}

		break
	}

	batchesCommitted := 0
	for batchId, status := range statuses {
		switch status {
		case "COMMITTED":
			delete(self.batchIds, batchId)
			batchesCommitted++

		case "INVALID":
			logContext.Errorf("Batch %s is INVALID", batchId)
		}
	}

	self.totalCommit += batchesCommitted
	logContext.WithFields(log.Fields{
		"submitted": self.totalSubmit,
		"committed": self.totalCommit,
		"pending": len(self.batchIds),
	}).Infof("Committed %d batches", batchesCommitted)

	if len(self.batchIds) < self.args.maxPending {
		self.condNeedSubmit.Signal()
	}

	if self.stopFlag && self.totalCommit == self.totalSubmit {
		return false
	}

	time.Sleep(time.Duration(self.args.delay) * time.Millisecond)

	return true
}


func (self *IntkeyTestSync) Run() (*sync.WaitGroup, error) {
	err := self.init()
	if err != nil {
		return nil, err
	}

	err = self.doInitialTransaction()
	if err != nil {
		return nil, err
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for {
			if !self.submitLoop() {
				wg.Done()
				return
			}
		}
	}()

	go func() {
		for {
			if !self.checkLoop(){
				wg.Done()
				return
			}
		}
	}()

	return wg, nil
}

func (self *IntkeyTestSync) Stop() {
	self.mutex.Lock()
	self.logContext.Info("Signal received, waiting for pending transactions")
	self.stopFlag = true
	self.mutex.Unlock()
}
