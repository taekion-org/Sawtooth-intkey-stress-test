package impls

import (
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/batch_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/transaction_pb2"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
	"os"
	"sync"
	"time"
)

const STANDARD_DEFAULT_WAIT_TIME = 60

type StandardArgs struct {
	batchSize int
	batchCount int
	delay int64
	noDeps bool
	fastRetries int
}

type IntkeyTestStandard struct {
	*IntkeyTestCommon
	args StandardArgs

	mutex *sync.Mutex

	batchIds []string
	lastTransactionId string

	logContext *log.Entry
}

func NewIntkeyTestStandard(flags *pflag.FlagSet) (*IntkeyTestStandard, error) {
	test := &IntkeyTestStandard{}

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
	test.logContext = log.WithField("component", "IntkeyTestStandard")

	// Initialize mutex
	test.mutex = &sync.Mutex{}

	return test, nil
}

func (self *IntkeyTestStandard) parseFlags(flags *pflag.FlagSet) error {
	batchSize, err := flags.GetInt("batch_size")
	if err != nil {
		return err
	}
	self.args.batchSize = batchSize

	batchCount, err := flags.GetInt("batch_count")
	if err != nil {
		return err
	}
	self.args.batchCount = batchCount

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

	fastRetries, err := flags.GetInt("fast_retries")
	if err != nil {
		return err
	}
	self.args.fastRetries = fastRetries

	return nil
}

func (self *IntkeyTestStandard) init() error {
	// Do common initialization
	err := self.IntkeyTestCommon.init()
	if err != nil {
		return err
	}

	// Initialize slice to store batchIds.
	self.batchIds = make([]string, 0, self.args.batchCount)

	return nil
}

func (self *IntkeyTestStandard) doInitialTransaction() error {
	// Create and submit the initial payload to set the key to 0.
	initPayload := intkey.IntkeyPayload{
		Verb:  intkey.VERB_SET,
		Name:  self.IntkeyTestCommon.args.intKey,
		Value: 0,
	}
	initTransaction, err := self.client.SawtoothClient.CreateTransaction(&initPayload)
	if err != nil {
		return NewLoggableError(self.logContext.WithError(err), "Unable to create initial transaction")
	}

	self.mutex.Lock()
	self.lastTransactionId = initTransaction.HeaderSignature
	self.mutex.Unlock()

	_, err = self.submitTransactions([]*transaction_pb2.Transaction{initTransaction})
	if err != nil {
		return NewLoggableError(self.logContext.WithError(err), "Unable to submit initial transaction")

	}

	return nil
}

func (self *IntkeyTestStandard) submitTransactions(transactions []*transaction_pb2.Transaction) (time.Duration, error) {
	logContext := self.logContext.WithField("operation", "submitTransactions")

	start := time.Now()

	self.mutex.Lock()
	defer self.mutex.Unlock()

	batch, err := self.client.CreateBatch(transactions)
	if err != nil {
		return 0, err
	}

	logContext.WithFields(log.Fields{
		"batchId": batch.HeaderSignature,
		"seq": len(self.batchIds),
	}).Info("Submitting Batch")

	batchList, err := self.client.CreateBatchList([]*batch_pb2.Batch{batch})
	if err != nil {
		return 0, err
	}

	retries := 0
	for err := self.client.SawtoothClient.Transport.SubmitBatchList(batchList); err != nil; err = self.client.SawtoothClient.Transport.SubmitBatchList(batchList) {
		self.logContext.WithError(err).Errorf("Error submitting batch")
		if retries < self.args.fastRetries {
			time.Sleep(100 * time.Millisecond)
			retries++
			continue
		}
		self.mutex.Unlock()
		retries = 0
		self.logContext.Infof("Backoff for %dms...\n", self.args.delay)
		time.Sleep(time.Duration(self.args.delay) * time.Millisecond)
		self.mutex.Lock()
	}

	self.batchIds = append(self.batchIds, batch.HeaderSignature)

	return time.Now().Sub(start), nil
}

func (self *IntkeyTestStandard) mainLoop() {
	logContext := self.logContext.WithField("operation", "mainLoop")

	// Main loop. Create and submit additional transactions.
	for i := 1; i < self.args.batchCount; i++ {
		transactions := make([]*transaction_pb2.Transaction, self.args.batchSize)
		for j := 0; j < self.args.batchSize; j++ {
			incPayload := intkey.IntkeyPayload{
				Verb:  intkey.VERB_INC,
				Name:  self.IntkeyTestCommon.args.intKey,
				Value: 1,
			}
			if self.args.noDeps == false {
				incPayload.AddDependency(self.lastTransactionId)
			}

			transaction, err := self.client.SawtoothClient.CreateTransaction(&incPayload)
			transactions[j] = transaction
			if err != nil {
				logContext.WithError(err).Error("Cannot create transaction")
				return
			}

			if incPayload.Name != self.IntkeyTestCommon.args.intKey {
				logContext.Errorf("Key dosen't match: %s", incPayload.Name)
				return
			}
		}
		// We save the id of the last transaction in the batch.
		// This is used to create a dependency on the previous batch, causing them to be
		// always committed in order.
		self.mutex.Lock()
		self.lastTransactionId = transactions[len(transactions)-1].HeaderSignature
		self.mutex.Unlock()

		elapsed, err := self.submitTransactions(transactions)
		if err != nil {
			logContext.WithError(err).Error("Cannot submit transaction")
		}
		remaining := self.args.delay - elapsed.Milliseconds()
		logContext.Infof("Elapsed %dms, sleeping %dms\n", elapsed.Milliseconds(), remaining)
		time.Sleep(time.Duration(remaining) * time.Millisecond)
	}
}

func (self *IntkeyTestStandard) waitForTransactions() {
	logContext := self.logContext.WithField("operation", "waitForTransactions")

	self.mutex.Lock()
	for i, batchId := range self.batchIds {
		batchLogContext := logContext.WithFields(log.Fields{
			"batchId": batchId,
			"seq":     i,
		})

		batchLogContext.Info("Waiting for batch")
		status, err := self.client.SawtoothClient.Transport.GetBatchStatus(batchId, STANDARD_DEFAULT_WAIT_TIME)
		if err != nil {
			batchLogContext.WithError(err).Errorf("Error in Get Batch Status")
			os.Exit(-1)
		}

		batchLogContext.Info(status)
		if status != "COMMITTED" {
			batchLogContext.Errorf("Batch not completed")
			os.Exit(-1)
		}
	}

	// Display the result value
	result, err := self.client.Show(self.IntkeyTestCommon.args.intKey)
	if err != nil {
		logContext.WithError(err).Info("Could not get intkey value")
	}

	logContext.WithField("key", self.IntkeyTestCommon.args.intKey).Infof("Final intkey value: %d", result)
}
func (self *IntkeyTestStandard) Run() (*sync.WaitGroup, error) {
	err := self.init()
	if err != nil {
		return nil, err
	}

	err = self.doInitialTransaction()
	if err != nil {
		return nil, err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		self.mainLoop()
		self.Stop()
		wg.Done()
	}()

	return wg, nil
}

func (self *IntkeyTestStandard) Stop() {
	self.waitForTransactions()
	os.Exit(0)
}
