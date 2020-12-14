package impls

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
)

type CommonArgs struct {
	url string
	keyFile string
	transport string
	intKey string
}

type IntkeyTestCommon struct {
	args CommonArgs
	client *intkey.IntkeyClient
	logContext *log.Entry
}

func NewIntkeyTestBase(flags *pflag.FlagSet) (*IntkeyTestCommon, error) {
	test := &IntkeyTestCommon{}

	// Initialize log context
	test.logContext = log.WithField("component", "IntkeyTestCommon")

	// Parse configuration flags
	err := test.parseFlags(flags)
	if err != nil {
		return nil, err
	}

	// Setup a client and connect to Sawtooth
	err = test.setupClient()
	if err != nil {
		return nil, err
	}

	return test, nil
}

func (self *IntkeyTestCommon) parseFlags(flags *pflag.FlagSet) error {
	logContext := self.logContext.WithField("operation", "parseFlags")

	url, err := flags.GetString("url")
	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Error parsing flags")
	}
	self.args.url = url

	keyFile, err := flags.GetString("keyfile")
	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Error parsing flags")
	}
	self.args.keyFile = keyFile

	transport, err := flags.GetString("transport")
	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Error parsing flags")
	}
	self.args.transport = transport

	intKey, err := flags.GetString("int_key")
	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Error parsing flags")
	}
	self.args.intKey = intKey

	return nil
}

func (self *IntkeyTestCommon) setupClient() error {
	var client *intkey.IntkeyClient
	var err error

	logContext := self.logContext.WithFields(log.Fields{
		"operation": "setupClient",
		"url": self.args.url,
		"transport": self.args.transport,
	})

	switch self.args.transport {
	case "rest":
		client, err = intkey.NewIntkeyClient(self.args.url, self.args.keyFile)
	case "zmq":
		client, err = intkey.NewIntkeyClientZmq(self.args.url, self.args.keyFile)
	}

	if err != nil {
		return NewLoggableError(logContext.WithError(err), "Cannot initialize client")
	}

	self.client = client
	self.logContext.Info("Successfully initialized client")

	return nil
}

func (self *IntkeyTestCommon) init() error {
	logContext := self.logContext.WithField("key", self.args.intKey)

	// Output the key being used
	logContext.Info("Key configured")

	// See if the key exists
	_, err := self.client.Show(self.args.intKey)
	if err == nil {
		return NewLoggableError(logContext, "You must use a previously unused key")
	}

	return nil
}

type LoggableError struct {
	logContext *log.Entry
	msg string
}

func NewLoggableError(logContext *log.Entry, msg string) *LoggableError {
	return &LoggableError{
		logContext: logContext,
		msg: msg,
	}
}

func (self *LoggableError) Log() {
	self.logContext.Error(self.msg)
}

func (self *LoggableError) Error() string {
	return fmt.Sprintf("LoggableError: %s", self.msg)
}
