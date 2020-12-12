package cmd

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/taekion-org/sawtooth-client-sdk-go/examples/intkey"
	"os"
)

const DEFAULT_URL = "http://localhost:8008"
const DEFAULT_TRANSPORT = "rest"
const DEFAULT_WAIT_TIME = 60

var rootCmd = &cobra.Command{
	Use:   "intkey-stress-test",
	Short: "Utility for stress-testing Hyperledger Sawtooth via the intkey transaction family.",
	Run:   root,
}

type CommonArgs struct {
	url string
	keyFile string
	transport string
	intKey string
}

var commonArgs CommonArgs

func init() {
	rootCmd.Flags().StringVar(&commonArgs.url, "url", DEFAULT_URL, "Sawtooth URL")
	rootCmd.Flags().StringVar(&commonArgs.keyFile, "keyfile", "", "Sawtooth private key file")
	rootCmd.Flags().StringVar(&commonArgs.transport, "transport", DEFAULT_TRANSPORT, "Sawtooth transport ('rest' or 'zmq')")
	rootCmd.Flags().StringVar(&commonArgs.intKey, "int_key", getUUIDKey(), "Name of integer to increment")
}

func root(cmd *cobra.Command, _ []string) {
	// Otherwise, simply output help
	if err := cmd.Help(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(1)
}

func getUUIDKey() string {
	key, err := uuid.NewRandom()
	if err != nil {
		handleError(err)
	}

	return key.String()[:18]
}

func getClient() (*intkey.IntkeyClient, error) {
	var client *intkey.IntkeyClient
	var err error

	switch commonArgs.transport {
	case "rest":
		client, err = intkey.NewIntkeyClient(commonArgs.url, commonArgs.keyFile)
	case "zmq":
		client, err = intkey.NewIntkeyClientZmq(commonArgs.url, commonArgs.keyFile)
	}

	if err != nil {
		return nil, err
	}

	return client, nil
}

func handleError(err error) {
	fmt.Printf("%s error:\n - %s\n", rootCmd.Use, err)
	os.Exit(-1)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		handleError(err)
	}
}
