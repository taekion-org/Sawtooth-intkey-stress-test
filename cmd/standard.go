package cmd

import (
	"github.com/spf13/cobra"
	"github.com/taekion-org/intkey-stress-test/impls"
	"os"
)

var standardCmd = &cobra.Command{
	Use: "standard [flags]",
	Short: "standard runs the traditional intkey-stress-test routine.",
	Run: standardRun,
}

func init() {
	standardCmd.Flags().Int("batch_size", 0, "Number of transactions per batch")
	standardCmd.Flags().Int("batch_count", 0, "Number of batches to submit")
	standardCmd.Flags().Int64("delay", 1000, "Milliseconds between submits")
	standardCmd.Flags().Bool("nodeps", false, "Disable transaction dependencies")
	standardCmd.Flags().Int("fast_retries", 0, "Number of times to quickly (~100ms) retry a batch if rejected")
	rootCmd.AddCommand(standardCmd)
}

func standardRun(cmd *cobra.Command, args []string) {
	standardTest, err := impls.NewIntkeyTestStandard(cmd.Flags())
	loggableError, ok := err.(*impls.LoggableError)
	if err != nil && ok {
		loggableError.Log()
		os.Exit(-1)
	}

	registerSigintHandler(standardTest.Stop)

	wg, err := standardTest.Run()
	if err != nil {
		loggableError := err.(*impls.LoggableError)
		loggableError.Log()
		os.Exit(-1)
	}

	wg.Wait()
}
