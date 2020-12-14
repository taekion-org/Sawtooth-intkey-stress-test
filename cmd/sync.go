package cmd

import (
	"github.com/spf13/cobra"
	"github.com/taekion-org/intkey-stress-test/impls"
	"os"
)

var syncCmd = &cobra.Command{
	Use: "sync [flags]",
	Short: "sync runs a modified intkey stress routine that synchronously waits for batches.",
	Run: syncRun,
}

func init() {
	syncCmd.Flags().Int("max_submit", 10, "Number of transactions/batches per batchlist")
	syncCmd.Flags().Int("max_pending", 100, "Maximum number of pending transactions/batches")
	syncCmd.Flags().Int64("delay", 1000, "Milliseconds between submits")
	syncCmd.Flags().Bool("nodeps", false, "Disable transaction dependencies")
	rootCmd.AddCommand(syncCmd)
}

func syncRun(cmd *cobra.Command, args []string) {
	syncTest, err := impls.NewIntkeyTestSync(cmd.Flags())
	loggableError, ok := err.(*impls.LoggableError)
	if err != nil && ok {
		loggableError.Log()
		os.Exit(-1)
	}

	registerSigintHandler(syncTest.Stop)

	wg, err := syncTest.Run()
	if err != nil {
		loggableError := err.(*impls.LoggableError)
		loggableError.Log()
		os.Exit(-1)
	}

	wg.Wait()
}
