package temporal

import (
	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"go.temporal.io/sdk/worker"

	"github.com/canonical/lxd/shared/logger"
)

const (
	LXDTaskQueue = "LXD_TASK_QUEUE"
)

func workermain(ctx context.Context) error {
	logger.Warn("Starting Temporal worker")

	c, err := GetClient()
	if err != nil {
		return fmt.Errorf("Client failed to get a temporal client: %w", err)
	}

	w := worker.New(c, LXDTaskQueue, worker.Options{})
	w.RegisterWorkflow(GreetingWorkflow)
	w.RegisterActivity(ComposeGreeting)

	w.RegisterWorkflow(GetInstanceStateWorkflow)
	w.RegisterActivity(GetInstanceStateActivity)

	w.RegisterWorkflow(ExtendProjectStorageSchemaWorkflow)

	if err := w.Start(); err != nil {
		return fmt.Errorf("Failed to start worker: %w", err)
	}
	defer w.Stop()

	logger.Warn("Temporal worker started")

	// Wait for a signal to exit
	<-ctx.Done()
	logger.Warn("Stopping Temporal worker as context was canceled...")

	return nil
}
