package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"

	"github.com/canonical/lxd/shared/logger"
	_ "github.com/mattn/go-sqlite3"
)

var clientPtr client.Client

func GetClient() (client.Client, error) {
	if clientPtr == nil {
		return nil, fmt.Errorf("Temporal client wasn't initialized")
	}

	return clientPtr, nil
}

func executeHelloWorldWorkflow(ctx context.Context, identity string) error {
	c, err := GetClient()
	if err != nil {
		return fmt.Errorf("Client failed to get a temporal client: %w", err)
	}

	name := fmt.Sprintf("World (from LXD %s)", identity)
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        GreetingWorkflowID,
		TaskQueue: LXDTaskQueue,
	}, GreetingWorkflow, name)
	if err != nil {
		return err
	}

	var result string
	err = run.Get(ctx, &result)
	if err != nil {
		return err
	}

	logger.Warn("Temporal Workflow executed", logger.Ctx{"ID": run.GetID(), "RunID": run.GetRunID(), "result": result})
	return nil
}
