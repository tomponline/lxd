package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"

	"github.com/canonical/lxd/shared/logger"
)

const (
	GreetingWorkflowID = "greeting-workflow"
)

func ExecuteHelloWorldWorkflow(ctx context.Context, c client.Client, identity string) error {
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
