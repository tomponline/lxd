package temporal

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/shared/api"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const (
	GetInstanceStateWorkflowID = "get-instance-state-workflow"
)

func ComposeGreeting(ctx context.Context, name string) (string, error) {
	greeting := fmt.Sprintf("Hello %s!", name)
	return greeting, nil
}

func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	})
	var result string
	err := workflow.ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &result)
	return result, err
}

func GetInstanceStateActivity(ctx context.Context, projectName string, name string) (api.InstanceState, error) {
	s := StateFunc()
	c, err := instance.LoadByProjectAndName(s, projectName, name)
	if err != nil {
		return api.InstanceState{}, err
	}

	hostInterfaces, _ := net.Interfaces()
	state, err := c.RenderState(hostInterfaces)
	if err != nil {
		return api.InstanceState{}, err
	}

	return *state, nil
}

func GetInstanceStateWorkflow(ctx workflow.Context, projectName string, instanceName string) (*api.InstanceState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
	})
	var result api.InstanceState
	err := workflow.ExecuteActivity(ctx, GetInstanceStateActivity, projectName, instanceName).Get(ctx, &result)
	return &result, err
}

func GetInstanceState(ctx context.Context, c client.Client, projectName string, instanceName string) (*api.InstanceState, error) {
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        GetInstanceStateWorkflowID,
		TaskQueue: LXDTaskQueue,
	}, GetInstanceStateWorkflow, projectName, instanceName)
	if err != nil {
		return nil, fmt.Errorf("Workflow failed to complete: %w", err)
	}

	var result api.InstanceState
	err = run.Get(context.Background(), &result)
	if err != nil {
		return nil, fmt.Errorf("Failed to get workflow result: %w", err)
	}

	//log.Printf("WorkflowID: %s RunID: %s Result: %v", run.GetID(), run.GetRunID(), result)
	return &result, nil
}
