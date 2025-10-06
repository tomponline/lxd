package temporal

import (
	"context"
	"fmt"
	"time"

	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/shared/api"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ExtendProjectStorageSchemaWorkflowID2 = "extend-project-storage-schema-per-node-workflow"
)

func ExtendProjectStorageSchemaOnThisNodeActivity(ctx context.Context, project api.ProjectsPost) error {
	// fmt.Println("Extend project storage schema on node", localClusterAddress)
	ExtendLocalConfigSchemaForProject(project.Name)
	return nil
}

func CompensateExtendProjectStorageSchemaOnThisNodeActivity(ctx context.Context, peer db.NodeInfo, project api.ProjectsPost) error {
	// fmt.Println("Delete project storage schema on node", localClusterAddress)
	DeleteLocalConfigSchemaForProject(project.Name)
	return nil
}

func ExtendProjectStorageSchemaWorkflowPerNode(ctx workflow.Context, project api.ProjectsPost) (err error) {
	s := StateFunc()
	localClusterAddress = s.LocalConfig.ClusterAddress()

	// fmt.Println("ExtendProjectStorageSchemaWorkflowPerNode executed")

	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	// Get list of peers, no need to compensate if this fails.
	var peers []db.NodeInfo
	err = workflow.ExecuteLocalActivity(ctx, GetClusterNodesActivity).Get(ctx, &peers)
	if err != nil {
		return err
	}

	// var compensations Compensations
	compensations := make(Compensations, 0, len(peers)+1)

	defer func() {
		if err != nil {
			// activity failed, and workflow context is canceled
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			compensations.Compensate(disconnectedCtx, true)
		}
	}()

	// Notify all peers in parallel.
	selector := workflow.NewSelector(ctx)
	localSchemaExtensionFailed := false
	for _, peer := range peers {
		compensations.AddCompensation(func(ctx context.Context) error {
			return CompensateExtendProjectStorageSchemaOnThisNodeActivity(ctx, peer, project)
		})

		// Execute the activity in target peer task queue
		peerctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
			TaskQueue:           LXDTaskQueue + peer.Name,
		})

		execution := workflow.ExecuteActivity(peerctx, ExtendProjectStorageSchemaOnThisNodeActivity, project)
		selector.AddFuture(execution, func(f workflow.Future) {
			err := f.Get(ctx, nil)
			if err != nil {
				fmt.Printf("ExtendProjectStorageSchemaActivity failed for peer %q: %v\n", peer.Name, err)
				localSchemaExtensionFailed = true
			}
		})
	}

	for range peers {
		selector.Select(ctx)
	}

	if localSchemaExtensionFailed {
		return fmt.Errorf("Failed to extend project schema on some cluster members")
	}

	err = workflow.ExecuteLocalActivity(ctx, CreateProjectInDBActivity, project).Get(ctx, nil)
	return err
}

func CreateProjectWithTemporalPerNode(serverName string, c client.Client, project api.ProjectsPost) error {
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        ExtendProjectStorageSchemaWorkflowID2,
		TaskQueue: LXDTaskQueue + serverName,
	}, ExtendProjectStorageSchemaWorkflowPerNode, project)
	if err != nil {
		return fmt.Errorf("Workflow failed to start: %w", err)
	}

	err = run.Get(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("Failed to get workflow result: %w", err)
	}

	return nil
}
