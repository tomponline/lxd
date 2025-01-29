package temporal

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.temporal.io/sdk/client"

	_ "github.com/mattn/go-sqlite3"
)

var clientPtr *client.Client

func clientmain(ctx context.Context, wg *sync.WaitGroup, identity string, HostPort string) {
	defer wg.Done()

	temporalServerReady.Wait()

	c, err := client.Dial(client.Options{
		Identity: identity,
		HostPort: HostPort,
		Logger:   NewTemporalLogger(),
	})
	if err != nil {
		log.Fatalf("client failed to connect to server: %s", err)
	}
	defer c.Close()

	clientPtr = &c
	executeHelloWorldWorkflow(identity)

	// Wait for a signal to exit
	<-ctx.Done()
}

func GetClient() (client.Client, error) {
	if clientPtr == nil {
		return nil, fmt.Errorf("Temporal client wasn't initialized")
	}

	return *clientPtr, nil
}

func executeHelloWorldWorkflow(identity string) {
	c, err := GetClient()
	if err != nil {
		log.Fatalf("client failed to get a temporal client: %s", err)
	}

	name := fmt.Sprintf("World (from LXD %s)", identity)
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        GreetingWorkflowID,
		TaskQueue: LXDTaskQueue,
	}, GreetingWorkflow, name)
	if err != nil {
		log.Fatalf("workflow failed to complete: %s", err)
	}

	var result string
	err = run.Get(context.Background(), &result)
	if err != nil {
		log.Fatalln("failed to get workflow result", err)
	}

	log.Printf("WorkflowID: %s RunID: %s Result: %s", run.GetID(), run.GetRunID(), result)
}
