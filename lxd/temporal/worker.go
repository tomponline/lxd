package temporal

import (
	"context"
	"log"
	"sync"

	"go.temporal.io/sdk/client"

	"go.temporal.io/sdk/worker"

	_ "github.com/mattn/go-sqlite3"
)

const (
	LXDTaskQueue = "LXD_TASK_QUEUE"
)

func workermain(ctx context.Context, wg *sync.WaitGroup, identity string, HostPort string) {
	defer wg.Done()

	temporalServerReady.Wait()

	c, err := client.Dial(client.Options{
		Identity: identity,
		HostPort: HostPort,
		Logger:   NewTemporalLogger(),
	})
	if err != nil {
		log.Fatalf("worker failed to connect to server: %s", err)
	}
	defer c.Close()

	w := worker.New(c, LXDTaskQueue, worker.Options{})
	w.RegisterWorkflow(GreetingWorkflow)
	w.RegisterActivity(ComposeGreeting)

	if err := w.Start(); err != nil {
		log.Fatalf("failed to start worker: %s", err)
	}
	defer w.Stop()

	// Wait for a signal to exit
	<-ctx.Done()
	log.Printf("Stopping Temporal worker as context was canceled...")
}
