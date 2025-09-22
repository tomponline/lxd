package temporal

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.temporal.io/sdk/client"

	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/state"
	"github.com/canonical/lxd/shared/cancel"
	"github.com/canonical/lxd/shared/logger"
)

const (
	SQLDbName = "db"
)

var SQLDriverName string

var temporalServerReady = cancel.New()

var daemonState func() *state.State

func Init(ctx context.Context, stateFunc func() *state.State, db *db.DB) {
	s := stateFunc()

	nodeId := int(db.Cluster.GetNodeID())
	ip, _, _ := strings.Cut(s.LocalConfig.HTTPSAddress(), ":")
	if ip == "" {
		ip = "127.0.0.1"
	}

	basePort := 5233
	clusterID := s.GlobalConfig.ClusterUUID()

	// no serious reason behind this, only for simplicity sake
	if nodeId < 1 || nodeId > 9 {
		logger.Error("Temporal node_id must be in range 1..9")
		return
	}

	logger.Warn("Initializing Temporal services", logger.Ctx{"node_id": nodeId, "ip": ip, "basePort": basePort, "uuid": clusterID})

	SQLDriverName = db.Cluster.DriverName

	// time for ugly hacks...
	daemonState = stateFunc

	go func() {
		for {
			err := servermain(ctx, ip, basePort, clusterID)
			if err != nil {
				logger.Error("Temporal server failed", logger.Ctx{"err": err})
				time.Sleep(time.Second)
				continue
			} else {
				return
			}
		}
	}()

	identity := fmt.Sprintf("node%d", nodeId)

	<-temporalServerReady.Done()

	for {
		hostAddress := fmt.Sprintf("%s:%d", ip, basePort)
		logger.Warn("Temporal client connecting to server", logger.Ctx{"identity": identity, "address": hostAddress})
		var err error
		clientPtr, err = client.Dial(client.Options{
			Identity: identity,
			HostPort: hostAddress,
			Logger:   NewTemporalLogger(logger.Log),
		})
		if err != nil {
			logger.Error("Temporal client failed to connect to server", logger.Ctx{"err": err})
			time.Sleep(time.Second)
			continue
		}

		logger.Warn("Temporal client connected to server", logger.Ctx{"identity": identity, "address": hostAddress})
		break
	}

	go func() {
		err := executeHelloWorldWorkflow(ctx, identity)
		if err != nil {
			logger.Error("Temporal workflow failed to execute", logger.Ctx{"err": err})
		}
	}()

	go func() {
		<-temporalServerReady.Done()
		for {
			err := workermain(ctx)
			if err != nil {
				logger.Error("Temporal worker failed", logger.Ctx{"err": err})
				time.Sleep(time.Second)
			} else {
				return
			}
		}
	}()
}
