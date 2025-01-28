package temporal

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/state"
	"github.com/canonical/lxd/shared/logger"
	_ "github.com/mattn/go-sqlite3"
)

const (
	SQLDbName = "db"
)

var SQLDriverName string

var temporalServerReady *flagCond

var daemonState func() *state.State

func Init(stateFunc func() *state.State, ctx context.Context, db *db.DB) {
	var wg sync.WaitGroup

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
		log.Fatalf("node_id must be in range 1..9")
	}

	logger.Warn("Initializing Temporal services", logger.Ctx{"node_id": nodeId, "ip": ip, "basePort": basePort, "uuid": clusterID})

	identity := fmt.Sprintf("node%d", nodeId)

	SQLDriverName = db.Cluster.DriverName

	temporalServerReady = NewFlagCond()

	// time for ugly hacks...
	daemonState = stateFunc

	wg.Add(3)
	go servermain(ctx, &wg, db, ip, basePort, clusterID, nodeId)
	go workermain(ctx, &wg, identity, fmt.Sprintf("%s:%d", ip, basePort))
	go clientmain(ctx, &wg, identity, fmt.Sprintf("%s:%d", ip, basePort))

	go func(wg *sync.WaitGroup) {
		logger.Warn("Waiting for Temporal shutdown...")
		wg.Wait()
		fmt.Println("Temporal shutdown.")
	}(&wg)
}
