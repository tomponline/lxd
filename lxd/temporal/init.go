package temporal

import (
	_ "github.com/mattn/go-sqlite3"

	"github.com/canonical/lxd/lxd/state"
	"github.com/canonical/lxd/shared/cancel"
)

const (
	sqlDbName      = "db"
	LXDTaskQueue   = "LXD_TASK_QUEUE"
	MutexTaskQueue = "mutex"
)

var StateFunc func() *state.State
var ServerReady = cancel.New()
