package temporal

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	uiserver "github.com/temporalio/ui-server/v2/server"
	uiconfig "github.com/temporalio/ui-server/v2/server/config"
	uiserveroptions "github.com/temporalio/ui-server/v2/server/server_options"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	temporallog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	dqliteplugin "go.temporal.io/server/common/persistence/sql/sqlplugin/dqlite"
	dqliteschema "go.temporal.io/server/schema/sqlite"
	"go.temporal.io/server/temporal"

	"github.com/canonical/lxd/shared/logger"

	_ "github.com/mattn/go-sqlite3"
)

func ServerMain(ctx context.Context, sqlDriverName string, ip string, port int, clusterID string) error {
	metricsPath := "/metrics"
	namespace := "default"
	clusterName := "active"

	historyPort := port + 2
	matchingPort := port + 4
	workerPort := port + 5
	uiPort := port + 1000
	metricsPort := uiPort + 1000

	nsConfig, err := dqliteschema.NewNamespaceConfig(clusterName, namespace, false, map[string]enums.IndexedValueType{})
	if err != nil {
		return fmt.Errorf("Unable to create namespace config: %w", err)
	}

	const dqliteNamespace = "dqlite-default"
	ds := map[string]config.DataStore{
		dqliteNamespace: {
			SQL: &config.SQL{
				//Connect: func(sqlConfig *config.SQL) (*sqlx.DB, error) {
				//	db := sqlx.NewDb(db.Cluster.DB(), SQLDriverName)
				//
				// Maps struct names in CamelCase to snake without need for db struct tags.
				//	db.MapperFunc(strcase.ToSnake)
				//
				//	return db, nil
				//},
				PluginName: dqliteplugin.PluginName,
				ConnectAttributes: map[string]string{
					//"mode":           "memory",
					"goSqlDriverName": sqlDriverName,
					"setup":           "true",
				},
				DatabaseName: sqlDbName,
			},
		},
	}

	if err := dqliteschema.CreateNamespaces(ds[dqliteNamespace].SQL, nsConfig); err != nil {
		return fmt.Errorf("Unable to create namespace: %w", err)
	}

	logger.Warn("Temporal create namespaces")

	conf := &config.Config{
		Global: config.Global{
			Membership: config.Membership{
				MaxJoinDuration:  30 * time.Second,
				BroadcastAddress: ip, // IP that is advertised
			},
			Metrics: &metrics.Config{
				Prometheus: &metrics.PrometheusConfig{
					ListenAddress: fmt.Sprintf("%s:%d", ip, metricsPort),
					HandlerPath:   metricsPath,
				},
			},
		},
		Persistence: config.Persistence{
			DefaultStore:     dqliteNamespace,
			VisibilityStore:  dqliteNamespace,
			NumHistoryShards: 1,
			DataStores:       ds,
		},
		ClusterMetadata: &cluster.Config{
			EnableGlobalNamespace:    false,
			FailoverVersionIncrement: 10,
			MasterClusterName:        clusterName,
			CurrentClusterName:       clusterName,
			ClusterInformation: map[string]cluster.ClusterInformation{
				clusterName: {
					Enabled:                true,
					InitialFailoverVersion: int64(1),
					RPCAddress:             fmt.Sprintf("%s:%d", ip, port),
					ClusterID:              clusterID,
				},
			},
		},
		DCRedirectionPolicy: config.DCRedirectionPolicy{
			Policy: "noop",
		},
		Services: map[string]config.Service{
			"frontend": {
				RPC: config.RPC{
					GRPCPort:       port,
					BindOnIP:       ip,
					MembershipPort: port + 1,
				},
			},
			"history": {
				RPC: config.RPC{
					GRPCPort:       historyPort,
					BindOnIP:       ip,
					MembershipPort: historyPort + 1,
				},
			},
			"matching": {
				RPC: config.RPC{
					GRPCPort:       matchingPort,
					BindOnIP:       ip,
					MembershipPort: matchingPort + 1,
				},
			},
			"worker": {
				RPC: config.RPC{
					GRPCPort:       workerPort,
					BindOnIP:       ip,
					MembershipPort: workerPort + 1,
				},
			},
		},
		Archival: config.Archival{
			History: config.HistoryArchival{
				State: "disabled",
			},
			Visibility: config.VisibilityArchival{
				State: "disabled",
			},
		},
		NamespaceDefaults: config.NamespaceDefaults{
			Archival: config.ArchivalNamespaceDefaults{
				History: config.HistoryArchivalNamespaceDefaults{
					State: "disabled",
				},
				Visibility: config.VisibilityArchivalNamespaceDefaults{
					State: "disabled",
				},
			},
		},
		PublicClient: config.PublicClient{
			HostPort: fmt.Sprintf("%s:%d", ip, port),
		},
	}

	authorizer, err := authorization.GetAuthorizerFromConfig(&conf.Global.Authorization)
	if err != nil {
		return fmt.Errorf("Unable to create authorizer: %w", err)
	}

	//tlogger := temporallog.NewNoopLogger().With()
	tlogger := temporallog.NewCLILogger().With()

	claimMapper, err := authorization.GetClaimMapperFromConfig(&conf.Global.Authorization, tlogger)
	if err != nil {
		return fmt.Errorf("Unable to create claim mapper: %w", err)
	}

	dynConf := make(dynamicconfig.StaticClient)
	dynConf[dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Key()] = true

	server, err := temporal.NewServer(
		//temporal.InterruptOn(temporal.InterruptCh()),
		temporal.WithConfig(conf),
		temporal.ForServices(temporal.DefaultServices),
		//	temporal.WithStaticHosts(map[primitives.ServiceName]static.Hosts{
		//		primitives.FrontendService: static.SingleLocalHost(fmt.Sprintf("%s:%d", ip, port)),
		//		primitives.HistoryService:  static.SingleLocalHost(fmt.Sprintf("%s:%d", ip, historyPort)),
		//		primitives.MatchingService: static.SingleLocalHost(fmt.Sprintf("%s:%d", ip, matchingPort)),
		//		primitives.WorkerService:   static.SingleLocalHost(fmt.Sprintf("%s:%d", ip, workerPort)),
		//	}),
		temporal.WithLogger(tlogger),
		temporal.WithAuthorizer(authorizer),
		temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper { return claimMapper }),
		temporal.WithDynamicConfigClient(dynConf))
	if err != nil {
		return fmt.Errorf("Unable to start server: %w", err)
	}

	if err := server.Start(); err != nil {
		return fmt.Errorf("Unable to start server: %w", err)
	}
	defer server.Stop()

	ui := uiserver.NewServer(uiserveroptions.WithConfigProvider(&uiconfig.Config{
		TemporalGRPCAddress: fmt.Sprintf("%s:%d", ip, port),
		Host:                ip,
		Port:                uiPort,
		EnableUI:            true,
		CORS:                uiconfig.CORS{CookieInsecure: true},
		HideLogs:            true,
	}))

	go func() {
		if err := ui.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error("Temporal UI server error", logger.Ctx{"err": err})
		}
	}()

	logger.Warn("Temporal server started", logger.Ctx{"address": ip + ":" + strconv.Itoa(port)})
	logger.Warn("Temporal UI started", logger.Ctx{"url": "http://" + ip + ":" + strconv.Itoa(uiPort)})
	logger.Warn("Temporal metrics started", logger.Ctx{"url": "http://" + ip + ":" + strconv.Itoa(metricsPort) + "/metrics"})

	// inform worker and client goroutines that server is ready
	ServerReady.Cancel()

	// Wait for a signal to exit
	<-ctx.Done()
	logger.Warn("Stopping Temporal server as context was canceled...")

	return nil
}
