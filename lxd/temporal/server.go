package temporal

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
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

	"github.com/canonical/lxd/lxd/db"
	_ "github.com/mattn/go-sqlite3"
)

func servermain(ctx context.Context, wg *sync.WaitGroup, db *db.DB, ip string, port int, clusterID string, nodeId int) {
	defer wg.Done()

	metricsPath := "/metrics"
	namespace := "default"
	clusterName := "active"

	historyPort := port + 2
	matchingPort := port + 4
	workerPort := port + 5
	uiPort := port + 1000
	metricsPort := uiPort + 1000
	//dqlitePort := port + 4

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
			log.Fatalf("UI server error: %s", err)
		}
	}()

	conf := &config.Config{
		Global: config.Global{
			Membership: config.Membership{
				MaxJoinDuration:  30 * time.Second,
				BroadcastAddress: "127.0.0.1",
			},
			Metrics: &metrics.Config{
				Prometheus: &metrics.PrometheusConfig{
					ListenAddress: fmt.Sprintf("%s:%d", ip, metricsPort),
					HandlerPath:   metricsPath,
				},
			},
		},
		Persistence: config.Persistence{
			DefaultStore:     "dqlite-default",
			VisibilityStore:  "dqlite-default",
			NumHistoryShards: 1,
			DataStores: map[string]config.DataStore{
				"dqlite-default": {
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
							"goSqlDriverName": SQLDriverName,
							"setup":           "true",
						},
						DatabaseName: SQLDbName,
					},
				},
			},
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

	nsConfig, err := dqliteschema.NewNamespaceConfig(clusterName, namespace, false, map[string]enums.IndexedValueType{})
	if err != nil {
		log.Fatalf("unable to create namespace config: %s", err)
	}

	if err := dqliteschema.CreateNamespaces(conf.Persistence.DataStores["dqlite-default"].SQL, nsConfig); err != nil {
		log.Fatalf("unable to create namespace: %s", err)
	}
	authorizer, err := authorization.GetAuthorizerFromConfig(&conf.Global.Authorization)
	if err != nil {
		log.Fatalf("unable to create authorizer: %s", err)
	}

	logger := temporallog.NewNoopLogger().With()
	//logger := temporallog.NewCLILogger().With()

	claimMapper, err := authorization.GetClaimMapperFromConfig(&conf.Global.Authorization, logger)
	if err != nil {
		log.Fatalf("unable to create claim mapper: %s", err)
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
		temporal.WithLogger(logger),
		temporal.WithAuthorizer(authorizer),
		temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper { return claimMapper }),
		temporal.WithDynamicConfigClient(dynConf))
	if err != nil {
		log.Fatalf("unable to start server: %s", err)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("unable to start server: %s", err)
	}
	defer server.Stop()

	log.Printf("%-8s %v:%v", "Server:", ip, port)
	log.Printf("%-8s http://%v:%v", "UI:", ip, uiPort)
	log.Printf("%-8s http://%v:%v/metrics", "Metrics:", ip, metricsPort)

	// inform worker and client goroutines that server is ready
	temporalServerReady.Signal()

	// Wait for a signal to exit
	<-ctx.Done()
	log.Printf("Stopping Temporal server as context was canceled...")
}
