package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	dqliteClient "github.com/canonical/go-dqlite/v3/client"
	"github.com/canonical/go-dqlite/v3/driver"
	"github.com/gorilla/mux"
	liblxc "github.com/lxc/go-lxc"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/acme"
	"github.com/canonical/lxd/lxd/apparmor"
	"github.com/canonical/lxd/lxd/auth"
	authDrivers "github.com/canonical/lxd/lxd/auth/drivers"
	"github.com/canonical/lxd/lxd/auth/oidc"
	"github.com/canonical/lxd/lxd/bgp"
	"github.com/canonical/lxd/lxd/cluster"
	clusterConfig "github.com/canonical/lxd/lxd/cluster/config"
	"github.com/canonical/lxd/lxd/config"
	"github.com/canonical/lxd/lxd/daemon"
	"github.com/canonical/lxd/lxd/db"
	dbCluster "github.com/canonical/lxd/lxd/db/cluster"
	dbOIDC "github.com/canonical/lxd/lxd/db/oidc"
	"github.com/canonical/lxd/lxd/db/openfga"
	"github.com/canonical/lxd/lxd/db/warningtype"
	"github.com/canonical/lxd/lxd/dns"
	"github.com/canonical/lxd/lxd/endpoints"
	"github.com/canonical/lxd/lxd/events"
	"github.com/canonical/lxd/lxd/firewall"
	"github.com/canonical/lxd/lxd/fsmonitor"
	fsmonitorDrivers "github.com/canonical/lxd/lxd/fsmonitor/drivers"
	"github.com/canonical/lxd/lxd/identity"
	"github.com/canonical/lxd/lxd/idmap"
	"github.com/canonical/lxd/lxd/instance"
	instanceDrivers "github.com/canonical/lxd/lxd/instance/drivers"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/loki"
	"github.com/canonical/lxd/lxd/maas"
	"github.com/canonical/lxd/lxd/metrics"
	networkZone "github.com/canonical/lxd/lxd/network/zone"
	"github.com/canonical/lxd/lxd/node"
	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/rsync"
	"github.com/canonical/lxd/lxd/seccomp"
	"github.com/canonical/lxd/lxd/state"
	storageDrivers "github.com/canonical/lxd/lxd/storage/drivers"
	"github.com/canonical/lxd/lxd/storage/filesystem"
	"github.com/canonical/lxd/lxd/storage/s3/miniod"
	"github.com/canonical/lxd/lxd/sys"
	"github.com/canonical/lxd/lxd/task"
	"github.com/canonical/lxd/lxd/ubuntupro"
	"github.com/canonical/lxd/lxd/ucred"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/lxd/warnings"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/cancel"
	"github.com/canonical/lxd/shared/entity"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/version"
)

// Cross-origin request handling has been moved to the stdlib
// net/http CrossOriginProtection in Go 1.25+. See lxd/csrf.go for
// initialization and wiring.

// A Daemon can respond to requests from a shared client.
type Daemon struct {
	identityCache *identity.Cache
	os            *sys.OS
	db            *db.DB
	firewall      firewall.Firewall
	maas          *maas.Controller
	bgp           *bgp.Server
	dns           *dns.Server

	// Event servers
	devLXDEvents     *events.DevLXDServer
	events           *events.Server
	internalListener *events.InternalListener

	// Tasks registry for long-running background tasks
	// Keep clustering tasks separate as they cause a lot of CPU wakeups
	tasks        *task.Group
	clusterTasks *task.Group

	// Indexes of tasks that need to be reset when their execution interval changes
	taskPruneImages      *task.Task
	taskClusterHeartbeat *task.Task

	// Stores startup time of daemon
	startTime time.Time

	// Whether daemon was started by systemd socket activation.
	systemdSocketActivated bool

	config    *DaemonConfig
	endpoints *endpoints.Endpoints
	gateway   *cluster.Gateway
	seccomp   *seccomp.Server

	proxy func(req *http.Request) (*url.URL, error)

	oidcVerifier *oidc.Verifier

	// Stores last heartbeat node information to detect node changes.
	lastNodeList *cluster.APIHeartbeat

	// Serialize changes to cluster membership (joins, leaves, role
	// changes).
	clusterMembershipMutex sync.RWMutex

	serverCert    func() *shared.CertInfo
	serverCertInt *shared.CertInfo // Do not use this directly, use servertCert func.

	// Status control.
	startStopLock    sync.Mutex       // Prevent concurrent starts and stops.
	setupChan        chan struct{}    // Closed when basic Daemon setup is completed
	waitReady        cancel.Canceller // Cancelled when LXD is fully ready
	waitNetworkReady cancel.Canceller // Closed when all networks are ready.
	waitStorageReady cancel.Canceller // Closed when all storage pools are ready.
	shutdownCtx      cancel.Canceller // Cancelled when shutdown starts.
	shutdownDoneCh   chan error       // Receives the result of the d.Stop() function and tells LXD to end.

	// Device monitor for watching filesystem events
	devmonitor fsmonitor.FSMonitor

	// Keep track of skews.
	timeSkew bool

	// Configuration.
	globalConfig   *clusterConfig.Config
	localConfig    *node.Config
	globalConfigMu sync.Mutex

	// Cluster.
	serverName      string
	serverClustered bool

	lokiClient *loki.Client

	// HTTP-01 challenge provider for ACME
	http01Provider acme.HTTP01Provider

	// Authorization.
	authorizer auth.Authorizer

	// Syslog listener cancel function.
	syslogSocketCancel context.CancelFunc

	// Ubuntu Pro settings
	ubuntuPro *ubuntupro.Client

	// internalSecrets holds the current in-memory value of the secrets
	internalSecrets   dbCluster.AuthSecrets
	internalSecretsMu sync.Mutex
}

// DaemonConfig holds configuration values for Daemon.
type DaemonConfig struct {
	Group              string        // Group name the local unix socket should be chown'ed to
	Trace              []string      // List of sub-systems to trace
	RaftLatency        float64       // Coarse grain measure of the cluster latency
	DqliteSetupTimeout time.Duration // How long to wait for the cluster database to be up
}

// newDaemon returns a new Daemon object with the given configuration.
func newDaemon(config *DaemonConfig, os *sys.OS) *Daemon {
	shutdownCtx := cancel.New()

	d := &Daemon{
		identityCache:    &identity.Cache{},
		config:           config,
		tasks:            task.NewGroup(),
		clusterTasks:     task.NewGroup(),
		db:               &db.DB{},
		http01Provider:   acme.NewHTTP01Provider(),
		os:               os,
		setupChan:        make(chan struct{}),
		waitReady:        cancel.New(),
		waitNetworkReady: cancel.New(),
		waitStorageReady: cancel.New(),
		shutdownCtx:      shutdownCtx,
		shutdownDoneCh:   make(chan error),
	}

	d.serverCert = func() *shared.CertInfo { return d.serverCertInt }

	return d
}

// defaultDaemonConfig returns a DaemonConfig object with default values.
func defaultDaemonConfig() *DaemonConfig {
	return &DaemonConfig{
		RaftLatency:        3.0,
		DqliteSetupTimeout: 36 * time.Hour, // Account for snap refresh lag
	}
}

// defaultDaemon returns a new, un-initialized Daemon object with default values.
func defaultDaemon() *Daemon {
	config := defaultDaemonConfig()
	os := sys.DefaultOS()
	return newDaemon(config, os)
}

// APIEndpoint represents a URL in our API.
type APIEndpoint struct {
	Name        string             // Name for this endpoint.
	Path        string             // Path pattern for this endpoint.
	MetricsType entity.Type        // Main entity type related to this endpoint. Used by the API metrics.
	Aliases     []APIEndpointAlias // Any aliases for this endpoint.
	Get         APIEndpointAction
	Head        APIEndpointAction
	Put         APIEndpointAction
	Post        APIEndpointAction
	Delete      APIEndpointAction
	Patch       APIEndpointAction
}

// APIEndpointAlias represents an alias URL of and APIEndpoint in our API.
type APIEndpointAlias struct {
	Name string // Name for this alias.
	Path string // Path pattern for this alias.
}

// APIEndpointAction represents an action on an API endpoint.
type APIEndpointAction struct {
	Handler        func(d *Daemon, r *http.Request) response.Response
	AccessHandler  func(d *Daemon, r *http.Request) response.Response
	AllowUntrusted bool
	ContentTypes   []string // Client content types to allow.
}

// allowAuthenticated is an AccessHandler which allows only authenticated requests. This should be used in conjunction
// with further access control within the handler (e.g. to filter resources the user is able to view/edit).
func allowAuthenticated(_ *Daemon, r *http.Request) response.Response {
	requestor, err := request.GetRequestor(r.Context())
	if err != nil {
		return response.SmartError(err)
	}

	if requestor.IsTrusted() {
		return response.EmptySyncResponse
	}

	return response.Forbidden(nil)
}

// allowPermission is a wrapper to check access against a given object, an object being an image, instance, network, etc.
// Mux vars should be passed in so that the object we are checking can be created. For example, a certificate object requires
// a fingerprint, the mux var for certificate fingerprints is "fingerprint", so that string should be passed in.
// Mux vars should always be passed in with the same order they appear in the API route.
func allowPermission(entityType entity.Type, entitlement auth.Entitlement, muxVars ...string) func(d *Daemon, r *http.Request) response.Response {
	return func(d *Daemon, r *http.Request) response.Response {
		s := d.State()
		var err error
		var entityURL *api.URL
		if entityType == entity.TypeServer {
			// For server permission checks, skip mux var logic.
			entityURL = entity.ServerURL()
		} else if entityType == entity.TypeProject && len(muxVars) == 0 {
			// If we're checking project permissions on a non-project endpoint (e.g. `can_create_instances` on POST /1.0/instances)
			// we get the project name from the query parameter.
			// If we're checking project permissions on a project endpoint, we expect to get the project name from its path variable
			// in the next else block.
			entityURL = entity.ProjectURL(request.ProjectParam(r))
		} else {
			muxValues := make([]string, 0, len(muxVars))
			vars := mux.Vars(r)
			for _, muxVar := range muxVars {
				muxValue := vars[muxVar]
				if muxValue == "" {
					return response.InternalError(fmt.Errorf("Failed to perform permission check: Path argument label %q not found in request URL %q", muxVar, r.URL))
				}

				muxValues = append(muxValues, muxValue)
			}

			entityURL, err = entityType.URL(request.QueryParam(r, "project"), request.QueryParam(r, "target"), muxValues...)
			if err != nil {
				return response.InternalError(fmt.Errorf("Failed to perform permission check: %w", err))
			}
		}

		// Validate whether the user has the needed permission
		err = s.Authorizer.CheckPermission(r.Context(), entityURL, entitlement)
		if err != nil {
			return response.SmartError(err)
		}

		return response.EmptySyncResponse
	}
}

// allowProjectResourceList should be used instead of allowAuthenticated when listing resources within a project.
// This prevents a restricted TLS client from listing resources in a project that they do not have access to.
// The allowAllProjects parameter controls whether usage of the "all-projects" query parameter is allowed for restricted TLS clients.
func allowProjectResourceList(allowAllProjects bool) func(d *Daemon, r *http.Request) response.Response {
	return func(d *Daemon, r *http.Request) response.Response {
		requestor, err := request.GetRequestor(r.Context())
		if err != nil {
			return response.SmartError(err)
		}

		// The caller must be authenticated.
		if !requestor.IsTrusted() {
			return response.Forbidden(nil)
		}

		// A root user can list resources in any project.
		if requestor.IsAdmin() {
			return response.EmptySyncResponse
		}

		id := requestor.CallerIdentity()
		if id == nil {
			return response.InternalError(errors.New("No identity present in request details"))
		}

		idType := requestor.CallerIdentityType()
		if idType == nil {
			return response.InternalError(errors.New("No identity type present in request details"))
		}

		requestProjectName, allProjects, err := request.ProjectParams(r)
		if err != nil {
			return response.SmartError(err)
		}

		if idType.IsFineGrained() {
			if allProjects {
				return response.EmptySyncResponse
			}

			s := d.State()

			// Fine-grained clients must be able to view the containing project.
			err = s.Authorizer.CheckPermission(r.Context(), entity.ProjectURL(requestProjectName), auth.EntitlementCanView)
			if err != nil {
				return response.SmartError(err)
			}

			return response.EmptySyncResponse
		}

		// We should now only be left with restricted client certificates. Metrics certificates should have been disregarded
		// already, because they cannot call any endpoint other than /1.0/metrics (which is enforced during authentication).
		if idType.Name() != api.IdentityTypeCertificateClientRestricted {
			return response.InternalError(fmt.Errorf("Encountered unexpected identity type %q listing resources", idType.Name()))
		}

		// all-projects requests may not be allowed, depending on the handler.
		if allProjects {
			if allowAllProjects {
				return response.EmptySyncResponse
			}

			return response.Forbidden(errors.New("Certificate is restricted"))
		}

		// Disallow listing resources in projects the caller does not have access to.
		if !slices.Contains(id.Projects, requestProjectName) {
			return response.Forbidden(errors.New("Certificate is restricted"))
		}

		return response.EmptySyncResponse
	}
}

// reportEntitlements takes a map of entity URLs to EntitlementReporters (in practice, API types that implement the ReportEntitlements method), and
// reports the entitlements that the caller has on each entity URL to the corresponding EntitlementReporter.
func reportEntitlements(ctx context.Context, authorizer auth.Authorizer, entityType entity.Type, requestedEntitlements []auth.Entitlement, entityURLToEntitlementReporter map[*api.URL]auth.EntitlementReporter) error {
	// Nothing to do
	if len(entityURLToEntitlementReporter) == 0 {
		return nil
	}

	requestor, err := request.GetRequestor(ctx)
	if err != nil {
		return err
	}

	// No fine-grained identities are global admins. Check this first in case the caller is using e.g. the unix socket.
	if requestor.IsAdmin() {
		return api.NewStatusError(http.StatusBadRequest, "Cannot report entitlements for identities that do not use fine-grained authorization")
	}

	// Any other requestor should have an identity type present.
	identityType := requestor.CallerIdentityType()
	if identityType == nil {
		return errors.New("No identity type present in request details")
	}

	// Check the identity type is fine-grained (it could be a restricted client certificate).
	if !identityType.IsFineGrained() {
		return api.NewStatusError(http.StatusBadRequest, "Cannot report entitlements for identities that do not use fine-grained authorization")
	}

	// In the case where we have only one entity URL, we'll use the authorizer's CheckPermission method
	// whereas if we have multiple entity URLs, we'll use the authorizer's GetPermissionChecker method that
	// is more efficient for returning entitlements for a batch of entities.
	if len(entityURLToEntitlementReporter) == 1 {
		for u, r := range entityURLToEntitlementReporter {
			entitlements := make([]string, 0, len(requestedEntitlements))
			for _, entitlement := range requestedEntitlements {
				err = authorizer.CheckPermission(ctx, u, entitlement)
				if err != nil {
					if auth.IsDeniedError(err) {
						continue
					}

					return fmt.Errorf("Failed to check entitlement %q for entity URL %q: %w", entitlement, u, err)
				}

				entitlements = append(entitlements, string(entitlement))
			}

			r.ReportEntitlements(entitlements)
		}

		return nil
	}

	checkersByEntitlement := make(map[auth.Entitlement]auth.PermissionChecker)
	for _, entitlement := range requestedEntitlements {
		checker, err := authorizer.GetPermissionChecker(ctx, entitlement, entityType)
		if err != nil {
			return fmt.Errorf("Failed to get a permission checker for entitlement %q and for entity type %q: %w", entitlement, entityType, err)
		}

		checkersByEntitlement[entitlement] = checker
	}

	for u, reporter := range entityURLToEntitlementReporter {
		entitlements := make([]string, 0, len(requestedEntitlements))
		for entitlement, checker := range checkersByEntitlement {
			if checker(u) {
				entitlements = append(entitlements, string(entitlement))
			}
		}

		reporter.ReportEntitlements(entitlements)
	}

	return nil
}

// extractEntitlementsFromQuery extracts the entitlements from the query string of the request.
func extractEntitlementsFromQuery(r *http.Request, entityType entity.Type, allowRecursion bool) ([]auth.Entitlement, error) {
	rawEntitlements := request.QueryParam(r, "with-access-entitlements")
	if rawEntitlements == "" {
		return nil, nil
	}

	allowedEntitlements := auth.EntityTypeToEntitlements[entityType]
	entitlements := strings.Split(rawEntitlements, ",")
	validEntitlements := make([]auth.Entitlement, 0, len(entitlements))
	for _, e := range entitlements {
		if !slices.Contains(allowedEntitlements, auth.Entitlement(e)) {
			return nil, api.StatusErrorf(http.StatusBadRequest, "Requested entitlement %q is not valid for entity type %q", e, entityType)
		}

		validEntitlements = append(validEntitlements, auth.Entitlement(e))
	}

	// Entitlements can only be requested when recursion is enabled for a request returning multiple entities (this function call uses `allowRecursion=true`).
	// If the request is meant to return a single entity, the entitlements can be requested regardless of the recursion setting (in this case, the function is called with `allowRecursion=false`).
	if len(validEntitlements) > 0 && (!util.IsRecursionRequest(r) && allowRecursion) {
		return nil, errors.New("Entitlements can only be requested when recursion is enabled")
	}

	return validEntitlements, nil
}

// Authenticate validates an incoming http Request
// It will check over what protocol it came, what type of request it is and
// will validate the TLS certificate or OIDC token.

