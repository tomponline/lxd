package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strings"

	"github.com/gorilla/mux"

	clusterConfig "github.com/canonical/lxd/lxd/cluster/config"
	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/metrics"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/lxd/response"
	storagePools "github.com/canonical/lxd/lxd/storage"
	"github.com/canonical/lxd/lxd/storage/s3"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/entity"
	"github.com/canonical/lxd/shared/logger"
)

// swagger:operation GET / server api_get
//
//	Get the supported API endpoints
//
//	Returns a list of supported API versions (URLs).
//
//	Internal API endpoints are not reported as those aren't versioned and
//	should only be used by LXD itself.
//
//	---
//	produces:
//	  - application/json
//	responses:
//	  "200":
//	    description: API endpoints
//	    schema:
//	      type: object
//	      description: Sync response
//	      properties:
//	        type:
//	          type: string
//	          description: Response type
//	          example: sync
//	        status:
//	          type: string
//	          description: Status description
//	          example: Success
//	        status_code:
//	          type: integer
//	          description: Status code
//	          example: 200
//	        metadata:
//	          type: array
//	          description: List of endpoints
//	          items:
//	            type: string
//	          example: ["/1.0"]
func restServer(d *Daemon) *http.Server {
	/* Setup the web server */
	mux := mux.NewRouter()
	mux.StrictSlash(false) // Don't redirect to URL with trailing slash.
	mux.SkipClean(true)
	mux.UseEncodedPath() // Allow encoded values in path segments.

	const errorMessage = `<html><title>The UI is not enabled</title><body><p>The UI is not enabled. For instructions to enable it check: <a href="https://documentation.ubuntu.com/lxd/latest/howto/access_ui/">How to access the LXD web UI</a></p></body></html>`

	uiPath := os.Getenv("LXD_UI")
	uiEnabled := uiPath != "" && shared.PathExists(uiPath)
	if uiEnabled {
		uiHTTPDir := uiHTTPDir{http.Dir(uiPath)}

		// Serve the LXD user interface.
		uiHandler := http.StripPrefix("/ui/", http.FileServer(uiHTTPDir))

		// Set security headers
		uiHandlerWithSecurity := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Disables the FLoC (Federated Learning of Cohorts) feature on the browser,
			// preventing the current page from being included in the user's FLoC calculation.
			// FLoC is a proposed replacement for third-party cookies to enable interest-based advertising.
			w.Header().Set("Permissions-Policy", "interest-cohort=()")
			// Prevents the browser from trying to guess the MIME type, which can have security implications.
			// This tells the browser to strictly follow the MIME type provided in the Content-Type header.
			w.Header().Set("X-Content-Type-Options", "nosniff")
			// Restricts the page from being displayed in a frame, iframe, or object to avoid click jacking attacks,
			// but allows it if the site is navigating to the same origin.
			w.Header().Set("X-Frame-Options", "SAMEORIGIN")
			// Sets the Content Security Policy (CSP) for the page, which helps mitigate XSS attacks and data injection attacks.
			// The policy allows loading resources (scripts, styles, images, etc.) only from the same origin ('self'), data URLs, and a restrictive list of domains.
			w.Header().Set("Content-Security-Policy", "default-src 'self' data: https://assets.ubuntu.com https://cloud-images.ubuntu.com https://images.lxd.canonical.com; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'")

			uiHandler.ServeHTTP(w, r)
		})

		mux.PathPrefix("/ui/").Handler(uiHandlerWithSecurity)
		mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
			return
		})
	} else {
		uiHandlerErrorUINotEnabled := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, err := fmt.Fprint(w, errorMessage)
			if err != nil {
				logger.Warn("Failed sending error message to client", logger.Ctx{"url": r.URL, "method": r.Method, "remote": r.RemoteAddr, "err": err})
			}
		})
		mux.PathPrefix("/ui").Handler(uiHandlerErrorUINotEnabled)
	}

	// Serving the LXD documentation.
	documentationPath := os.Getenv("LXD_DOCUMENTATION")
	docEnabled := documentationPath != "" && shared.PathExists(documentationPath)
	if docEnabled {
		documentationHTTPDir := documentationHTTPDir{http.Dir(documentationPath)}

		// Serve the LXD documentation.
		documentationHandler := http.StripPrefix("/documentation/", http.FileServer(documentationHTTPDir))

		// Set security headers
		documentationHandlerWithSecurity := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Permissions-Policy", "interest-cohort=()")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("X-Frame-Options", "SAMEORIGIN")
			w.Header().Set("X-Xss-Protection", "1; mode=block")

			documentationHandler.ServeHTTP(w, r)
		})

		mux.PathPrefix("/documentation/").Handler(documentationHandlerWithSecurity)
		mux.HandleFunc("/documentation", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/documentation/", http.StatusMovedPermanently)
			return
		})
	}

	// OIDC browser login (code flow).
	mux.HandleFunc("/oidc/login", func(w http.ResponseWriter, r *http.Request) {
		if d.oidcVerifier == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		d.oidcVerifier.Login(w, r)
	})

	mux.HandleFunc("/oidc/callback", func(w http.ResponseWriter, r *http.Request) {
		if d.oidcVerifier == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		d.oidcVerifier.Callback(w, r)
	})

	mux.HandleFunc("/oidc/logout", func(w http.ResponseWriter, r *http.Request) {
		if d.oidcVerifier == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		d.oidcVerifier.Logout(w, r)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if isBrowserClient(r) {
			http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
			return
		}

		// Normal client handling.
		_ = response.SyncResponse(true, []string{"/1.0"}).Render(w, r)
	})

	for endpoint, f := range d.gateway.HandlerFuncs(d.heartbeatHandler, d.identityCache) {
		mux.HandleFunc(endpoint, f)
	}

	for _, c := range api10 {
		// Every 1.0 endpoint should have a type for the API metrics.
		if !slices.Contains(entity.APIMetricsEntityTypes(), c.MetricsType) {
			panic(`Endpoint "/1.0/` + c.Path + `" has invalid MetricsType: ` + string(c.MetricsType))
		}

		d.createCmd(mux, "1.0", c)

		// Create any alias endpoints using the same handlers as the parent endpoint but
		// with a different path and name (so the handler can differentiate being called via
		// a different endpoint) if it wants to.
		for _, alias := range c.Aliases {
			ac := c
			ac.Name = alias.Name
			ac.Path = alias.Path
			d.createCmd(mux, "1.0", ac)
		}
	}

	for _, c := range apiInternal {
		d.createCmd(mux, "internal", c)
	}

	for _, c := range apiACME {
		d.createCmd(mux, "", c)
	}

	mux.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metrics.TrackStartedRequest(r, entity.TypeServer) // Use TypeServer for not found handler
		logger.Info("Sending top level 404", logger.Ctx{"url": r.URL, "method": r.Method, "remote": r.RemoteAddr})
		w.Header().Set("Content-Type", "application/json")
		_ = response.NotFound(nil).Render(w, r)
	})

	// Initialize API metrics with zero values.
	metrics.InitAPIMetrics()

	// Create CrossOriginProtection and wrap the top-level handler. This
	// centralizes CSRF/cross-origin protections using the stdlib implementation
	// (Go 1.25+).
	cp := newCrossOriginProtection(d.State().GlobalConfig)

	// Wrap the mux with our lxdHTTPServer which performs CORS header setting
	// and other pre-processing. Then wrap that with CrossOriginProtection.
	wrapped := &lxdHTTPServer{r: mux, d: d}

	return &http.Server{
		Handler:     cp.Handler(wrapped),
		ConnContext: request.SaveConnectionInContext,
	}
}

// isBrowserClient checks if the request is coming from a browser client.
func isBrowserClient(r *http.Request) bool {
	// Check if the User-Agent starts with "Mozilla" which is common for browsers.
	return strings.HasPrefix(r.Header.Get("User-Agent"), "Mozilla")
}

func metricsServer(d *Daemon) *http.Server {
	/* Setup the web server */
	mux := mux.NewRouter()
	mux.StrictSlash(false)
	mux.SkipClean(true)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = response.SyncResponse(true, []string{"/1.0"}).Render(w, r)
	})

	for endpoint, f := range d.gateway.HandlerFuncs(d.heartbeatHandler, d.identityCache) {
		mux.HandleFunc(endpoint, f)
	}

	d.createCmd(mux, "1.0", api10Cmd)
	d.createCmd(mux, "1.0", metricsCmd)

	mux.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metrics.TrackStartedRequest(r, entity.TypeServer) // Use TypeServer for not found handler
		logger.Info("Sending top level 404", logger.Ctx{"url": r.URL, "method": r.Method, "remote": r.RemoteAddr})
		w.Header().Set("Content-Type", "application/json")
		_ = response.NotFound(nil).Render(w, r)
	})

	return &http.Server{Handler: &lxdHTTPServer{r: mux, d: d}}
}

func storageBucketsServer(d *Daemon) *http.Server {
	/* Setup the web server */
	m := mux.NewRouter()
	m.StrictSlash(false)
	m.SkipClean(true)

	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Wait until daemon is fully started.
		<-d.waitReady.Done()

		s := d.State()

		// Check if request contains an access key, and if so try and route it to the associated bucket.
		accessKey := s3.AuthorizationHeaderAccessKey(r.Header.Get("Authorization"))
		if accessKey != "" {
			// Lookup access key to ascertain if it maps to a bucket.
			var err error
			var bucket *db.StorageBucket

