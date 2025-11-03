package main

import (
    "net/http"

    clusterConfig "github.com/canonical/lxd/lxd/cluster/config"
    "github.com/canonical/lxd/shared/logger"
)

// newCrossOriginProtection creates and configures a CrossOriginProtection
// instance using values from the cluster/global config. It returns a
// non-nil *http.CrossOriginProtection ready to be used to wrap handlers.
func newCrossOriginProtection(config *clusterConfig.Config) *http.CrossOriginProtection {
    cp := http.NewCrossOriginProtection()

    // Allow internal endpoints to bypass the check (these are internal API
    // paths used by LXD itself and shouldn't be subject to browser CSRF checks).
    cp.AddInsecureBypassPattern("/internal/")

    // If an allowed origin is configured, add it as a trusted origin so
    // requests with an Origin header matching it are permitted.
    if config != nil {
        if origin := config.HTTPSAllowedOrigin(); origin != "" {
            if err := cp.AddTrustedOrigin(origin); err != nil {
                logger.Warn("Failed to add trusted origin to CrossOriginProtection", logger.Ctx{"origin": origin, "err": err})
            }
        }
    }

    return cp
}
