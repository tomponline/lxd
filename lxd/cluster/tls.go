package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/canonical/lxd/lxd/identity"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
)

// Return a TLS configuration suitable for establishing intra-member network connections using the server cert.
func tlsClientConfig(networkCert *shared.CertInfo, serverCert *shared.CertInfo) (*tls.Config, error) {
	if networkCert == nil {
		return nil, errors.New("Invalid networkCert")
	}

	if serverCert == nil {
		return nil, errors.New("Invalid serverCert")
	}

	keypair := serverCert.KeyPair()
	config := shared.InitTLSConfig()
	config.Certificates = []tls.Certificate{keypair}
	config.RootCAs = x509.NewCertPool()
	ca := serverCert.CA()
	if ca != nil {
		config.RootCAs.AddCert(ca)
	}

	// Since the same cluster keypair is used both as server and as client
	// cert, let's add it to the CA pool to make it trusted.
	networkKeypair := networkCert.KeyPair()
	netCert, err := x509.ParseCertificate(networkKeypair.Certificate[0])
	if err != nil {
		return nil, err
	}

	netCert.IsCA = true
	netCert.KeyUsage = x509.KeyUsageCertSign
	config.RootCAs.AddCert(netCert)

	// Always use network certificate's DNS name rather than server cert, so that it matches.
	if len(netCert.DNSNames) > 0 {
		config.ServerName = netCert.DNSNames[0]
	}

	return config, nil
}

// tlsCheckCert checks certificate access, returns true if certificate is trusted.
func tlsCheckCert(r *http.Request, networkCert *shared.CertInfo, serverCert *shared.CertInfo, cache *identity.Cache) bool {
	_, err := x509.ParseCertificate(networkCert.KeyPair().Certificate[0])
	if err != nil {
		// Since we have already loaded this certificate, typically
		// using LoadX509KeyPair, an error should never happen, but
		// check for good measure.
		panic(fmt.Sprintf("Invalid keypair material: %v", err))
	}

	if r.TLS == nil {
		return false
	}

	trustedServerCert, err := x509.ParseCertificate(serverCert.KeyPair().Certificate[0])
	if err != nil {
		return false
	}

	trustedServerCerts := map[string]x509.Certificate{serverCert.Fingerprint(): *trustedServerCert}
	cachedIdentityTypeCertificateServer := cache.X509Certificates(api.IdentityTypeCertificateServer)

	for _, i := range r.TLS.PeerCertificates {
		// Trust our own server certificate. This allows Dqlite to start with a connection back to this
		// member before the database is available. It also allows us to switch the server certificate to
		// the network certificate during cluster upgrade to per-server certificates, and it be trusted.
		trusted, _ := util.CheckMutualTLS(*i, trustedServerCerts)
		if trusted {
			return true
		}

		// Check the trusted server certficates list provided.
		trusted, _ = util.CheckMutualTLS(*i, cachedIdentityTypeCertificateServer)
		if trusted {
			return true
		}

		logger.Errorf("Invalid client certificate %v (%v) from %v", i.Subject, shared.CertFingerprint(i), r.RemoteAddr)
	}

	return false
}

// Return an http.Transport configured using the given configuration and a
// cleanup function to use to close all connections the transport has been
// used.
func tlsTransport(config *tls.Config) (*http.Transport, func()) {
	transport := &http.Transport{
		TLSClientConfig:       config,
		DisableKeepAlives:     true,
		MaxIdleConns:          0,
		ExpectContinueTimeout: time.Second * 30,
		ResponseHeaderTimeout: time.Second * 3600,
		TLSHandshakeTimeout:   time.Second * 5,
	}

	return transport, transport.CloseIdleConnections
}
