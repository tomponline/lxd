package ch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// Client provides a client for the cloud-hypervisor REST API.
type Client struct {
	socketPath string
	httpClient *http.Client
}

// VMMInfo represents the VMM ping response.
type VMMInfo struct {
	APIVersion string `json:"api_version"`
	Version    string `json:"version"`
	PID        int64  `json:"pid"`
}

// VMInfo represents the VM info response.
type VMInfo struct {
	Config     any    `json:"config"`
	State      string `json:"state"`
	MemorySize int64  `json:"memory_actual_size"`
}

// NewClient creates a new cloud-hypervisor API client.
func NewClient(socketPath string) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			d := net.Dialer{}
			return d.DialContext(ctx, "unix", socketPath)
		},
	}

	return &Client{
		socketPath: socketPath,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
	}
}

// Ping checks if the VMM is alive and returns VMM info.
func (c *Client) Ping(ctx context.Context) (*VMMInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/api/v1/vmm.ping", nil)
	if err != nil {
		return nil, fmt.Errorf("Failed creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var info VMMInfo
	err = json.NewDecoder(resp.Body).Decode(&info)
	if err != nil {
		return nil, fmt.Errorf("Failed decoding response: %w", err)
	}

	return &info, nil
}

// GetInfo returns information about the running VM.
func (c *Client) GetInfo(ctx context.Context) (*VMInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/api/v1/vm.info", nil)
	if err != nil {
		return nil, fmt.Errorf("Failed creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var info VMInfo
	err = json.NewDecoder(resp.Body).Decode(&info)
	if err != nil {
		return nil, fmt.Errorf("Failed decoding response: %w", err)
	}

	return &info, nil
}

// Shutdown gracefully shuts down the VM.
func (c *Client) Shutdown(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost/api/v1/vm.shutdown", nil)
	if err != nil {
		return fmt.Errorf("Failed creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Failed sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ShutdownVMM terminates the entire VMM process.
func (c *Client) ShutdownVMM(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost/api/v1/vmm.shutdown", nil)
	if err != nil {
		return fmt.Errorf("Failed creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Failed sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// IsRunning checks if the VMM is alive.
func (c *Client) IsRunning(ctx context.Context) bool {
	_, err := c.Ping(ctx)
	return err == nil
}
