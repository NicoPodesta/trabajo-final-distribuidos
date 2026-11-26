package monitor

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultDockerSocket  = "/var/run/docker.sock"
	defaultDockerTimeout = 10 * time.Second
)

type Restarter interface {
	Restart(ctx context.Context, containerName string) error
}

type DockerRestarter struct {
	client *http.Client
}

func NewDockerRestarter(socketPath string) *DockerRestarter {
	if socketPath == "" {
		socketPath = defaultDockerSocket
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socketPath)
		},
	}

	return &DockerRestarter{
		client: &http.Client{
			Transport: transport,
			Timeout:   defaultDockerTimeout,
		},
	}
}

func (r *DockerRestarter) Restart(ctx context.Context, containerName string) error {
	endpoint := fmt.Sprintf("http://unix/containers/%s/restart?t=1", url.PathEscape(containerName))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("docker restart %s failed with status %d: %s", containerName, resp.StatusCode, strings.TrimSpace(string(body)))
}
