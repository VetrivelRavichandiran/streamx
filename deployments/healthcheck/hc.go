// Command healthcheck is a tiny dependency-free HTTP probe used as the
// Docker HEALTHCHECK for the distroless container images (which ship no
// shell, curl, or wget).
//
// Usage: healthcheck <url>
//   - <url> is a full URL (http://host:port/path), or
//   - "env:NAME" — resolved as http://localhost:$NAME/healthz where $NAME
//     holds a port or host:port (used because the metrics port is
//     configurable via STREAMX_METRICS_ADDR).
//
// Exit code: 0 on 2xx/3xx, 1 on any other outcome, 2 on usage error.
package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck <url|env:NAME>")
		os.Exit(2)
	}
	target := os.Args[1]
	if strings.HasPrefix(target, "env:") {
		name := strings.TrimPrefix(target, "env:")
		v := os.Getenv(name)
		if v == "" {
			fmt.Fprintf(os.Stderr, "healthcheck: %s is not set\n", name)
			os.Exit(1)
		}
		// Accept "host:port", ":port" (listen-addr form), "port", or a URL.
		v = strings.TrimPrefix(v, ":")
		if !strings.Contains(v, "://") {
			if !strings.Contains(v, ".") && !strings.Contains(v, ":") {
				v = "localhost:" + v // bare port
			}
			v = "http://" + v
		}
		if !strings.Contains(v, "/") {
			v += "/healthz"
		}
		target = v
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "healthcheck: %s -> %s\n", target, resp.Status)
	os.Exit(1)
}
