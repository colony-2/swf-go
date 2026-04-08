package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var compliancePackages = []string{
	"./pkg/swf/internal/swftest/runtimeconformance",
	"./pkg/swf/internal/swftest/engineconformance",
	"./pkg/swf/internal/swftest/usageparity",
}

type config struct {
	workerPath    string
	port          int
	startupWait   time.Duration
	goTestTimeout time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := parseFlags()
	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.workerPath, "worker-path", "", "path to the Cloudflare Worker package or repo containing swf-runtime-worker")
	flag.IntVar(&cfg.port, "port", 8787, "local port for wrangler dev")
	flag.DurationVar(&cfg.startupWait, "startup-wait", 60*time.Second, "maximum time to wait for wrangler dev to become reachable")
	flag.DurationVar(&cfg.goTestTimeout, "go-test-timeout", 30*time.Minute, "timeout to pass to each go test invocation")
	flag.Parse()
	if strings.TrimSpace(cfg.workerPath) == "" {
		log.Fatal("--worker-path is required")
	}
	return cfg
}

func run(ctx context.Context, cfg config) error {
	moduleRoot, err := moduleRoot()
	if err != nil {
		return err
	}
	workerDir, err := resolveWorkerDir(cfg.workerPath)
	if err != nil {
		return err
	}
	persistDir, err := os.MkdirTemp("", "swf-runtime-compliance-*")
	if err != nil {
		return fmt.Errorf("create wrangler persistence dir: %w", err)
	}
	defer os.RemoveAll(persistDir)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", cfg.port)
	workerLogs := &bytes.Buffer{}
	workerCmd := exec.CommandContext(
		ctx,
		"pnpm",
		"exec",
		"wrangler",
		"dev",
		"--local",
		"--ip",
		"127.0.0.1",
		"--port",
		strconv.Itoa(cfg.port),
		"--persist-to",
		persistDir,
		"--show-interactive-dev-session=false",
	)
	workerCmd.Dir = workerDir
	workerCmd.Stdout = io.MultiWriter(os.Stderr, workerLogs)
	workerCmd.Stderr = io.MultiWriter(os.Stderr, workerLogs)
	workerCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := workerCmd.Start(); err != nil {
		return fmt.Errorf("start wrangler dev in %s: %w", workerDir, err)
	}
	defer stopProcessGroup(workerCmd)

	if err := waitForWorkerReady(ctx, baseURL, cfg.startupWait); err != nil {
		return fmt.Errorf("wait for wrangler dev at %s: %w\n\nworker output:\n%s", baseURL, err, workerLogs.String())
	}

	env := append(os.Environ(),
		"SWF_EXTERNAL_REMOTE_BASE_URL="+baseURL,
		"SWF_EXTERNAL_REMOTE_NAME=remote-worker",
		"NO_PROXY=localhost,127.0.0.1",
		"no_proxy=localhost,127.0.0.1",
	)
	for _, pkg := range compliancePackages {
		args := []string{"test", "-count=1", "-timeout", cfg.goTestTimeout.String(), pkg}
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = moduleRoot
		cmd.Env = env
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("run %q in %s: %w", "go "+strings.Join(args, " "), moduleRoot, err)
		}
	}
	return nil
}

func moduleRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("resolve module root: runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("resolve module root %s: %w", root, err)
	}
	return root, nil
}

func resolveWorkerDir(input string) (string, error) {
	abs, err := filepath.Abs(input)
	if err != nil {
		return "", fmt.Errorf("resolve worker path %q: %w", input, err)
	}

	candidates := []string{
		abs,
		filepath.Join(abs, "swf-runtime-worker"),
	}
	for _, candidate := range candidates {
		if stat, err := os.Stat(filepath.Join(candidate, "wrangler.jsonc")); err == nil && !stat.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("worker path %q does not contain wrangler.jsonc or swf-runtime-worker/wrangler.jsonc", abs)
}

func waitForWorkerReady(ctx context.Context, baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			Proxy: nil,
		},
	}
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/__ready__", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out after %s", timeout)
}

func stopProcessGroup(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_, _ = cmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
