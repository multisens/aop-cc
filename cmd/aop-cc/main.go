// Command aop-cc is the Common Core component of the platform: in this
// stage, the acquisition of the transport layer signaling and the object
// proxy armed for the signaling channel.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/multisens/aop-cc/internal/acquisition"
	"github.com/multisens/aop-cc/internal/broker"
	"github.com/multisens/aop-cc/internal/config"
	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/proxy"
	"github.com/multisens/aop-cc/internal/topics"
)

const (
	clientID          = "aop-cc"
	shutdownTimeout   = 2 * time.Second
	readHeaderTimeout = 5 * time.Second
	loopbackHost      = "127.0.0.1"
)

func main() {
	settings, err := config.Load()
	if err != nil {
		slog.Error("invalid environment", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: settings.LogLevel})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	objectProxy := proxy.New(origin.New(), time.Now)
	server, err := listen(settings.ObjectProxyPort, objectProxy.Handler())
	if err != nil {
		slog.Error("object proxy not started", "error", err)
		os.Exit(1)
	}
	slog.Info("object proxy started", "port", settings.ObjectProxyPort)

	subscriptions := []broker.Subscription{{Filter: topics.EmissionFilter(), Receive: objectProxy.Receive}}
	client, err := broker.Connect(ctx, settings.MQTTURL, clientID, subscriptions)
	if err != nil {
		slog.Error("MQTT client not started", "error", err)
		os.Exit(1)
	}
	if err := client.AwaitConnection(ctx); err != nil {
		slog.Info("stopped before connecting to the MQTT broker")
		shutdown(server, client)
		return
	}
	slog.Info("acquisition started", "signaling", settings.SignalingBaseURL.String())
	proxyBase := proxyBaseURL(settings.ObjectProxyPort)
	acquisition.New(settings.SignalingBaseURL, proxyBase, client, time.Now).Run(ctx)

	shutdown(server, client)
	slog.Info("aop-cc stopped")
}

// Binds the port before returning, so that a port in use fails the
// startup; a later failure of the server stops the process.
func listen(port int, handler http.Handler) (*http.Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: readHeaderTimeout}
	go func() {
		err := server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("object proxy stopped", "error", err)
			os.Exit(1)
		}
	}()
	return server, nil
}

// The <comporta> of the object proxy of this process, on the loopback.
func proxyBaseURL(port int) *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(loopbackHost, strconv.Itoa(port)),
		Path:   "/",
	}
}

func shutdown(server *http.Server, client *broker.Client) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("object proxy shutdown failed", "error", err)
	}
	if err := client.Disconnect(shutdownCtx); err != nil {
		slog.Error("MQTT disconnect failed", "error", err)
	}
}
