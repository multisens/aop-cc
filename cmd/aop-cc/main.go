// Command aop-cc is the Common Core component of the platform: the
// acquisition of the transport layer signaling, the object proxy, the
// Application Context Cache and the application lifecycle manager.
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
	"github.com/multisens/aop-cc/internal/cache"
	"github.com/multisens/aop-cc/internal/config"
	"github.com/multisens/aop-cc/internal/lifecycle"
	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/proxy"
	"github.com/multisens/aop-cc/internal/storage"
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

	client := broker.New(settings.MQTTURL, clientID)
	store := storage.New(settings.RedisAddress)
	defer store.Close()
	origins := proxy.Origins{AppsDomain: settings.AppsDomain, PublicPort: settings.PublicPort}
	proxyBase := loopbackURL(settings.ObjectProxyPort)
	cacheClient := cache.NewClient(loopbackURL(settings.CachePort))
	quota := storage.Quota{ByService: settings.CacheQuota, Total: settings.CacheTotal}
	contextCache := cache.New(store, cache.NewProxyFetcher(proxyBase), quota, time.Now)
	cacheServer, err := listen(settings.CachePort, contextCache.Handler())
	if err != nil {
		slog.Error("application context cache not started", "error", err)
		os.Exit(1)
	}
	slog.Info("application context cache started", "port", settings.CachePort)
	objectProxy := proxy.New(origin.New(), time.Now, client, origins, cacheClient)
	server, err := listen(settings.ObjectProxyPort, objectProxy.Handler())
	if err != nil {
		slog.Error("object proxy not started", "error", err)
		os.Exit(1)
	}
	slog.Info("object proxy started", "port", settings.ObjectProxyPort)

	signalingAcquisition := acquisition.New(settings.SignalingBaseURL, proxyBase, client, time.Now)
	for _, filter := range proxy.Filters() {
		client.Subscribe(broker.Subscription{Filter: filter, Receive: objectProxy.Receive})
	}
	for _, filter := range []string{topics.LLSFilter(), topics.SLSFilter(), topics.EmissionFilter()} {
		client.Subscribe(broker.Subscription{Filter: filter, Receive: signalingAcquisition.Receive})
	}
	for _, filter := range cache.Filters() {
		client.Subscribe(broker.Subscription{Filter: filter, Receive: contextCache.Receive})
	}
	lifecycleManager := lifecycle.New(client, store, cacheClient, origins, time.Now)
	for _, filter := range lifecycle.Filters() {
		client.Subscribe(broker.Subscription{Filter: filter, Receive: lifecycleManager.Receive})
	}
	if err := client.Connect(ctx); err != nil {
		slog.Error("MQTT client not started", "error", err)
		os.Exit(1)
	}
	if err := client.AwaitConnection(ctx); err != nil {
		slog.Info("stopped before connecting to the MQTT broker")
		shutdown([]*http.Server{server, cacheServer}, client)
		return
	}
	slog.Info("acquisition started", "signaling", settings.SignalingBaseURL.String())
	go objectProxy.RunEvents(ctx)
	go contextCache.Run(ctx)
	go lifecycleManager.Run(ctx)
	signalingAcquisition.Run(ctx)

	shutdown([]*http.Server{server, cacheServer}, client)
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
			slog.Error("HTTP server stopped", "port", port, "error", err)
			os.Exit(1)
		}
	}()
	return server, nil
}

// The base of an HTTP server of this process, on the loopback.
func loopbackURL(port int) *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(loopbackHost, strconv.Itoa(port)),
		Path:   "/",
	}
}

func shutdown(servers []*http.Server, client *broker.Client) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown failed", "error", err)
		}
	}
	if err := client.Disconnect(shutdownCtx); err != nil {
		slog.Error("MQTT disconnect failed", "error", err)
	}
}
