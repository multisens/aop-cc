// Command aop-cc is the Common Core component of the platform: in this
// stage, the acquisition of the transport layer signaling.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/multisens/aop-cc/internal/acquisition"
	"github.com/multisens/aop-cc/internal/broker"
	"github.com/multisens/aop-cc/internal/config"
)

const (
	clientID        = "aop-cc"
	shutdownTimeout = 2 * time.Second
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

	client, err := broker.Connect(ctx, settings.MQTTURL, clientID)
	if err != nil {
		slog.Error("MQTT client not started", "error", err)
		os.Exit(1)
	}
	if err := client.AwaitConnection(ctx); err != nil {
		slog.Info("stopped before connecting to the MQTT broker")
		return
	}
	slog.Info("acquisition started", "signaling", settings.SignalingBaseURL.String())
	acquisition.New(settings.SignalingBaseURL, client, time.Now).Run(ctx)

	disconnectCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := client.Disconnect(disconnectCtx); err != nil {
		slog.Error("MQTT disconnect failed", "error", err)
	}
	slog.Info("acquisition stopped")
}
