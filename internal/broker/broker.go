// Package broker publishes retained messages on the MQTT 5 broker.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// Publisher publishes a retained message; an empty payload clears the
// retained message of the topic.
type Publisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}

// Client is a Publisher over a connection that reconnects by itself.
type Client struct {
	manager *autopaho.ConnectionManager
}

const (
	keepAliveSeconds = 20
	reconnectDelay   = 2 * time.Second
	firstErrorCode   = 0x80
)

// ErrRefused is the broker refusing the message, as it does for a payload
// outside the schema of the topic.
var ErrRefused = errors.New("refused by the broker")

// Connect starts the connection; Publish fails while it is down.
func Connect(ctx context.Context, server *url.URL, clientID string) (*Client, error) {
	manager, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{server},
		KeepAlive:                     keepAliveSeconds,
		CleanStartOnInitialConnection: true,
		ReconnectBackoff:              func(int) time.Duration { return reconnectDelay },
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			slog.Info("connected to the MQTT broker", "server", server.Host)
		},
		OnConnectError: func(err error) {
			slog.Error("MQTT connection failed", "server", server.Host, "error", err)
		},
		ClientConfig: paho.ClientConfig{ClientID: clientID},
	})
	if err != nil {
		return nil, err
	}
	return &Client{manager: manager}, nil
}

// Publish sends with QoS 1 and retain; a reason code of error in the
// PUBACK gives ErrRefused.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) error {
	response, err := c.manager.Publish(ctx, &paho.Publish{
		QoS:     1,
		Retain:  true,
		Topic:   topic,
		Payload: payload,
	})
	if response != nil && response.ReasonCode >= firstErrorCode {
		return fmt.Errorf("%w: reason code 0x%02x", ErrRefused, response.ReasonCode)
	}
	return err
}

// AwaitConnection waits for the first connection.
func (c *Client) AwaitConnection(ctx context.Context) error {
	return c.manager.AwaitConnection(ctx)
}

// Disconnect ends the connection.
func (c *Client) Disconnect(ctx context.Context) error {
	return c.manager.Disconnect(ctx)
}

// Done is closed when the connection manager stops.
func (c *Client) Done() <-chan struct{} {
	return c.manager.Done()
}
