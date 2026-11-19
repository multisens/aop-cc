// Package broker publishes retained messages on the MQTT 5 broker and
// delivers the messages of the subscribed topics.
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

// Subscription is a topic filter and the function that receives each
// message on it, retained ones included; an empty payload is a cleared
// topic.
type Subscription struct {
	Filter  string
	Receive func(topic string, payload []byte)
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

// Connect starts the connection and subscribes on every connection up;
// Publish fails while it is down.
func Connect(ctx context.Context, server *url.URL, clientID string, subscriptions []Subscription) (*Client,
	error) {
	router := paho.NewStandardRouter()
	for _, subscription := range subscriptions {
		receive := subscription.Receive
		router.RegisterHandler(subscription.Filter, func(message *paho.Publish) {
			receive(message.Topic, message.Payload)
		})
	}
	manager, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{server},
		KeepAlive:                     keepAliveSeconds,
		CleanStartOnInitialConnection: true,
		ReconnectBackoff:              func(int) time.Duration { return reconnectDelay },
		OnConnectionUp: func(manager *autopaho.ConnectionManager, _ *paho.Connack) {
			slog.Info("connected to the MQTT broker", "server", server.Host)
			subscribe(ctx, manager, subscriptions)
		},
		OnConnectError: func(err error) {
			slog.Error("MQTT connection failed", "server", server.Host, "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(received paho.PublishReceived) (bool, error) {
					router.Route(received.Packet.Packet())
					return true, nil
				},
			},
		},
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

// A failed subscription is logged: the messages of that filter are lost
// until the next connection.
func subscribe(ctx context.Context, manager *autopaho.ConnectionManager, subscriptions []Subscription) {
	if len(subscriptions) == 0 {
		return
	}
	options := []paho.SubscribeOptions{}
	for _, subscription := range subscriptions {
		options = append(options, paho.SubscribeOptions{Topic: subscription.Filter, QoS: 1})
	}
	acknowledgement, err := manager.Subscribe(ctx, &paho.Subscribe{Subscriptions: options})
	if err != nil {
		slog.Error("MQTT subscription failed", "error", err)
		return
	}
	for index, code := range acknowledgement.Reasons {
		if code >= firstErrorCode {
			slog.Error("MQTT subscription refused", "filter", options[index].Topic,
				"reasonCode", fmt.Sprintf("0x%02x", code))
		}
	}
}
