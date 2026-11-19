// Package storage writes the platform state of this service to Redis, only
// through the functions of the tv30 library (infra/redis/schema.json), which
// check the schema of each write.
package storage

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// App is the record of session:current-app.
type App struct {
	ServiceContextID string
	AppID            string
	Origin           string
}

// Store is the Redis client of the service.
type Store struct {
	client *redis.Client
}

// A command without a connection fails after one retry; one that is not
// answered, after the read timeout.
const (
	dialTimeout  = 2 * time.Second
	readTimeout  = 1500 * time.Millisecond
	writeTimeout = 1500 * time.Millisecond
	maxRetries   = 1
)

// New gives a Store for the Redis at address (host:port).
func New(address string) *Store {
	client := redis.NewClient(&redis.Options{
		Addr:         address,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		MaxRetries:   maxRetries,
	})
	return &Store{client: client}
}

// OriginRegister maps the origin of an application to the globalServiceId
// of its service in origins:associated.
func (s *Store) OriginRegister(ctx context.Context, origin string, serviceContextID string) error {
	return s.fcall(ctx, "tv30_origin_register", origin, serviceContextID)
}

// OriginRemove removes the origin from origins:associated.
func (s *Store) OriginRemove(ctx context.Context, origin string) error {
	return s.fcall(ctx, "tv30_origin_remove", origin)
}

// OriginsClear deletes origins:associated.
func (s *Store) OriginsClear(ctx context.Context) error {
	return s.fcall(ctx, "tv30_origins_clear")
}

// CurrentApp rewrites session:current-app with the running application.
func (s *Store) CurrentApp(ctx context.Context, app App) error {
	return s.fcall(ctx, "tv30_current_app",
		"serviceContextId", app.ServiceContextID,
		"appId", app.AppID,
		"origin", app.Origin)
}

// ClearCurrentApp deletes session:current-app.
func (s *Store) ClearCurrentApp(ctx context.Context) error {
	return s.fcall(ctx, "tv30_current_app")
}

// Close closes the client.
func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) fcall(ctx context.Context, function string, args ...string) error {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = arg
	}
	return s.client.FCall(ctx, function, nil, values...).Err()
}
