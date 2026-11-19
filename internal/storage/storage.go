// Package storage writes the platform state of this service to Redis, only
// through the functions of the tv30 library (infra/redis/schema.json), which
// check the schema of each write.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// App is the record of session:current-app.
type App struct {
	ServiceContextID string
	AppID            string
	Origin           string
}

// CachedFile is a file of the Application Context Cache.
type CachedFile struct {
	ContentType  string
	LastModified time.Time
	Body         []byte
}

// Quota is the limit of the cache: by DTV service context and in total, in
// bytes.
type Quota struct {
	ByService int64
	Total     int64
}

// Store is the Redis client of the service.
type Store struct {
	client *redis.Client
}

// The metadata of a cached file, as cache:meta:{appContextId} keeps it.
type cachedMeta struct {
	ContentType  string `json:"contentType"`
	LastModified int64  `json:"lastModified"`
	Size         int    `json:"size"`
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

// CachePut stores a file of an Application Context ID of a DTV service
// context; false when the quota or the total refuses it. evicted are the
// Application Context IDs that left the cache to make room.
func (s *Store) CachePut(ctx context.Context, appContextID string, serviceContextID string, path string,
	file CachedFile, now time.Time, quota Quota) (bool, []string, error) {
	meta, err := json.Marshal(cachedMeta{ContentType: file.ContentType, LastModified: file.LastModified.UnixMilli(),
		Size: len(file.Body)})
	if err != nil {
		return false, nil, err
	}
	values := []interface{}{appContextID, serviceContextID, path, string(meta), millis(now),
		strconv.FormatInt(quota.ByService, 10), strconv.FormatInt(quota.Total, 10), file.Body}
	reply, err := s.client.FCall(ctx, "tv30_cache_put", nil, values...).Slice()
	if err != nil {
		return false, nil, err
	}
	if len(reply) == 0 {
		return false, nil, fmt.Errorf("tv30_cache_put: empty reply")
	}
	stored, isInt := reply[0].(int64)
	if !isInt {
		return false, nil, fmt.Errorf("tv30_cache_put: reply %v", reply)
	}
	evicted := []string{}
	for _, item := range reply[1:] {
		evicted = append(evicted, fmt.Sprint(item))
	}
	return stored == 1, evicted, nil
}

// CacheTouch records a read of the Application Context ID.
func (s *Store) CacheTouch(ctx context.Context, appContextID string, now time.Time) error {
	return s.fcall(ctx, "tv30_cache_touch", appContextID, millis(now))
}

// CacheRemove takes the Application Context ID out of the cache.
func (s *Store) CacheRemove(ctx context.Context, appContextID string) error {
	return s.fcall(ctx, "tv30_cache_remove", appContextID)
}

// CacheClearBefore removes the files of the Application Context ID whose
// Last-Modified is before the instant; gives how many.
func (s *Store) CacheClearBefore(ctx context.Context, appContextID string, before time.Time) (int64, error) {
	return s.client.FCall(ctx, "tv30_cache_clear_before", nil, appContextID, millis(before)).Int64()
}

// CacheFile reads a cached file; false when it is not in the cache.
func (s *Store) CacheFile(ctx context.Context, appContextID string, path string) (CachedFile, bool, error) {
	encoded, err := s.client.HGet(ctx, cacheKey("meta", appContextID), path).Result()
	if errors.Is(err, redis.Nil) {
		return CachedFile{}, false, nil
	}
	if err != nil {
		return CachedFile{}, false, err
	}
	var meta cachedMeta
	if err := json.Unmarshal([]byte(encoded), &meta); err != nil {
		return CachedFile{}, false, err
	}
	body, err := s.client.HGet(ctx, cacheKey("body", appContextID), path).Bytes()
	if errors.Is(err, redis.Nil) {
		return CachedFile{}, false, nil
	}
	if err != nil {
		return CachedFile{}, false, err
	}
	file := CachedFile{ContentType: meta.ContentType, LastModified: time.UnixMilli(meta.LastModified), Body: body}
	return file, true, nil
}

// CachePaths are the paths of the cached files of the Application Context
// ID.
func (s *Store) CachePaths(ctx context.Context, appContextID string) ([]string, error) {
	return s.client.HKeys(ctx, cacheKey("meta", appContextID)).Result()
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

// cache:{kind}:{appContextId}, kind being meta or body.
func cacheKey(kind string, appContextID string) string {
	return "cache:" + kind + ":" + appContextID
}

func millis(instant time.Time) string {
	return strconv.FormatInt(instant.UnixMilli(), 10)
}
