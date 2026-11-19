// Package config reads and checks the environment at startup.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Config is the checked environment of the service.
type Config struct {
	MQTTURL          *url.URL
	RedisAddress     string
	SignalingBaseURL *url.URL
	ObjectProxyPort  int
	// PublicPort is the port of the object proxy as the browser reaches
	// it; AppsDomain, the domain of the origins by application context.
	PublicPort int
	AppsDomain string
	// CachePort is the port of the routes of the Application Context Cache;
	// CacheQuota and CacheTotal, its limits in bytes: by DTV service context
	// and in total.
	CachePort  int
	CacheQuota int64
	CacheTotal int64
	LogLevel   slog.Level
}

const (
	defaultMQTTPort  = "1883"
	defaultRedisPort = "6379"
	maxPort          = 65535
	maxDomainLength  = 253 - 64
	// ABNT NBR 25608, 10.3.3, for the 4K@60fps receiver profiles: 112 MB by
	// DTV service context and 500 MB in total.
	defaultCacheQuota = 112_000_000
	defaultCacheTotal = 500_000_000
)

// A DNS label: letters, digits and '-', not at the ends.
var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

var logLevels = map[string]slog.Level{
	"ERROR": slog.LevelError,
	"INFO":  slog.LevelInfo,
	"DEBUG": slog.LevelDebug,
}

// Load reads MQTT_HOST, REDIS_HOST, SIGNALING_BASE_URL, OBJECT_PROXY_PORT,
// OBJECT_PROXY_PUBLIC_PORT, APPS_DOMAIN, CACHE_PORT, CACHE_QUOTA_BYTES,
// CACHE_TOTAL_BYTES and LOG_LEVEL.
func Load() (Config, error) {
	mqttURL, err := mqttURL(os.Getenv("MQTT_HOST"))
	if err != nil {
		return Config{}, err
	}
	redisAddress, err := hostPort("REDIS_HOST", os.Getenv("REDIS_HOST"), defaultRedisPort)
	if err != nil {
		return Config{}, err
	}
	signaling, err := baseURL("SIGNALING_BASE_URL", os.Getenv("SIGNALING_BASE_URL"))
	if err != nil {
		return Config{}, err
	}
	proxyPort, err := port("OBJECT_PROXY_PORT", os.Getenv("OBJECT_PROXY_PORT"))
	if err != nil {
		return Config{}, err
	}
	publicPort, err := port("OBJECT_PROXY_PUBLIC_PORT", os.Getenv("OBJECT_PROXY_PUBLIC_PORT"))
	if err != nil {
		return Config{}, err
	}
	appsDomain, err := domain("APPS_DOMAIN", os.Getenv("APPS_DOMAIN"))
	if err != nil {
		return Config{}, err
	}
	cachePort, err := port("CACHE_PORT", os.Getenv("CACHE_PORT"))
	if err != nil {
		return Config{}, err
	}
	cacheQuota, err := bytesOr("CACHE_QUOTA_BYTES", os.Getenv("CACHE_QUOTA_BYTES"), defaultCacheQuota)
	if err != nil {
		return Config{}, err
	}
	cacheTotal, err := bytesOr("CACHE_TOTAL_BYTES", os.Getenv("CACHE_TOTAL_BYTES"), defaultCacheTotal)
	if err != nil {
		return Config{}, err
	}
	levelName := os.Getenv("LOG_LEVEL")
	if levelName == "" {
		levelName = "INFO"
	}
	level, known := logLevels[levelName]
	if !known {
		return Config{}, fmt.Errorf("LOG_LEVEL must be ERROR, INFO or DEBUG; got '%s'", levelName)
	}
	return Config{
		MQTTURL:          mqttURL,
		RedisAddress:     redisAddress,
		SignalingBaseURL: signaling,
		ObjectProxyPort:  proxyPort,
		PublicPort:       publicPort,
		AppsDomain:       appsDomain,
		CachePort:        cachePort,
		CacheQuota:       cacheQuota,
		CacheTotal:       cacheTotal,
		LogLevel:         level,
	}, nil
}

// MQTT_HOST is host or host:port, as in the other services.
func mqttURL(host string) (*url.URL, error) {
	address, err := hostPort("MQTT_HOST", host, defaultMQTTPort)
	if err != nil {
		return nil, err
	}
	return &url.URL{Scheme: "mqtt", Host: address}, nil
}

// host or host:port; without the port, defaultPort.
func hostPort(name string, value string, defaultPort string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	if _, _, err := net.SplitHostPort(value); err != nil {
		return net.JoinHostPort(value, defaultPort), nil
	}
	return value, nil
}

// An absolute http(s) URL ending in '/'.
func baseURL(name string, value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("%s must be an absolute http(s) URL; got '%s'", name, value)
	}
	if !strings.HasSuffix(parsed.Path, "/") {
		return nil, fmt.Errorf("%s must end in '/'; got '%s'", name, value)
	}
	return parsed, nil
}

// A TCP port, 1 to 65535.
func port(name string, value string) (int, error) {
	number, err := strconv.Atoi(value)
	if err != nil || number < 1 || number > maxPort {
		return 0, fmt.Errorf("%s must be a TCP port; got '%s'", name, value)
	}
	return number, nil
}

// A DNS name of lowercase labels, short enough for a label in front of it.
func domain(name string, value string) (string, error) {
	labels := strings.Split(value, ".")
	valid := value != "" && len(value) <= maxDomainLength
	for _, label := range labels {
		valid = valid && dnsLabel.MatchString(label)
	}
	if !valid {
		return "", fmt.Errorf("%s must be a DNS name of lowercase labels; got '%s'", name, value)
	}
	return value, nil
}

// A positive number of bytes; empty gives the default.
func bytesOr(name string, value string, fallback int64) (int64, error) {
	if value == "" {
		return fallback, nil
	}
	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil || number < 1 {
		return 0, fmt.Errorf("%s must be a positive number of bytes; got '%s'", name, value)
	}
	return number, nil
}
