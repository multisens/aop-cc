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
	SignalingBaseURL *url.URL
	ObjectProxyPort  int
	// PublicPort is the port of the object proxy as the browser reaches
	// it; AppsDomain, the domain of the origins by application context.
	PublicPort int
	AppsDomain string
	LogLevel   slog.Level
}

const (
	defaultMQTTPort = "1883"
	maxPort         = 65535
	maxDomainLength = 253 - 64
)

// A DNS label: letters, digits and '-', not at the ends.
var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

var logLevels = map[string]slog.Level{
	"ERROR": slog.LevelError,
	"INFO":  slog.LevelInfo,
	"DEBUG": slog.LevelDebug,
}

// Load reads MQTT_HOST, SIGNALING_BASE_URL, OBJECT_PROXY_PORT,
// OBJECT_PROXY_PUBLIC_PORT, APPS_DOMAIN and LOG_LEVEL.
func Load() (Config, error) {
	mqttURL, err := mqttURL(os.Getenv("MQTT_HOST"))
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
		SignalingBaseURL: signaling,
		ObjectProxyPort:  proxyPort,
		PublicPort:       publicPort,
		AppsDomain:       appsDomain,
		LogLevel:         level,
	}, nil
}

// MQTT_HOST is host or host:port, as in the other services.
func mqttURL(host string) (*url.URL, error) {
	if host == "" {
		return nil, fmt.Errorf("MQTT_HOST is required")
	}
	address := host
	if _, _, err := net.SplitHostPort(host); err != nil {
		address = net.JoinHostPort(host, defaultMQTTPort)
	}
	return &url.URL{Scheme: "mqtt", Host: address}, nil
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
