// Package config reads and checks the environment at startup.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
)

// Config is the checked environment of the service.
type Config struct {
	MQTTURL          *url.URL
	SignalingBaseURL *url.URL
	LogLevel         slog.Level
}

const defaultMQTTPort = "1883"

var logLevels = map[string]slog.Level{
	"ERROR": slog.LevelError,
	"INFO":  slog.LevelInfo,
	"DEBUG": slog.LevelDebug,
}

// Load reads MQTT_HOST, SIGNALING_BASE_URL and LOG_LEVEL.
func Load() (Config, error) {
	mqttURL, err := mqttURL(os.Getenv("MQTT_HOST"))
	if err != nil {
		return Config{}, err
	}
	signaling, err := baseURL("SIGNALING_BASE_URL", os.Getenv("SIGNALING_BASE_URL"))
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
	return Config{MQTTURL: mqttURL, SignalingBaseURL: signaling, LogLevel: level}, nil
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
