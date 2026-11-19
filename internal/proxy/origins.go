package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
)

// The label of a context origin: ctx- and the first 16 hexadecimal digits of
// the SHA-256 of the appContextId in UTF-8; that of a broadband origin, the
// same digits after bb-.
const (
	contextLabelPrefix   = "ctx-"
	broadbandLabelPrefix = "bb-"
	contextHashDigits    = 16
	httpPort             = 80
)

// ContextLabel is the DNS label of the origin of an Application Context ID.
func ContextLabel(appContextID string) string {
	return contextLabelPrefix + contextHash(appContextID)
}

// BroadbandLabel is the DNS label of the origin under which the broadband
// application of an Application Context ID is served.
func BroadbandLabel(appContextID string) string {
	return broadbandLabelPrefix + contextHash(appContextID)
}

// ContextOrigin is <app:{appContextId}>, as the browser writes it in the
// Origin header (port 80 omitted).
func (o Origins) ContextOrigin(appContextID string) string {
	return fmt.Sprintf("http://%s.%s%s", ContextLabel(appContextID), o.AppsDomain, o.portSuffix())
}

// BroadbandOrigin is the origin of the broadband application of an
// Application Context ID, as the browser writes it in the Origin header.
func (o Origins) BroadbandOrigin(appContextID string) string {
	return fmt.Sprintf("http://%s.%s%s", BroadbandLabel(appContextID), o.AppsDomain, o.portSuffix())
}

// ServiceBase is <comporta>/svc/{serviceId}/, as the browser reaches it.
func (o Origins) ServiceBase(serviceID int) string {
	return fmt.Sprintf("http://%s%s/svc/%d/", o.AppsDomain, o.portSuffix(), serviceID)
}

// hostLabel is the context or broadband label of a Host header under the
// apps domain; false for any other host.
func (o Origins) hostLabel(host string) (string, bool) {
	name := strings.ToLower(host)
	if hostOnly, _, err := net.SplitHostPort(name); err == nil {
		name = hostOnly
	}
	label, found := strings.CutSuffix(name, "."+o.AppsDomain)
	if !found || strings.Contains(label, ".") {
		return "", false
	}
	if !strings.HasPrefix(label, contextLabelPrefix) && !strings.HasPrefix(label, broadbandLabelPrefix) {
		return "", false
	}
	return label, true
}

func contextHash(appContextID string) string {
	digest := sha256.Sum256([]byte(appContextID))
	return hex.EncodeToString(digest[:])[:contextHashDigits]
}

// ":port", empty for port 80.
func (o Origins) portSuffix() string {
	if o.PublicPort == httpPort {
		return ""
	}
	return fmt.Sprintf(":%d", o.PublicPort)
}
