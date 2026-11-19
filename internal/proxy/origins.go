package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
)

// 5.4, "Origem por contexto de aplicação": ctx- and the first 16
// hexadecimal digits of the SHA-256 of the appContextId in UTF-8.
const (
	contextLabelPrefix = "ctx-"
	contextHashDigits  = 16
)

// ContextLabel is the DNS label of the origin of an Application Context ID.
func ContextLabel(appContextID string) string {
	digest := sha256.Sum256([]byte(appContextID))
	return contextLabelPrefix + hex.EncodeToString(digest[:])[:contextHashDigits]
}

// ContextOrigin is <app:{appContextId}>, as the browser writes it.
func (o Origins) ContextOrigin(appContextID string) string {
	return fmt.Sprintf("http://%s.%s:%d", ContextLabel(appContextID), o.AppsDomain, o.PublicPort)
}

// ServiceBase is <comporta>/svc/{serviceId}/, as the browser reaches it.
func (o Origins) ServiceBase(serviceID int) string {
	return fmt.Sprintf("http://%s:%d/svc/%d/", o.AppsDomain, o.PublicPort, serviceID)
}

// hostContextLabel is the context label of a Host header under the apps
// domain; false for any other host.
func (o Origins) hostContextLabel(host string) (string, bool) {
	name := strings.ToLower(host)
	if hostOnly, _, err := net.SplitHostPort(name); err == nil {
		name = hostOnly
	}
	label, found := strings.CutSuffix(name, "."+o.AppsDomain)
	if !found || !strings.HasPrefix(label, contextLabelPrefix) || strings.Contains(label, ".") {
		return "", false
	}
	return label, true
}
