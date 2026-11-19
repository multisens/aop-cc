// Package topics builds the names of the transport layer topics this
// service publishes and subscribes (implementation document, 2.3 and
// Appendix A.2).
package topics

import (
	"fmt"
	"strconv"
	"strings"
)

// LLS table names, the last level of <lls>/{bsid}/{table}.
const (
	SLT  = "slt"
	BAMT = "bamt"
)

// SLS fragment names the object proxy subscribes to.
const (
	STSID = "stsid"
	USBD  = "usbd"
)

// Object events of 5.6.
const (
	ObjectAvailable = "available"
	ObjectExpired   = "expired"
)

const (
	llsBase = "tlm/lls"
	slsBase = "tlm/sls"
	tlBase  = "tlm"
)

// slsFragments are the metadataURI values that have a topic.
var slsFragments = map[string]bool{
	"usbd":  true,
	"stsid": true,
	"mpd":   true,
	"bald":  true,
	"prrd":  true,
}

// LLS is <lls>/{bsid}/{table}.
func LLS(bsid int, table string) string {
	return fmt.Sprintf("%s/%d/%s", llsBase, bsid, table)
}

// LLSPrefix is the prefix of every LLS topic.
func LLSPrefix() string {
	return llsBase + "/"
}

// SLS is <sls>/{serviceId}/{fragment}; false for a fragment without topic.
func SLS(serviceID int, fragment string) (string, bool) {
	if !slsFragments[fragment] {
		return "", false
	}
	return fmt.Sprintf("%s/%d/%s", slsBase, serviceID, fragment), true
}

// LLSFilter is the subscription filter of every LLS topic.
func LLSFilter() string {
	return llsBase + "/#"
}

// SLSFilter is the subscription filter of every SLS topic.
func SLSFilter() string {
	return slsBase + "/#"
}

// AnySLSPrefix is the prefix of every SLS topic of every service.
func AnySLSPrefix() string {
	return slsBase + "/"
}

// SLSServiceID is the serviceId of an <sls> topic; false for any other
// topic.
func SLSServiceID(topic string) (int, bool) {
	suffix, found := strings.CutPrefix(topic, AnySLSPrefix())
	if !found {
		return 0, false
	}
	serviceSegment, _, _ := strings.Cut(suffix, "/")
	serviceID, err := strconv.Atoi(serviceSegment)
	if err != nil {
		return 0, false
	}
	return serviceID, true
}

// SLSFragmentFilter is the subscription filter of a fragment of every
// service: <sls>/+/{fragment}.
func SLSFragmentFilter(fragment string) string {
	return fmt.Sprintf("%s/+/%s", slsBase, fragment)
}

// SLSFragment is the serviceId and the fragment of an <sls> topic; false
// for any other topic.
func SLSFragment(topic string) (int, string, bool) {
	serviceID, found := SLSServiceID(topic)
	if !found {
		return 0, "", false
	}
	return serviceID, topic[strings.LastIndex(topic, "/")+1:], true
}

// SLSPrefix is the prefix of every SLS topic of a service.
func SLSPrefix(serviceID int) string {
	return fmt.Sprintf("%s/%d/", slsBase, serviceID)
}

// EmissionPrefix is the prefix of every <tl>/emission topic.
func EmissionPrefix() string {
	return tlBase + "/emission/"
}

// Emission is <tl>/emission/{serviceId}.
func Emission(serviceID int) string {
	return fmt.Sprintf("%s%d", EmissionPrefix(), serviceID)
}

// EmissionFilter is the subscription filter of every <tl>/emission topic.
func EmissionFilter() string {
	return EmissionPrefix() + "+"
}

// EmissionServiceID is the serviceId of an <tl>/emission topic; false for
// any other topic.
func EmissionServiceID(topic string) (int, bool) {
	suffix, found := strings.CutPrefix(topic, EmissionPrefix())
	if !found {
		return 0, false
	}
	serviceID, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, false
	}
	return serviceID, true
}

// Tuned is <tl>/tuned.
func Tuned() string {
	return tlBase + "/tuned"
}

// Object is <tl>/object/{serviceId}/{event}.
func Object(serviceID int, event string) string {
	return fmt.Sprintf("%s/object/%d/%s", tlBase, serviceID, event)
}

// Scan is <tl>/scan.
func Scan() string {
	return tlBase + "/scan"
}
