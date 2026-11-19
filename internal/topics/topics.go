// Package topics builds the names of the topics this service publishes and
// subscribes: the transport layer ones (implementation document, 2.3 and
// Appendix A.2) and the ones of aop-application-lifecycle-manager.
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

// SLS fragment names this service subscribes to.
const (
	STSID = "stsid"
	USBD  = "usbd"
	BALD  = "bald"
)

// Object events of 5.6.
const (
	ObjectAvailable = "available"
	ObjectExpired   = "expired"
)

const (
	llsBase       = "tlm/lls"
	slsBase       = "tlm/sls"
	tlBase        = "tlm"
	lifecycleBase = "aop/application-lifecycle-manager"
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

// LLSTableFilter is the subscription filter of a table of every RF
// channel: <lls>/+/{table}.
func LLSTableFilter(table string) string {
	return fmt.Sprintf("%s/+/%s", llsBase, table)
}

// LLSTable is the bsid and the table of an <lls> topic; false for any
// other topic.
func LLSTable(topic string) (int, string, bool) {
	suffix, found := strings.CutPrefix(topic, LLSPrefix())
	if !found {
		return 0, "", false
	}
	bsidSegment, table, found := strings.Cut(suffix, "/")
	if !found {
		return 0, "", false
	}
	bsid, err := strconv.Atoi(bsidSegment)
	if err != nil {
		return 0, "", false
	}
	return bsid, table, true
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

// ObjectFilter is the subscription filter of an object event of every
// service: <tl>/object/+/{event}.
func ObjectFilter(event string) string {
	return fmt.Sprintf("%s/object/+/%s", tlBase, event)
}

// ObjectEvent is the serviceId and the event of an object event topic;
// false for any other topic.
func ObjectEvent(topic string) (int, string, bool) {
	suffix, found := strings.CutPrefix(topic, tlBase+"/object/")
	if !found {
		return 0, "", false
	}
	serviceSegment, event, found := strings.Cut(suffix, "/")
	if !found {
		return 0, "", false
	}
	serviceID, err := strconv.Atoi(serviceSegment)
	if err != nil {
		return 0, "", false
	}
	return serviceID, event, true
}

// ApplicationState is the change of state of an application, not
// retained.
func ApplicationState() string {
	return lifecycleBase + "/application-state"
}

// GraphicsApplication is the broadcaster application of the graphics
// layer, retained; empty is the Bootstrap Application.
func GraphicsApplication() string {
	return lifecycleBase + "/graphics-application"
}

// PresentApplications is the list of the PRESENT and STORED_PRESENT
// applications of the tuned service, retained.
func PresentApplications() string {
	return lifecycleBase + "/present-applications"
}

// EndRequest is the request of the viewer to end the running application.
func EndRequest() string {
	return lifecycleBase + "/end-request"
}

// Scan is <tl>/scan.
func Scan() string {
	return tlBase + "/scan"
}
