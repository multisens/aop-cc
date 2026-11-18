// Package topics builds the names of the transport layer topics this
// service publishes (implementation document, 2.3 and Appendix A.2).
package topics

import "fmt"

// LLS table names, the last level of <lls>/{bsid}/{table}.
const (
	SLT  = "slt"
	BAMT = "bamt"
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

// SLSPrefix is the prefix of every SLS topic of a service.
func SLSPrefix(serviceID int) string {
	return fmt.Sprintf("%s/%d/", slsBase, serviceID)
}

// Emission is <tl>/emission/{serviceId}.
func Emission(serviceID int) string {
	return fmt.Sprintf("%s/emission/%d", tlBase, serviceID)
}

// Scan is <tl>/scan.
func Scan() string {
	return tlBase + "/scan"
}
