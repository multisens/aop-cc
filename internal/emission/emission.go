// Package emission decodes the emission descriptor (implementation
// document, 3.7) and counts the passes of the signaling channel (5.3).
package emission

import (
	"encoding/json"
	"fmt"
	"time"
)

// Descriptor is the part of the emission descriptor used here: where the
// SLS package is and when it passes.
type Descriptor struct {
	ServiceID  int
	CycleEpoch time.Time
	Signaling  Signaling
}

// Signaling is the signaling channel of a service.
type Signaling struct {
	URL         string
	CyclePeriod time.Duration
}

type wireDescriptor struct {
	ServiceID  *int           `json:"serviceId"`
	CycleEpoch *string        `json:"cycleEpoch"`
	Signaling  *wireSignaling `json:"signaling"`
}

type wireSignaling struct {
	URL         string `json:"url"`
	CyclePeriod *int   `json:"cyclePeriod"`
}

// Decode reads a descriptor; serviceId, cycleEpoch, signaling.url and a
// positive signaling.cyclePeriod are required.
func Decode(body []byte) (Descriptor, error) {
	var wire wireDescriptor
	if err := json.Unmarshal(body, &wire); err != nil {
		return Descriptor{}, err
	}
	if wire.ServiceID == nil || wire.CycleEpoch == nil || wire.Signaling == nil {
		return Descriptor{}, fmt.Errorf("serviceId, cycleEpoch or signaling missing")
	}
	epoch, err := time.Parse(time.RFC3339, *wire.CycleEpoch)
	if err != nil {
		return Descriptor{}, fmt.Errorf("cycleEpoch: %w", err)
	}
	if wire.Signaling.URL == "" || wire.Signaling.CyclePeriod == nil || *wire.Signaling.CyclePeriod <= 0 {
		return Descriptor{}, fmt.Errorf("signaling.url or a positive signaling.cyclePeriod missing")
	}
	return Descriptor{
		ServiceID:  *wire.ServiceID,
		CycleEpoch: epoch,
		Signaling: Signaling{
			URL:         wire.Signaling.URL,
			CyclePeriod: time.Duration(*wire.Signaling.CyclePeriod) * time.Millisecond,
		},
	}, nil
}

// LastSignalingPassEnd is fim(c*) of the signaling count (5.3): the end of
// the last complete pass of the SLS package; false before the first one
// ends.
func (d Descriptor) LastSignalingPassEnd(now time.Time) (time.Time, bool) {
	period := d.Signaling.CyclePeriod
	elapsed := now.Sub(d.CycleEpoch)
	if elapsed < period {
		return time.Time{}, false
	}
	completed := elapsed / period
	return d.CycleEpoch.Add(completed * period), true
}

// NextSignalingPassEnd is the first fim(c) of the signaling count after
// now.
func (d Descriptor) NextSignalingPassEnd(now time.Time) time.Time {
	period := d.Signaling.CyclePeriod
	elapsed := now.Sub(d.CycleEpoch)
	if elapsed < period {
		return d.CycleEpoch.Add(period)
	}
	completed := elapsed / period
	return d.CycleEpoch.Add((completed + 1) * period)
}

// SameSignaling tells whether two descriptors give the same signaling
// channel and count.
func (d Descriptor) SameSignaling(other Descriptor) bool {
	return d.CycleEpoch.Equal(other.CycleEpoch) && d.Signaling == other.Signaling
}
