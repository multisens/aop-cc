// Package emission decodes the emission descriptor (implementation
// document, 3.7) and counts the passes of the signaling channel (5.3).
package emission

import (
	"encoding/json"
	"fmt"
	"time"
)

// Descriptor is the emission descriptor: where the objects and the SLS
// package are, and when they pass.
type Descriptor struct {
	ServiceID       int
	EmissionBaseURL string
	CycleEpoch      time.Time
	Signaling       Signaling
	Flows           []Flow
}

// Flow is a carousel flow: the time of a lap and its enumerated objects;
// a flow with fileTemplate has none.
type Flow struct {
	TSI         int
	CyclePeriod time.Duration
	Objects     []Object
}

// Object is the place of an object in the lap and its version.
type Object struct {
	ContentLocation string
	Order           int
	LastModified    time.Time
}

// Signaling is the signaling channel of a service.
type Signaling struct {
	URL         string
	CyclePeriod time.Duration
}

type wireDescriptor struct {
	ServiceID       *int           `json:"serviceId"`
	EmissionBaseURL string         `json:"emissionBaseUrl"`
	CycleEpoch      *string        `json:"cycleEpoch"`
	Signaling       *wireSignaling `json:"signaling"`
	Flows           []wireFlow     `json:"flows"`
}

type wireFlow struct {
	TSI         int          `json:"tsi"`
	CyclePeriod int          `json:"cyclePeriod"`
	Objects     []wireObject `json:"objects"`
}

type wireObject struct {
	ContentLocation string `json:"contentLocation"`
	Order           int    `json:"order"`
	LastModified    string `json:"lastModified"`
}

type wireSignaling struct {
	URL         string `json:"url"`
	CyclePeriod *int   `json:"cyclePeriod"`
}

// Decode reads a descriptor; serviceId, cycleEpoch, signaling.url and a
// positive signaling.cyclePeriod are required, and each flow needs a
// positive cyclePeriod and a lastModified date in each object.
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
	flows, err := decodeFlows(wire.Flows)
	if err != nil {
		return Descriptor{}, err
	}
	return Descriptor{
		ServiceID:       *wire.ServiceID,
		EmissionBaseURL: wire.EmissionBaseURL,
		CycleEpoch:      epoch,
		Signaling: Signaling{
			URL:         wire.Signaling.URL,
			CyclePeriod: time.Duration(*wire.Signaling.CyclePeriod) * time.Millisecond,
		},
		Flows: flows,
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

func decodeFlows(wire []wireFlow) ([]Flow, error) {
	flows := []Flow{}
	for _, flow := range wire {
		if flow.CyclePeriod <= 0 {
			return nil, fmt.Errorf("flow %d: cyclePeriod must be positive", flow.TSI)
		}
		objects := []Object{}
		for _, object := range flow.Objects {
			lastModified, err := time.Parse(time.RFC3339, object.LastModified)
			if err != nil {
				return nil, fmt.Errorf("flow %d, %s: lastModified: %w", flow.TSI, object.ContentLocation, err)
			}
			objects = append(objects, Object{
				ContentLocation: object.ContentLocation,
				Order:           object.Order,
				LastModified:    lastModified,
			})
		}
		flows = append(flows, Flow{
			TSI:         flow.TSI,
			CyclePeriod: time.Duration(flow.CyclePeriod) * time.Millisecond,
			Objects:     objects,
		})
	}
	return flows, nil
}
