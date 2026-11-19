// Package carousel joins the S-TSID and the emission descriptor of a service
// into its carousel: the enumerated objects, with their place in the lap, and
// the file templates. It counts the passes of each object.
package carousel

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/multisens/aop-cc/internal/emission"
)

// Carousel is what a service emits by broadcast.
type Carousel struct {
	ServiceID       int
	EmissionBaseURL string
	CycleEpoch      time.Time
	Objects         []Object
	Templates       []Template
}

// Object is an enumerated object of the S-TSID: a file, or an entry package
// (contentType multipart/related).
type Object struct {
	TSI             int
	ContentLocation string
	ContentType     string
	Order           int
	Count           int
	CycleEpoch      time.Time
	CyclePeriod     time.Duration
	HasExpiry       bool
	MaxExpiresDelta time.Duration
	LastModified    time.Time
	AppContextIDs   []string
	FilterCodes     []int
}

// Template is the fileTemplate of a flow, split at $TOI$.
type Template struct {
	TSI    int
	Prefix string
	Suffix string
}

// Pass is a pass of an object in the lap.
type Pass struct {
	Start time.Time
	End   time.Time
}

// ATSC A/331, A.3.3.2.3, in the JSON form of DP9.
type wireStsid struct {
	RS []struct {
		LS []struct {
			TSI     *int `json:"tsi"`
			SrcFlow struct {
				EFDT struct {
					Instance wireInstance `json:"FDT-Instance"`
				} `json:"EFDT"`
			} `json:"SrcFlow"`
		} `json:"LS"`
	} `json:"RS"`
}

type wireInstance struct {
	MaxExpiresDelta  *int       `json:"maxExpiresDelta"`
	AppContextIDList []string   `json:"appContextIdList"`
	FileTemplate     string     `json:"fileTemplate"`
	FilterCodes      []int      `json:"filterCodes"`
	File             []wireFile `json:"File"`
}

type wireFile struct {
	ContentLocation  string   `json:"Content-Location"`
	ContentType      string   `json:"Content-Type"`
	AppContextIDList []string `json:"appContextIdList"`
	FilterCodes      []int    `json:"filterCodes"`
}

// PackageType is the Content-Type of an entry package (ATSC A/331,
// A.3.3.4).
const PackageType = "multipart/related"

const toiVariable = "$TOI$"

// Build joins an S-TSID with the descriptor of its service. An object of
// the S-TSID without a place in the descriptor is left out and logged.
func Build(descriptor emission.Descriptor, stsidBody []byte) (Carousel, error) {
	var stsid wireStsid
	if err := json.Unmarshal(stsidBody, &stsid); err != nil {
		return Carousel{}, fmt.Errorf("S-TSID: %w", err)
	}
	flows := map[int]emission.Flow{}
	for _, flow := range descriptor.Flows {
		flows[flow.TSI] = flow
	}
	built := Carousel{
		ServiceID:       descriptor.ServiceID,
		EmissionBaseURL: descriptor.EmissionBaseURL,
		CycleEpoch:      descriptor.CycleEpoch,
		Objects:         []Object{},
		Templates:       []Template{},
	}
	for _, rs := range stsid.RS {
		for _, ls := range rs.LS {
			if ls.TSI == nil {
				return Carousel{}, fmt.Errorf("S-TSID: LS without tsi")
			}
			instance := ls.SrcFlow.EFDT.Instance
			if instance.FileTemplate != "" {
				template, err := splitTemplate(*ls.TSI, instance.FileTemplate)
				if err != nil {
					return Carousel{}, err
				}
				built.Templates = append(built.Templates, template)
			}
			flow, found := flows[*ls.TSI]
			if !found {
				slog.Error("S-TSID flow without place in the emission descriptor", "serviceId",
					descriptor.ServiceID, "tsi", *ls.TSI)
				continue
			}
			built.Objects = append(built.Objects, flowObjects(descriptor, flow, instance)...)
		}
	}
	return built, nil
}

// LastCompletePass is the pass c* that makes the object available:
// the last complete pass, begun at or after tunedAt and not expired; false
// when there is none.
func (o Object) LastCompletePass(now time.Time, tunedAt time.Time) (Pass, bool) {
	slice := o.CyclePeriod / time.Duration(o.Count)
	sinceFirstEnd := now.Sub(o.CycleEpoch) - time.Duration(o.Order+1)*slice
	if sinceFirstEnd < 0 {
		return Pass{}, false
	}
	lap := sinceFirstEnd / o.CyclePeriod
	start := o.CycleEpoch.Add(lap*o.CyclePeriod + time.Duration(o.Order)*slice)
	pass := Pass{Start: start, End: start.Add(slice)}
	if pass.Start.Before(tunedAt) {
		return Pass{}, false
	}
	if o.HasExpiry && !now.Before(pass.Start.Add(o.MaxExpiresDelta)) {
		return Pass{}, false
	}
	return pass, true
}

// Expires is ini(c*) + X, X being the maxExpiresDelta; false without it.
func (o Object) Expires(pass Pass) (time.Time, bool) {
	if !o.HasExpiry {
		return time.Time{}, false
	}
	return pass.Start.Add(o.MaxExpiresDelta), true
}

// NextChange is the first instant after now when the availability of the
// object may change: the end of a pass, or an expiry.
func (o Object) NextChange(now time.Time) time.Time {
	slice := o.CyclePeriod / time.Duration(o.Count)
	next := o.nextAfter(now, time.Duration(o.Order+1)*slice)
	if o.HasExpiry {
		expiry := o.nextAfter(now, time.Duration(o.Order)*slice+o.MaxExpiresDelta)
		if expiry.Before(next) {
			next = expiry
		}
	}
	return next
}

// TOI gives the TOI a path has under the template: an integer of at least
// 1 in place of $TOI$; false when the path does not match.
func (t Template) TOI(path string) (int, bool) {
	middle, found := strings.CutPrefix(path, t.Prefix)
	if !found {
		return 0, false
	}
	middle, found = strings.CutSuffix(middle, t.Suffix)
	if !found || middle == "" || strings.HasPrefix(middle, "0") {
		return 0, false
	}
	toi, err := strconv.Atoi(middle)
	if err != nil || toi < 1 {
		return 0, false
	}
	return toi, true
}

// Object gives the enumerated object at a contentLocation.
func (c Carousel) Object(contentLocation string) (Object, bool) {
	for _, object := range c.Objects {
		if object.ContentLocation == contentLocation {
			return object, true
		}
	}
	return Object{}, false
}

// AppContextIDs lists, sorted and without repetition, the Application
// Context IDs of the objects.
func (c Carousel) AppContextIDs() []string {
	ids := []string{}
	for _, object := range c.Objects {
		for _, id := range object.AppContextIDs {
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	return ids
}

// The objects of a flow, with the appContextIdList of the flow summed to that
// of the file; the filterCodes of a file prevail over those of the flow (ATSC
// A/331, A.3.3.2.3).
func flowObjects(descriptor emission.Descriptor, flow emission.Flow, instance wireInstance) []Object {
	places := map[string]emission.Object{}
	for _, object := range flow.Objects {
		places[object.ContentLocation] = object
	}
	objects := []Object{}
	for _, file := range instance.File {
		place, found := places[file.ContentLocation]
		if !found {
			slog.Error("S-TSID file without place in the emission descriptor", "serviceId", descriptor.ServiceID,
				"tsi", flow.TSI, "contentLocation", file.ContentLocation)
			continue
		}
		filterCodes := instance.FilterCodes
		if file.FilterCodes != nil {
			filterCodes = file.FilterCodes
		}
		object := Object{
			TSI:             flow.TSI,
			ContentLocation: file.ContentLocation,
			ContentType:     file.ContentType,
			Order:           place.Order,
			Count:           len(flow.Objects),
			CycleEpoch:      descriptor.CycleEpoch,
			CyclePeriod:     flow.CyclePeriod,
			LastModified:    place.LastModified,
			AppContextIDs:   union(instance.AppContextIDList, file.AppContextIDList),
			FilterCodes:     filterCodes,
		}
		if instance.MaxExpiresDelta != nil {
			object.HasExpiry = true
			object.MaxExpiresDelta = time.Duration(*instance.MaxExpiresDelta) * time.Second
		}
		objects = append(objects, object)
	}
	return objects
}

// The first instant after now of the form cycleEpoch + offset + k × P.
func (o Object) nextAfter(now time.Time, offset time.Duration) time.Time {
	first := o.CycleEpoch.Add(offset)
	if now.Before(first) {
		return first
	}
	laps := now.Sub(first)/o.CyclePeriod + 1
	return first.Add(laps * o.CyclePeriod)
}

func splitTemplate(tsi int, fileTemplate string) (Template, error) {
	prefix, suffix, found := strings.Cut(fileTemplate, toiVariable)
	if !found || strings.Contains(suffix, toiVariable) {
		return Template{}, fmt.Errorf("S-TSID: fileTemplate '%s' of flow %d needs one %s", fileTemplate, tsi,
			toiVariable)
	}
	return Template{TSI: tsi, Prefix: prefix, Suffix: suffix}, nil
}

func union(first []string, second []string) []string {
	ids := []string{}
	for _, id := range append(slices.Clone(first), second...) {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids
}
