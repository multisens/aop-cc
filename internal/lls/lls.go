// Package lls is aop-transport-layer-metadata-lls: the scan of the LLS tables
// of every RF channel and their publication, one retained topic per table.
package lls

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/retained"
	"github.com/multisens/aop-cc/internal/topics"
)

// Channel is an RF channel of the scan, with the services of its SLT.
type Channel struct {
	Bsid        int
	CyclePeriod time.Duration
	Services    []Service
}

// Service is an SLT entry; SignalingURL is its SvcInetUrl of urlType 1,
// empty when absent.
type Service struct {
	ServiceID    int
	SignalingURL string
}

// Acquirer scans <sinal>/lls.
type Acquirer struct {
	origin *origin.Client
	url    string
	store  *retained.Store
	now    func() time.Time
}

type response struct {
	RfChannels []rfChannel `json:"rfChannels"`
}

type rfChannel struct {
	Bsid           *int    `json:"bsid"`
	LlsCyclePeriod *int    `json:"llsCyclePeriod"`
	Tables         []table `json:"tables"`
}

type table struct {
	ID      *int            `json:"LLS_table_id"`
	Version *int            `json:"LLS_table_version"`
	SLT     json.RawMessage `json:"SLT"`
	BAMT    json.RawMessage `json:"BAMT"`
}

type slt struct {
	Service []struct {
		ServiceID  *int `json:"serviceId"`
		SvcInetURL []struct {
			URLType int    `json:"urlType"`
			Value   string `json:"value"`
		} `json:"SvcInetUrl"`
	} `json:"Service"`
}

type scanMarker struct {
	CompletedAt string `json:"completedAt"`
	RfChannels  []int  `json:"rfChannels"`
}

// ATSC A/331, 6.1 (Table 6.1) and ABNT NBR 25602, 8.2.
const (
	tableIDSLT  = 1
	tableIDBAMT = 0x90
)

// ATSC A/331, 6.3 (Table 6.2): urlType of the Signaling Server.
const signalingServerURLType = 1

const llsPath = "lls"

// New gives an Acquirer of the LLS route under signalingBase.
func New(client *origin.Client, signalingBase *url.URL, store *retained.Store, now func() time.Time) *Acquirer {
	return &Acquirer{
		origin: client,
		url:    signalingBase.JoinPath(llsPath).String(),
		store:  store,
		now:    now,
	}
}

// Scan fetches the tables, publishes those whose LLS_table_version changed,
// clears the topics of tables no longer present and publishes the scan
// marker. A failure of the origin publishes nothing.
func (a *Acquirer) Scan(ctx context.Context) ([]Channel, error) {
	fetched, err := a.origin.Get(ctx, a.url)
	if err != nil {
		return nil, err
	}
	if fetched.Status != http.StatusOK {
		return nil, fmt.Errorf("GET %s answered %d", a.url, fetched.Status)
	}
	var decoded response
	if err := json.Unmarshal(fetched.Body, &decoded); err != nil {
		return nil, fmt.Errorf("GET %s: %w", a.url, err)
	}
	channels := []Channel{}
	present := map[string]bool{}
	for _, rf := range decoded.RfChannels {
		channel, err := a.channel(ctx, rf, present)
		if err != nil {
			slog.Error("RF channel ignored", "error", err)
			continue
		}
		channels = append(channels, channel)
	}
	for _, topic := range a.store.Topics(topics.LLSPrefix()) {
		if !present[topic] {
			a.clear(ctx, topic)
		}
	}
	a.publishMarker(ctx, channels)
	return channels, nil
}

// Publishes the tables of one RF channel and marks their topics present.
func (a *Acquirer) channel(ctx context.Context, rf rfChannel, present map[string]bool) (Channel, error) {
	if rf.Bsid == nil || rf.LlsCyclePeriod == nil || *rf.LlsCyclePeriod <= 0 {
		return Channel{}, fmt.Errorf("bsid or llsCyclePeriod missing")
	}
	channel := Channel{
		Bsid:        *rf.Bsid,
		CyclePeriod: time.Duration(*rf.LlsCyclePeriod) * time.Millisecond,
		Services:    []Service{},
	}
	for _, entry := range rf.Tables {
		if entry.ID == nil || entry.Version == nil {
			slog.Error("LLS table ignored: LLS_table_id or LLS_table_version missing", "bsid", channel.Bsid)
			continue
		}
		name, content := tableContent(entry)
		if name == "" {
			continue
		}
		if content == nil {
			slog.Error("LLS table ignored: no content", "bsid", channel.Bsid, "table", name)
			continue
		}
		topic := topics.LLS(channel.Bsid, name)
		present[topic] = true
		a.publish(ctx, topic, strconv.Itoa(*entry.Version), content)
		if name == topics.SLT {
			services, err := sltServices(content)
			if err != nil {
				slog.Error("SLT services not read", "bsid", channel.Bsid, "error", err)
				continue
			}
			channel.Services = services
		}
	}
	return channel, nil
}

// Publishes the scan marker on <tl>/scan, every scan.
func (a *Acquirer) publishMarker(ctx context.Context, channels []Channel) {
	marker := scanMarker{
		CompletedAt: a.now().UTC().Format(time.RFC3339Nano),
		RfChannels:  []int{},
	}
	for _, channel := range channels {
		marker.RfChannels = append(marker.RfChannels, channel.Bsid)
	}
	slices.Sort(marker.RfChannels)
	payload, err := json.Marshal(marker)
	if err != nil {
		slog.Error("scan marker not encoded", "error", err)
		return
	}
	a.publish(ctx, topics.Scan(), marker.CompletedAt, payload)
}

func (a *Acquirer) publish(ctx context.Context, topic string, version string, payload []byte) {
	published, err := a.store.Publish(ctx, topic, version, payload)
	if err != nil {
		slog.Error("not published", "topic", topic, "version", version, "error", err)
		return
	}
	if published {
		slog.Debug("published", "topic", topic, "version", version)
	}
}

func (a *Acquirer) clear(ctx context.Context, topic string) {
	cleared, err := a.store.Clear(ctx, topic)
	if err != nil {
		slog.Error("not cleared", "topic", topic, "error", err)
		return
	}
	if cleared {
		slog.Info("cleared", "topic", topic)
	}
}

// The topic name and the content of a table; an empty name for an unknown
// LLS_table_id, which is ignored.
func tableContent(entry table) (string, json.RawMessage) {
	switch *entry.ID {
	case tableIDSLT:
		return topics.SLT, entry.SLT
	case tableIDBAMT:
		return topics.BAMT, entry.BAMT
	default:
		return "", nil
	}
}

func sltServices(content json.RawMessage) ([]Service, error) {
	var decoded slt
	if err := json.Unmarshal(content, &decoded); err != nil {
		return nil, err
	}
	services := []Service{}
	for _, entry := range decoded.Service {
		if entry.ServiceID == nil {
			return nil, fmt.Errorf("Service without serviceId")
		}
		service := Service{ServiceID: *entry.ServiceID}
		for _, inetURL := range entry.SvcInetURL {
			if inetURL.URLType == signalingServerURLType {
				service.SignalingURL = inetURL.Value
			}
		}
		services = append(services, service)
	}
	return services, nil
}
