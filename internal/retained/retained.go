// Package retained keeps, in memory, the version last published on each
// retained topic, so that only what changed is published again, and the topics
// found retained on the broker at startup, so that they can be cleared.
package retained

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/multisens/aop-cc/internal/broker"
)

// Store publishes through a broker.Publisher and remembers versions.
type Store struct {
	publisher broker.Publisher
	mu        sync.Mutex
	versions  map[string]string
}

// The version of a topic found retained on the broker and not published by
// this instance: no published version equals it.
const adoptedVersion = ""

// New gives an empty Store: after a restart everything is published once.
func New(publisher broker.Publisher) *Store {
	return &Store{publisher: publisher, versions: map[string]string{}}
}

// Adopt records a retained message received from the broker on a topic
// this store does not know, so that Clear and Topics reach it; an empty
// payload is not a retained message and is ignored.
func (s *Store) Adopt(topic string, payload []byte) {
	if len(payload) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, known := s.versions[topic]; known {
		return
	}
	s.versions[topic] = adoptedVersion
}

// Publish sends payload when version differs from the last one of the
// topic. A refusal by the broker also records the version, so that the same
// content is not sent again; any other failure records nothing.
func (s *Store) Publish(ctx context.Context, topic string, version string, payload []byte) (bool, error) {
	last, known := s.version(topic)
	if known && last == version {
		return false, nil
	}
	err := s.publisher.Publish(ctx, topic, payload)
	if err == nil || errors.Is(err, broker.ErrRefused) {
		s.setVersion(topic, version)
	}
	return err == nil, err
}

// Clear sends the empty retained message on a topic published before or
// adopted.
func (s *Store) Clear(ctx context.Context, topic string) (bool, error) {
	if _, known := s.version(topic); !known {
		return false, nil
	}
	if err := s.publisher.Publish(ctx, topic, nil); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.versions, topic)
	return true, nil
}

// Topics lists, sorted, the published or adopted topics that start with
// prefix.
func (s *Store) Topics(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := []string{}
	for topic := range s.versions {
		if strings.HasPrefix(topic, prefix) {
			found = append(found, topic)
		}
	}
	sort.Strings(found)
	return found
}

func (s *Store) version(topic string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	version, known := s.versions[topic]
	return version, known
}

func (s *Store) setVersion(topic string, version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions[topic] = version
}
