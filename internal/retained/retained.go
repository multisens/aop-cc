// Package retained keeps, in memory, the version last published on each
// retained topic, so that only what changed is published again
// (implementation document, 6.3, "Versão em memória").
package retained

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/multisens/aop-cc/internal/broker"
)

// Store publishes through a broker.Publisher and remembers versions.
type Store struct {
	publisher broker.Publisher
	versions  map[string]string
}

// New gives an empty Store: after a restart everything is published once.
func New(publisher broker.Publisher) *Store {
	return &Store{publisher: publisher, versions: map[string]string{}}
}

// Publish sends payload when version differs from the last one of the
// topic. A refusal by the broker also records the version, so that the same
// content is not sent again; any other failure records nothing.
func (s *Store) Publish(ctx context.Context, topic string, version string, payload []byte) (bool, error) {
	last, known := s.versions[topic]
	if known && last == version {
		return false, nil
	}
	err := s.publisher.Publish(ctx, topic, payload)
	if err == nil || errors.Is(err, broker.ErrRefused) {
		s.versions[topic] = version
	}
	return err == nil, err
}

// Clear sends the empty retained message on a topic published before.
func (s *Store) Clear(ctx context.Context, topic string) (bool, error) {
	if _, known := s.versions[topic]; !known {
		return false, nil
	}
	if err := s.publisher.Publish(ctx, topic, nil); err != nil {
		return false, err
	}
	delete(s.versions, topic)
	return true, nil
}

// Topics lists, sorted, the published topics that start with prefix.
func (s *Store) Topics(prefix string) []string {
	found := []string{}
	for topic := range s.versions {
		if strings.HasPrefix(topic, prefix) {
			found = append(found, topic)
		}
	}
	sort.Strings(found)
	return found
}
