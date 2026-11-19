package emission

import (
	"testing"
	"time"
)

const body = `{"serviceId":1001,"cycleEpoch":"2026-01-01T00:00:00Z",` +
	`"signaling":{"url":"http://emission/signaling/sls/1001","cyclePeriod":3000},"flows":[]}`

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestDecodeReadsTheSignalingChannel(t *testing.T) {
	descriptor, err := Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	expected := Signaling{URL: "http://emission/signaling/sls/1001", CyclePeriod: 3 * time.Second}
	if descriptor.ServiceID != 1001 || !descriptor.CycleEpoch.Equal(epoch) || descriptor.Signaling != expected {
		t.Fatalf("decoded %+v", descriptor)
	}
}

func TestDecodeRefusesAMissingOrNonPositivePeriod(t *testing.T) {
	for _, broken := range []string{
		`{"serviceId":1001,"cycleEpoch":"2026-01-01T00:00:00Z","signaling":{"url":"http://x"}}`,
		`{"serviceId":1001,"cycleEpoch":"2026-01-01T00:00:00Z","signaling":{"url":"http://x","cyclePeriod":0}}`,
		`{"serviceId":1001,"cycleEpoch":"yesterday","signaling":{"url":"http://x","cyclePeriod":3000}}`,
		`{"serviceId":1001,"signaling":{"url":"http://x","cyclePeriod":3000}}`,
	} {
		if _, err := Decode([]byte(broken)); err == nil {
			t.Fatalf("decoded %s", broken)
		}
	}
}

func TestSignalingPassEnds(t *testing.T) {
	descriptor, err := Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		now    time.Duration
		passed bool
		last   time.Duration
		next   time.Duration
	}{
		{now: -time.Hour, passed: false, next: 3 * time.Second},
		{now: 0, passed: false, next: 3 * time.Second},
		{now: 2999 * time.Millisecond, passed: false, next: 3 * time.Second},
		{now: 3 * time.Second, passed: true, last: 3 * time.Second, next: 6 * time.Second},
		{now: 7500 * time.Millisecond, passed: true, last: 6 * time.Second, next: 9 * time.Second},
	}
	for _, entry := range cases {
		now := epoch.Add(entry.now)
		last, passed := descriptor.LastSignalingPassEnd(now)
		if passed != entry.passed || (passed && !last.Equal(epoch.Add(entry.last))) {
			t.Fatalf("at %v: last pass end %v (passed: %v)", entry.now, last, passed)
		}
		if next := descriptor.NextSignalingPassEnd(now); !next.Equal(epoch.Add(entry.next)) {
			t.Fatalf("at %v: next pass end %v", entry.now, next)
		}
	}
}
