package acquisition

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFirstCyclePublishesEveryTableAndFragment(t *testing.T) {
	_, publisher, _, acquisition := setup(t)
	acquisition.Cycle(context.Background())

	assertTopics(t, publisher.topics(), []string{
		"tlm/lls/10/slt", "tlm/lls/10/bamt", "tlm/scan", "tlm/emission/1001",
		"tlm/sls/1001/usbd", "tlm/sls/1001/mpd", "tlm/sls/1001/bald",
	})
	if mpd, _ := publisher.last("tlm/sls/1001/mpd"); !strings.HasPrefix(mpd, "<MPD") {
		t.Fatalf("MPD published as %q", mpd)
	}
	scan, _ := publisher.last("tlm/scan")
	var marker struct {
		CompletedAt string `json:"completedAt"`
		RfChannels  []int  `json:"rfChannels"`
	}
	err := json.Unmarshal([]byte(scan), &marker)
	if err != nil || marker.CompletedAt == "" || len(marker.RfChannels) != 1 || marker.RfChannels[0] != 10 {
		t.Fatalf("scan marker %q", scan)
	}
}

func TestUnchangedVersionsAreNotPublishedAgain(t *testing.T) {
	side, publisher, clock, acquisition := setup(t)
	acquisition.Cycle(context.Background())
	publisher.reset()

	clock.advance(5 * time.Second)
	side.setPackage(1001, slsPackage(1, 2, 1))
	acquisition.Cycle(context.Background())
	assertTopics(t, publisher.topics(), []string{"tlm/scan", "tlm/sls/1001/mpd"})
}

func TestOriginDownTouchesNoTopic(t *testing.T) {
	side, publisher, clock, acquisition := setup(t)
	acquisition.Cycle(context.Background())
	publisher.reset()

	side.setDown(true)
	clock.advance(5 * time.Second)
	acquisition.Cycle(context.Background())
	assertTopics(t, publisher.topics(), []string{})
}

func TestRemovedServiceClearsItsTopics(t *testing.T) {
	side, publisher, clock, acquisition := setup(t)
	acquisition.Cycle(context.Background())
	publisher.reset()

	side.setChannels(channel(10, []int{}, false, defaultPeriodMs))
	clock.advance(5 * time.Second)
	acquisition.Cycle(context.Background())
	for _, topic := range []string{"tlm/lls/10/bamt", "tlm/emission/1001", "tlm/sls/1001/usbd",
		"tlm/sls/1001/mpd", "tlm/sls/1001/bald"} {
		assertCleared(t, publisher, topic)
	}
}

func TestRemovedChannelClearsBothTables(t *testing.T) {
	side, publisher, _, acquisition := setup(t)
	acquisition.Cycle(context.Background())
	publisher.reset()

	side.setChannels()
	acquisition.Cycle(context.Background())
	assertCleared(t, publisher, "tlm/lls/10/slt")
	assertCleared(t, publisher, "tlm/lls/10/bamt")
	assertCleared(t, publisher, "tlm/sls/1001/bald")
}

func TestFragmentAbsentFromTheEnvelopeIsCleared(t *testing.T) {
	side, publisher, clock, acquisition := setup(t)
	acquisition.Cycle(context.Background())
	publisher.reset()

	side.setPackage(1001, packageOf(usbdFragment(1), mpdFragment(1)))
	clock.advance(5 * time.Second)
	acquisition.Cycle(context.Background())
	assertCleared(t, publisher, "tlm/sls/1001/bald")
	assertTopics(t, publisher.topics(), []string{"tlm/scan", "tlm/sls/1001/bald"})
}

func TestPackageWithoutTheEnvelopeFirstIsDiscarded(t *testing.T) {
	side, publisher, _, acquisition := setup(t)
	side.setPackage(1001, multipart(part{location: "bald", contentType: jsonType, body: "{}"}))
	acquisition.Cycle(context.Background())
	assertNoSls(t, publisher)
}

func TestFragmentThatDoesNotDecodeIsSkipped(t *testing.T) {
	side, publisher, _, acquisition := setup(t)
	broken := usbdFragment(1)
	broken.body = "{not json"
	side.setPackage(1001, packageOf(broken, mpdFragment(1), baldFragment(1)))
	acquisition.Cycle(context.Background())
	if _, found := publisher.last("tlm/sls/1001/usbd"); found {
		t.Fatalf("usbd published without decoding")
	}
	if _, found := publisher.last("tlm/sls/1001/bald"); !found {
		t.Fatalf("bald not published")
	}
}

func TestRefusedFragmentIsNotSentAgainWithTheSameVersion(t *testing.T) {
	side, publisher, clock, acquisition := setup(t)
	publisher.refuse["tlm/sls/1001/bald"] = true
	acquisition.Cycle(context.Background())
	delete(publisher.refuse, "tlm/sls/1001/bald")
	publisher.reset()

	clock.advance(5 * time.Second)
	acquisition.Cycle(context.Background())
	if _, found := publisher.last("tlm/sls/1001/bald"); found {
		t.Fatalf("refused bald sent again with the same version")
	}
	side.setPackage(1001, slsPackage(1, 1, 2))
	clock.advance(5 * time.Second)
	acquisition.Cycle(context.Background())
	if _, found := publisher.last("tlm/sls/1001/bald"); !found {
		t.Fatalf("bald of a new version not sent")
	}
}

func TestNotYetPublishesNoFragment(t *testing.T) {
	side, publisher, _, acquisition := setup(t)
	side.setPackage(1001, "")
	acquisition.Cycle(context.Background())
	assertNoSls(t, publisher)
}

func TestCyclePeriodIsTheSmallestLlsCyclePeriod(t *testing.T) {
	side, _, _, acquisition := setup(t)
	side.setChannels(channel(10, []int{1001}, true, defaultPeriodMs), channel(11, []int{}, false, 2000))
	if period := acquisition.Cycle(context.Background()); period != 2*time.Second {
		t.Fatalf("period %v", period)
	}
}

func TestSlowerChannelIsAcquiredOnlyWhenDue(t *testing.T) {
	side, publisher, clock, acquisition := setup(t)
	side.setChannels(channel(10, []int{1001}, true, defaultPeriodMs), channel(11, []int{}, false, 1000))
	acquisition.Cycle(context.Background())
	publisher.reset()

	side.setPackage(1001, slsPackage(2, 2, 2))
	clock.advance(time.Second)
	acquisition.Cycle(context.Background())
	if _, found := publisher.last("tlm/sls/1001/usbd"); found {
		t.Fatalf("service of channel 10 acquired before its llsCyclePeriod")
	}
	clock.advance(4 * time.Second)
	acquisition.Cycle(context.Background())
	if _, found := publisher.last("tlm/sls/1001/usbd"); !found {
		t.Fatalf("service of channel 10 not acquired when due")
	}
}

func TestServiceWithoutDescriptorClearsTheTopic(t *testing.T) {
	side, publisher, clock, acquisition := setup(t)
	acquisition.Cycle(context.Background())
	publisher.reset()

	side.setDescriptor(1001, "")
	clock.advance(5 * time.Second)
	acquisition.Cycle(context.Background())
	assertCleared(t, publisher, "tlm/emission/1001")
}
