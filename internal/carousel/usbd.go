package carousel

import (
	"encoding/json"
	"fmt"
	"strings"
)

// BasePatterns are the BasePattern of the USBD (ATSC A/331, 7.1.3.1).
type BasePatterns struct {
	Broadcast []string
	Unicast   []string
}

type wireUsbd struct {
	UserServiceDescription struct {
		DeliveryMethod []struct {
			BroadcastAppService []struct {
				BasePattern []string `json:"BasePattern"`
			} `json:"BroadcastAppService"`
			UnicastAppService []struct {
				BasePattern []string `json:"BasePattern"`
			} `json:"UnicastAppService"`
		} `json:"DeliveryMethod"`
	} `json:"UserServiceDescription"`
}

// DecodeUsbd reads the BasePattern of every DeliveryMethod.
func DecodeUsbd(body []byte) (BasePatterns, error) {
	var usbd wireUsbd
	if err := json.Unmarshal(body, &usbd); err != nil {
		return BasePatterns{}, fmt.Errorf("USBD: %w", err)
	}
	patterns := BasePatterns{Broadcast: []string{}, Unicast: []string{}}
	for _, method := range usbd.UserServiceDescription.DeliveryMethod {
		for _, service := range method.BroadcastAppService {
			patterns.Broadcast = append(patterns.Broadcast, service.BasePattern...)
		}
		for _, service := range method.UnicastAppService {
			patterns.Unicast = append(patterns.Unicast, service.BasePattern...)
		}
	}
	return patterns, nil
}

// IsBroadband resolves a URL by the BasePattern of the USBD: only with both
// kinds of pattern; the longest pattern contained in the URL wins, and a
// tie, or no match, is broadcast.
func (p BasePatterns) IsBroadband(url string) bool {
	if len(p.Broadcast) == 0 || len(p.Unicast) == 0 {
		return false
	}
	return longestMatch(p.Unicast, url) > longestMatch(p.Broadcast, url)
}

// The length of the longest pattern contained in url; 0 without any.
func longestMatch(patterns []string, url string) int {
	longest := 0
	for _, pattern := range patterns {
		if strings.Contains(url, pattern) && len(pattern) > longest {
			longest = len(pattern)
		}
	}
	return longest
}
