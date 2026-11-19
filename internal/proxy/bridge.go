package proxy

import (
	"bytes"
	_ "embed"
	"mime"
	"net/http"
	"strconv"
)

// The path of the bridge under every application origin, and the element
// that loads it.
const (
	bridgePath        = "/.tv30/bridge.js"
	bridgeElement     = `<script src="` + bridgePath + `"></script>`
	bridgeContentType = "text/javascript; charset=utf-8"
	htmlType          = "text/html"
)

// The bridge of the platform: the keys and the media events sent by the
// platform, dispatched on the document of the application.
//
//go:embed bridge.js
var bridgeScript []byte

// The bridge itself, under any application origin.
func serveBridge(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Set("Content-Type", bridgeContentType)
	header.Set("Cache-Control", "no-cache")
	header.Set("Content-Length", strconv.Itoa(len(bridgeScript)))
	writeBody(w, r, http.StatusOK, bridgeScript)
}

// An HTML document with the element of the bridge before </head>; without
// it, before <body; without either, at the end. Any other type unchanged.
func withBridge(contentType string, body []byte) []byte {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != htmlType {
		return body
	}
	at := indexFold(body, "</head>")
	if at < 0 {
		at = indexFold(body, "<body")
	}
	if at < 0 {
		at = len(body)
	}
	injected := make([]byte, 0, len(body)+len(bridgeElement))
	injected = append(injected, body[:at]...)
	injected = append(injected, bridgeElement...)
	return append(injected, body[at:]...)
}

// The first index of an ASCII pattern in body, ignoring case; -1 without it.
func indexFold(body []byte, pattern string) int {
	for at := 0; at+len(pattern) <= len(body); at++ {
		if bytes.EqualFold(body[at:at+len(pattern)], []byte(pattern)) {
			return at
		}
	}
	return -1
}
