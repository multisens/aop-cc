package carousel

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"strings"
)

// Part is a part of an entry package.
type Part struct {
	ContentLocation string
	ContentType     string
	Body            []byte
}

// Parts reads an entry package (ATSC A/331, A.3.3.4). A part whose
// Content-Location is not relative, or starts with '/' or '..', is left out
// and logged (5.5, "Indexação de pacote de entrada").
func Parts(contentType string, body []byte) ([]Part, error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}
	if mediaType != PackageType || params["boundary"] == "" {
		return nil, fmt.Errorf("Content-Type '%s' is not %s with a boundary", contentType, PackageType)
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	parts := []Part{}
	for {
		next, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		content, err := io.ReadAll(next)
		if err != nil {
			return nil, err
		}
		location := next.Header.Get("Content-Location")
		if !relativeLocation(location) {
			slog.Error("entry package part left out: Content-Location not relative", "contentLocation", location)
			continue
		}
		parts = append(parts, Part{
			ContentLocation: location,
			ContentType:     next.Header.Get("Content-Type"),
			Body:            content,
		})
	}
	return parts, nil
}

func relativeLocation(location string) bool {
	if location == "" || strings.HasPrefix(location, "/") || strings.HasPrefix(location, "..") {
		return false
	}
	return !strings.Contains(location, "://")
}
