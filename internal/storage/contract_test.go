package storage

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// Runs against a Redis with the tv30 library loaded, at TV30_REDIS_TEST
// (host:port); infra/redis/test/run.sh contract starts one.
func TestContractFunctions(t *testing.T) {
	address := os.Getenv("TV30_REDIS_TEST")
	if address == "" {
		t.Skip("TV30_REDIS_TEST not set")
	}
	ctx := context.Background()
	store := New(address)
	defer store.Close()

	origins := []string{
		"http://ctx-0123456789abcdef.apps.localhost:8081",
		"http://ctx-fedcba9876543210.tv30.example",
	}
	for _, origin := range origins {
		if err := store.OriginRegister(ctx, origin, "urn:tv30:service:1"); err != nil {
			t.Fatalf("origin %s refused: %v", origin, err)
		}
	}
	err := store.OriginRegister(ctx, "http://Ctx.apps.localhost", "urn:tv30:service:1")
	if err == nil || !strings.Contains(err.Error(), "TV30SCHEMA") {
		t.Fatalf("origin out of the schema accepted: %v", err)
	}
	if err := store.OriginRemove(ctx, origins[0]); err != nil {
		t.Fatal(err)
	}

	app := App{ServiceContextID: "urn:tv30:service:1", AppID: "urn:tv30:app:1", Origin: origins[1]}
	if err := store.CurrentApp(ctx, app); err != nil {
		t.Fatal(err)
	}
	err = store.CurrentApp(ctx, App{ServiceContextID: "urn:tv30:service:1", AppID: "", Origin: origins[1]})
	if err == nil || !strings.Contains(err.Error(), "TV30SCHEMA") {
		t.Fatalf("empty appId accepted: %v", err)
	}
	if err := store.ClearCurrentApp(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.OriginsClear(ctx); err != nil {
		t.Fatal(err)
	}

	const appContextID = "urn:tv30:ctx:contract"
	file := CachedFile{ContentType: "text/html", LastModified: time.UnixMilli(1763000000000), Body: []byte("<p>\x00\xff</p>")}
	now := time.UnixMilli(1763000001000)
	stored, evicted, err := store.CachePut(ctx, appContextID, "urn:tv30:service:1", "index.html", file, now,
		Quota{ByService: 1000, Total: 1000})
	if err != nil || !stored || len(evicted) != 0 {
		t.Fatalf("put: %v %v %v", stored, evicted, err)
	}
	stored, _, err = store.CachePut(ctx, appContextID, "urn:tv30:service:1", "big.bin", file, now,
		Quota{ByService: 10, Total: 1000})
	if err != nil || stored {
		t.Fatalf("over the quota: %v %v", stored, err)
	}
	read, found, err := store.CacheFile(ctx, appContextID, "index.html")
	if err != nil || !found || string(read.Body) != string(file.Body) || !read.LastModified.Equal(file.LastModified) ||
		read.ContentType != file.ContentType {
		t.Fatalf("read: %+v %v %v", read, found, err)
	}
	if _, found, err := store.CacheFile(ctx, appContextID, "nada"); err != nil || found {
		t.Fatalf("absent file: %v %v", found, err)
	}
	if paths, err := store.CachePaths(ctx, appContextID); err != nil || len(paths) != 1 {
		t.Fatalf("paths: %v %v", paths, err)
	}
	if err := store.CacheTouch(ctx, appContextID, now); err != nil {
		t.Fatal(err)
	}
	if removed, err := store.CacheClearBefore(ctx, appContextID, file.LastModified); err != nil || removed != 0 {
		t.Fatalf("clear before: %d %v", removed, err)
	}
	if err := store.CacheRemove(ctx, appContextID); err != nil {
		t.Fatal(err)
	}
}
