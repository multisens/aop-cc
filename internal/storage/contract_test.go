package storage

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/multisens/aop-cc/internal/proxy"
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
		proxy.Origins{AppsDomain: "apps.localhost", PublicPort: 8081}.ContextOrigin("urn:tv30:app-context:1"),
		proxy.Origins{AppsDomain: "tv30.example", PublicPort: 80}.ContextOrigin("Ação Ü!"),
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
}
