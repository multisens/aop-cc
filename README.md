# aop-cc

Common Core component of the TV 3.0 AoP testbed platform. It realizes the
units `aop-transport-layer-metadata-lls` and `aop-transport-layer-metadata-sls`:
it acquires the transport layer signaling the broadcaster side serves over
HTTP and distributes it inside the platform as retained MQTT messages.

## Packages

| Package | Does |
|---|---|
| `internal/lls` | scan of `<signaling>/lls`; one topic per LLS table and the scan marker |
| `internal/sls` | SLS package (`multipart/related`): envelope first, one topic per fragment |
| `internal/acquisition` | the cycle: scan, then emission descriptor and SLS of each service |
| `internal/retained` | last version published on each topic, in memory |
| `internal/broker` | MQTT 5 publication, QoS 1, retained |
| `internal/topics` | the topic names |
| `internal/origin` | HTTP fetch from the broadcaster side |
| `internal/config` | the environment, checked at startup |

## Cycle

Every `llsCyclePeriod` (the smallest among the RF channels; 5 s before the
first scan and after a failed one):

1. `GET <signaling>/lls`. Each table whose `LLS_table_version` changed is
   published on `tlm/lls/{bsid}/slt` or `tlm/lls/{bsid}/bamt`; the topic of a
   table that is gone is cleared. Then `tlm/scan` gets
   `{ completedAt, rfChannels }`.
2. For each service of the SLT of each RF channel due by its own
   `llsCyclePeriod`:
   - `GET <signaling>/emission/{serviceId}` goes to `tlm/emission/{serviceId}`
     when its content changes; `404` clears the topic.
   - `GET` of the `SvcInetUrl` of `urlType` 1, the Signaling Server. Each
     fragment of the envelope whose `version` changed is published on
     `tlm/sls/{serviceId}/{metadataURI}`; a fragment gone from the envelope is
     cleared.
3. A service no longer in any SLT has its SLS and descriptor topics cleared.

Every message is retained; clearing is the empty retained message. A failure
of the origin publishes and clears nothing. A fragment that is not JSON (or,
for the MPD, well-formed XML) is not published. The schema of each topic is
the broker's: a refused message is logged and not sent again until its
version changes. After a restart everything is published once more.

## Environment

| Variable | Meaning |
|---|---|
| `MQTT_HOST` | broker, `host` or `host:port` (default port 1883) |
| `SIGNALING_BASE_URL` | `<signaling>` of the broadcaster side, absolute, ending in `/` |
| `LOG_LEVEL` | `ERROR`, `INFO` (default) or `DEBUG` |

```sh
MQTT_HOST=localhost SIGNALING_BASE_URL=http://localhost:8082/signaling/ go run ./cmd/aop-cc
```

## Container

```sh
docker build -t tv30-aop-cc .
docker run --rm -e MQTT_HOST=mqtt-broker -e SIGNALING_BASE_URL=http://bcast-sim:8082/signaling/ tv30-aop-cc
```

## Tests

```sh
go vet ./...
go test ./...
```

## License

Apache License 2.0; see `LICENSE`.
