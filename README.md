# aop-cc

Common Core component of the TV 3.0 AoP testbed platform. It realizes the
units `aop-transport-layer-metadata-lls` and `aop-transport-layer-metadata-sls`:
it acquires the transport layer signaling the broadcaster side serves over
HTTP and distributes it inside the platform as retained MQTT messages. It also
realizes `aop-transport-layer-object-proxy`: it serves the broadcast objects of
each service with a carousel, subject to the passes of the carousel, and the
origin of each Application Context ID. And it realizes
`aop-application-lifecycle-manager`: by the `@controlCode` of the BALD of the
tuned service, it decides which broadcaster application replaces the Bootstrap
Application in the graphics layer.

## Packages

| Package | Does |
|---|---|
| `internal/lls` | scan of `<signaling>/lls`; one topic per LLS table and the scan marker |
| `internal/sls` | SLS package (`multipart/related`): envelope first, one topic per fragment |
| `internal/acquisition` | the cycle: scan, then emission descriptor and SLS of each service |
| `internal/proxy` | the object proxy: service family, application family, object events |
| `internal/carousel` | S-TSID and descriptor joined; passes, expiry, templates, USBD, entry package |
| `internal/lifecycle` | the application lifecycle manager: BALD, launch, end, associated origin |
| `internal/storage` | the writes to Redis, only through the functions of the `tv30` library |
| `internal/emission` | the emission descriptor and the count of the signaling channel passes |
| `internal/retained` | last version published on each topic, in memory |
| `internal/broker` | MQTT 5 publication, QoS 1, retained; subscription |
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
   - Without a descriptor, `GET` of the `SvcInetUrl` of `urlType` 1, the
     Signaling Server. Each fragment of the envelope whose `version` changed
     is published on `tlm/sls/{serviceId}/{metadataURI}`; a fragment gone from
     the envelope is cleared.
   - With a descriptor, the SLS comes instead from the signaling channel of
     the object proxy, `GET /svc/{serviceId}/sls`, at each end of a pass:
     `cycleEpoch + k × signaling.cyclePeriod`.
3. A service no longer in any SLT has its SLS and descriptor topics cleared.

## Object proxy

It subscribes to `tlm/emission/+`, `tlm/sls/+/stsid`, `tlm/sls/+/usbd` and
`tlm/tuned`. With the descriptor of a service it serves the signaling channel;
with the S-TSID too, the other objects. Only `GET` and `HEAD`; any other method
is `405`. The body of an error says the reason in text.

**Service family**, `/svc/{serviceId}/{path}` on any host but a context origin:

| Request | Answer |
|---|---|
| `sls`, after the first pass of the signaling count | `200`: the package of `signaling.url`, unchanged, with `Last-Modified` at the end of the last pass |
| `sls`, before it | `404` |
| a service without descriptor | `404` |
| before the S-TSID | `503` |
| a URL that only the unicast `BasePattern` of the USBD matches best | the origin, with its headers |
| a service other than the tuned one | `404` |
| a file of the S-TSID | `200` while available, `404` otherwise |
| a path of a `fileTemplate` (TOI of at least 1) | the origin decides: `200` or its `404` |
| anything else | `404`, without a request to the origin |

**Application family**, any path on `http://ctx-<h>.<APPS_DOMAIN>:<port>`, `<h>`
being the first 16 hexadecimal digits of the SHA-256 of the `appContextId`: a
file of the S-TSID of the tuned service, or a part of one of its entry packages
(`multipart/related`), whose `appContextIdList` has that `appContextId`.

A file is available after its first complete pass begun at or after
`tunedAt`, until `maxExpiresDelta` from the start of that pass; a part, as its
package. Broadcast answers carry `Content-Type` of the S-TSID (or of the part),
`Last-Modified` of the descriptor, `Expires` with `maxExpiresDelta`, and
`Access-Control-Allow-Origin: *`; `If-Modified-Since` at or after
`Last-Modified` gives `304`. The origin failing a declared object gives `502`,
logged.

**Events**, not retained, for the files and parts of the tuned service:
`tlm/object/{serviceId}/available` at the end of a pass after which the object
became available (or with a new `lastModified`), and
`tlm/object/{serviceId}/expired` when it stops being available. A change of
tuning emits no `expired`.

## Application lifecycle manager

It subscribes to `tlm/tuned`, `tlm/lls/+/slt` (the `globalServiceId` of each
`serviceId`), `tlm/sls/+/bald`, `tlm/object/+/available`,
`tlm/object/+/expired` and `aop/application-lifecycle-manager/end-request`.
Only `TV30-Ginga-HTML5` entries inside `validFrom` and `validUntil` count; an
entry outside them counts as not in the BALD.

| BALD of the tuned service | Effect |
|---|---|
| an entry becomes `AUTOSTART` (or has no `controlCode`) | it is launched, ending the running one; of several at once, the last of the BALD |
| the running entry becomes `DESTROY`, `KILL` or `STORED_REMOVE`, or leaves the BALD | it ends and the Bootstrap Application returns |
| `PRESENT` or `STORED_PRESENT` | listed in `aop/application-lifecycle-manager/present-applications` |

The `bbandEntryPointUrl` comes first; the `bcastEntryPointUrl` opens on the
context origin and waits for the `available` event of its `contentLocation`.
At a launch, the origin goes to `origins:associated` and the application to
`session:current-app`, then `aop/application-lifecycle-manager/application-state`
gets `started` and the retained
`aop/application-lifecycle-manager/graphics-application` gets
`{serviceContextId, appId, origin, url}`. At the end, `ended`, both removed
from Redis and the empty `graphics-application`. An end request of the running
application ends it, and it is not launched again while its `controlCode`
stays the same. A change of tuning ends the running application. At start,
`origins:associated` and `session:current-app` are cleared.

## Messages

Every message of the acquisition is retained; clearing is the empty retained message. A failure
of the origin publishes and clears nothing. A fragment that is not JSON (or,
for the MPD, well-formed XML) is not published. The schema of each topic is
the broker's: a refused message is logged and not sent again until its
version changes. After a restart everything is published once more; the
acquisition subscribes to `tlm/lls/#`, `tlm/sls/#` and `tlm/emission/+`, so
that a retained topic published before the restart and no longer declared is
cleared by the next scan.

## Environment

| Variable | Meaning |
|---|---|
| `MQTT_HOST` | broker, `host` or `host:port` (default port 1883) |
| `REDIS_HOST` | Redis with the `tv30` library, `host` or `host:port` (default port 6379) |
| `SIGNALING_BASE_URL` | `<signaling>` of the broadcaster side, absolute, ending in `/` |
| `OBJECT_PROXY_PORT` | port of the object proxy; the acquisition reaches it on `127.0.0.1` |
| `OBJECT_PROXY_PUBLIC_PORT` | the same port as the browser reaches it, in the context origins and event URLs |
| `APPS_DOMAIN` | domain of the context origins, e.g. `apps.localhost` |
| `LOG_LEVEL` | `ERROR`, `INFO` (default) or `DEBUG` |

```sh
MQTT_HOST=localhost REDIS_HOST=localhost SIGNALING_BASE_URL=http://localhost:8082/signaling/ OBJECT_PROXY_PORT=8081 \
    OBJECT_PROXY_PUBLIC_PORT=8081 APPS_DOMAIN=apps.localhost go run ./cmd/aop-cc
```

## Container

```sh
docker build -t tv30-aop-cc .
docker run --rm -e MQTT_HOST=mqtt-broker -e REDIS_HOST=redis -e SIGNALING_BASE_URL=http://bcast-sim:8082/signaling/ \
    -e OBJECT_PROXY_PORT=8081 -e OBJECT_PROXY_PUBLIC_PORT=8081 -e APPS_DOMAIN=apps.localhost tv30-aop-cc
```

## Tests

```sh
go vet ./...
go test ./...
```

The contract of `internal/storage` against the real functions runs with
`TV30_REDIS_TEST=host:port`; `infra/redis/test/run.sh contract` starts the
Redis and runs it.

## License

Apache License 2.0; see `LICENSE`.
