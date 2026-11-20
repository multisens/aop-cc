# aop-cc

Common Core component of the TV 3.0 AoP testbed platform. It realizes the
units `aop-transport-layer-metadata-lls` and `aop-transport-layer-metadata-sls`:
it acquires the transport layer signaling the broadcaster side serves over
HTTP and distributes it inside the platform as retained MQTT messages. It also
realizes `aop-transport-layer-object-proxy`: it serves the broadcast objects of
each service with a carousel, subject to the passes of the carousel, the
origin of each Application Context ID and that of its broadband application. It realizes
`aop-application-context-cache`: it keeps the files of the stored
applications. And it realizes `aop-application-lifecycle-manager`: by the `@controlCode` of the BALD of the
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
| `internal/cache` | the Application Context Cache: admission, removal, routes of reading |
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

It subscribes to `tlm/emission/+`, `tlm/sls/+/stsid`, `tlm/sls/+/usbd`,
`tlm/sls/+/bald` and `tlm/tuned`. With the descriptor of a service it serves the signaling channel;
with the S-TSID too, the other objects. Only `GET` and `HEAD`; any other method
is `405`. The body of an error says the reason in text.

**Service family**, `/svc/{serviceId}/{path}` on any host but an application
origin:

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

**Broadband family**, any path on `http://bb-<h>.<APPS_DOMAIN>:<port>`, the same
`<h>`: passed on to the scheme and host of the `bbandEntryPointUrl` of that
`appContextId` in the BALD of the tuned service, with the path and query of the
request, and answered with the status and headers of the server. Without such an
entry, `404`.

**Bridge**, `/.tv30/bridge.js` on both application families. Every HTML
document those families serve (`200`) carries `<script src="/.tv30/bridge.js">`
before `</head>`, or before `<body` without it, or at the end; the `ETag` of a
broadband server does not go with a body so changed. The bridge dispatches on
the document the keys, `timeupdate` and `stream_event` the platform sends by
`postMessage` (`{type: 'tv30-aop-event', event, key | detail}`), and sends
`{type: 'tv30-aop-back'}` to the platform for a `BrowserBack` no handler
cancels.

**Events**, not retained, for the files and parts of the tuned service:
`tlm/object/{serviceId}/available` at the end of a pass after which the object
became available (or with a new `lastModified`), and
`tlm/object/{serviceId}/expired` when it stops being available. A change of
tuning emits no `expired`.

## Application Context Cache

It subscribes to `tlm/tuned`, `tlm/lls/+/slt`, `tlm/sls/+/bald`,
`tlm/sls/+/stsid`, `tlm/emission/+` and `tlm/object/+/available`. An object of
the tuned service enters the cache when its `appContextIdList` has the
`appContextId` of a BALD entry with `STORED_AUTOSTART` or `STORED_PRESENT`,
unless its `Last-Modified` is before the `clearAppContextCacheDate` of that
entry. The cache fetches it from the object proxy, by the URL of the event,
and keeps its body, `Content-Type` and `Last-Modified` in Redis
(`cache:meta:{appContextId}` and `cache:body:{appContextId}`).

| BALD of the tuned service | Effect |
|---|---|
| `STORED_REMOVE` | the files of the `appContextId` leave the cache |
| `clearAppContextCacheDate` | the files with `Last-Modified` before it leave the cache |

The quota is by DTV service context: a file over `CACHE_QUOTA_BYTES` is not
stored. Over `CACHE_TOTAL_BYTES`, the DTV service context with the oldest last
access, other than the one being written, leaves the cache whole. Every read
counts as an access.

Routes, on `CACHE_PORT`:

| Route | Answer |
|---|---|
| `GET /app/{appContextId}/{path}` | the file, with its stored `Content-Type` and `Last-Modified`; `404` if not there |
| `GET /app/{appContextId}` | `{ complete, files }`: `complete` when every object of the `appContextId` in the S-TSID of the tuned service, the parts of its entry packages included, is in the cache |

The object proxy reads a file from here when the carousel does not offer it:
not passed since the tuning, expired, or not in the carousel. An entry package
is served with the `Content-Type` of the S-TSID and the parameters (`boundary`,
`type`) of the origin.

## Application lifecycle manager

It subscribes to `tlm/tuned`, `tlm/lls/+/slt` (the `globalServiceId` of each
`serviceId`), `tlm/sls/+/bald`, `tlm/object/+/available`,
`tlm/object/+/expired` and `aop/application-lifecycle-manager/end-request`.
Only `TV30-Ginga-HTML5` entries inside `validFrom` and `validUntil` count; an
entry outside them counts as not in the BALD.

| BALD of the tuned service | Effect |
|---|---|
| an entry becomes `AUTOSTART` (or has no `controlCode`) | it is launched, ending the running one; of several at once, the last of the BALD |
| an entry becomes `STORED_AUTOSTART` | the same, once the cache has it complete |
| the running entry becomes `DESTROY`, `KILL` or `STORED_REMOVE`, or leaves the BALD | it ends and the Bootstrap Application returns |
| `PRESENT` or `STORED_PRESENT` | listed in `aop/application-lifecycle-manager/present-applications` |

The `bbandEntryPointUrl` comes first, on the broadband origin with its path,
query and fragment; the `bcastEntryPointUrl` opens on the context origin and
waits for the `available` event of its `contentLocation`.
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
| `OBJECT_PROXY_PUBLIC_PORT` | the same port as the browser reaches it, in the application origins and event URLs |
| `APPS_DOMAIN` | domain of the application origins, e.g. `apps.localhost` |
| `CACHE_PORT` | port of the routes of the Application Context Cache, reached on `127.0.0.1` |
| `CACHE_QUOTA_BYTES` | quota of the cache by DTV service context (default 112000000) |
| `CACHE_TOTAL_BYTES` | total of the cache (default 500000000) |
| `LOG_LEVEL` | `ERROR`, `INFO` (default) or `DEBUG` |

```sh
MQTT_HOST=localhost REDIS_HOST=localhost SIGNALING_BASE_URL=http://localhost:8082/signaling/ OBJECT_PROXY_PORT=8081 \
    OBJECT_PROXY_PUBLIC_PORT=8081 APPS_DOMAIN=apps.localhost CACHE_PORT=8083 go run ./cmd/aop-cc
```

## Container

```sh
docker build -t tv30-aop-cc .
docker run --rm -e MQTT_HOST=mqtt-broker -e REDIS_HOST=redis -e SIGNALING_BASE_URL=http://bcast-sim:8082/signaling/ \
    -e OBJECT_PROXY_PORT=8081 -e OBJECT_PROXY_PUBLIC_PORT=8081 -e APPS_DOMAIN=apps.localhost -e CACHE_PORT=8082 tv30-aop-cc
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
