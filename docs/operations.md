# Operations

This is the policy and troubleshooting companion to the README. Internal
development checkpoints are not documented here.

## What the appliance actually establishes

Frigate documents `camera_fps == 0` as an offline-camera signal. Role-status
MQTT can flap while ffmpeg restarts against an unreachable camera, so this
appliance uses **frame telemetry**, not an isolated role-status message.

A successful HTTP 200 from `/api/stats` is not evidence. Frigate can return
cached snapshots. Freshness is judged from `service.last_updated` and
`service.uptime`. An unchanged producer timestamp is one observation, not six.

ONVIF `GetDeviceInformation` succeeding does not prove `SystemReboot` is
implemented or authorised. Preflight is a read-only check that the endpoint
accepts our credentials. Recovery success is **frames restored in Frigate
after the reboot request**, not a firmware root-cause.

## Eligibility (all must hold)

A reboot is proposed only when **all** of these hold. There is no
best-effort fallback around a failed condition.

* `mode` is `recover`
* the camera’s `recovery` is `onvif`
* the camera is not disabled in Frigate and not flagged `maintenance`
* a healthy baseline has been persisted (60s of continuous frames, once)
* Frigate telemetry and effective configuration are fresh
* watchdog startup grace and Frigate-restart grace have finished
* zero frames persisted for ≥120s, supported by ≥6 distinct fresh snapshots
* exactly one monitored, enabled camera is failing
* every other monitored, enabled camera is known and stably healthy
* at least one healthy peer exists
* no recovery is in flight or in boot grace
* this outage has not already consumed its attempt
* per-camera cooldown (1h runtime) and daily budget (3 / 24h runtime) permit it
* global spacing (5 minutes) permits it
* the state store and exclusive `/data` lock are healthy
* the ONVIF target passes a bounded read-only preflight
* the immediately preceding recheck still supports the action

## Inhibition reasons

`/stats` reports **every** applicable reason, not just the first. Common
codes:

| Code | Meaning |
| ---- | ------- |
| `MODE_OBSERVE` | Running in observe mode |
| `NOT_ARMED` | Camera never delivered a healthy baseline |
| `TELEMETRY_STALE` | Stats are old, future-dated, or cached without an advancing producer timestamp |
| `STARTUP_GRACE_ACTIVE` | Watchdog just started |
| `FRIGATE_RESTART_GRACE_ACTIVE` | Frigate uptime/last_updated moved backwards |
| `MONITORING_INTERRUPTED` | Polling gap; evidence was cleared, limits were not |
| `MULTIPLE_CAMERAS_FAILING` | Two or more enabled cameras have no frames |
| `NO_HEALTHY_PEER` | Single-camera install, or no stably healthy witness |
| `PEER_NOT_HEALTHY` | A non-failing peer is unknown or not yet stable |
| `OUTAGE_ATTEMPT_CONSUMED` | This outage already used its one attempt |
| `COOLDOWN_ACTIVE` | Less than one hour of runtime since the last attempt |
| `BUDGET_EXHAUSTED` | Three attempts in the last 24h of runtime |
| `OPERATION_IN_FLIGHT` | Another recovery is reserved or in boot grace |
| `PREFLIGHT_FAILED` | ONVIF read failed; no reboot was sent |
| `AUTH_LATCHED` | Bad credentials or unsupported reboot; wait for `acknowledge` |
| `STATE_STORE_UNSAFE` | SQLite unusable; recovery inhibited until restart |

## Persistence

SQLite on `/data`. Safety records (baselines, incidents, attempts, budgets)
are never pruned with display history.

Limits use an **accumulated-runtime** clock: only time the process is running
counts. A crash or host reboot can make a cooldown **longer**, never shorter.
Wall-clock jumps and DST cannot expire a limit early.

Camera keys can be renamed: limits follow the ONVIF endpoint identity, not
the YAML key. Removing and re-adding a camera with the same endpoint keeps
its budget.

A second process using the same `/data` exits without contacting cameras.

A corrupt, read-only, full, or incompatible store **never** recreates itself.
Recovery is inhibited with an explicit error. History is not invented in
memory.

## ONVIF outcomes

| Outcome | Meaning | Next |
| ------- | ------- | ---- |
| `ACKNOWLEDGED` | Camera accepted `SystemReboot` | Boot grace, then wait for frames |
| `OUTCOME_UNKNOWN` | Disconnect/timeout after possible delivery | **No retry.** Latch. Wait for frames or `acknowledge` |
| `AUTH_FAILED` | Credentials rejected | Auth-latch until `acknowledge` after correction |
| `UNSUPPORTED` | Camera does not implement reboot | Auth-latch until `acknowledge` |
| `UNREACHABLE` | Never connected | No send. Preflight backoff |
| `REJECTED` | Explicit SOAP fault | Latch |

An ONVIF acknowledgement is not recovery success.

## Frigate authentication

* `auth.mode: none` — internal port 5000. A 401/403 is a configuration
  error, not a camera failure. There is no silent fallback.
* `auth.mode: frigate` — native login, JWT cookie/bearer. One controlled
  re-login per read on expiry. Repeated bad passwords back off. TLS
  verification stays on; optional `ca_bundle`.

A permission-filtered camera list is not evidence that missing cameras
failed. They are `UNKNOWN`.

## Troubleshooting

**Everything is `UNKNOWN` / `TELEMETRY_STALE`.**
Frigate is returning cached or clock-skewed stats. Check
`service.last_updated` against the watchdog host clock. Do not reboot
cameras.

**`NO_HEALTHY_PEER` on a one-camera site.**
v0.1.0 will not automatically reboot without a witness. That is
intentional.

**`MULTIPLE_CAMERAS_FAILING` after a LAN blip.**
Wait. When one recovers, the remaining camera must pass a **new** full
observation window. Old proposals are discarded.

**Reboot requested, camera still dark.**
Read `last_outcome`. `OUTCOME_UNKNOWN` means the command may have landed;
there will not be a second attempt this outage. Use `fwatch probe`, then
`fwatch acknowledge CAMERA --reason "..."` **after stopping the service**
if you have corrected the cause and want a future outage to be eligible
(cooldown still applies).

**`STATE_STORE_UNSAFE`.**
Do not delete `/data`. Inspect the volume (permissions, disk full, copy
the file off). Fix the underlying problem and restart.

**Two containers, two data volumes.**
Unsupported. A local lock does not coordinate independent installations.

## Commissioning checklist

1. Observe mode, persistent `/data`.
2. All cameras `armed`.
3. `fwatch probe` each ONVIF target.
4. Owner performs one controlled reboot of one camera **out-of-band**
   (power-cycle or vendor UI). The watchdog itself has no reboot command;
   `fwatch probe` is read-only by design.
5. Confirm Frigate frames return.
6. `mode: recover` for verified targets only.
