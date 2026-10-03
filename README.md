# frigate-watchdog

Watch Frigate for a sustained loss of camera frames. When the evidence
points to the cameras rather than a wider outage, request **one** ONVIF
reboot per outage — one camera at a time — verify whether frames return,
and report the outcome.

> Recovers sustained capture failures one camera at a time. Wider outages
> and ambiguous failures alert without rebooting. Reboot support depends on
> the camera. Keep the data volume.

A camera outage may justify one recovery attempt. Uncertainty never
justifies a reboot loop.

## What this is

A small recovery appliance. It talks to:

* Frigate’s HTTP API (`/api/stats`, `/api/config`, optional native login)
* each camera’s ONVIF Device Management endpoint (`SystemReboot` only)

It does **not** need Home Assistant, Node-RED, a Docker socket, or a camera
VLAN interface. It initiates outbound connections only.

Supported Frigate: **0.18.0** (the stats/config contract is frozen against
that version). Works with stock Frigate regardless of detector.

## Quick start

1. Copy [`examples/config.example.yaml`](examples/config.example.yaml) to
   `config/config.yaml` and fill in your cameras. Camera keys are local
   identities; `frigate_name` must match Frigate **exactly** (case-sensitive).
   ONVIF endpoints must be explicit IP addresses.

2. Put camera passwords in the environment (`CAMERA_PORCH_PASSWORD`, …).
   Empty values are treated as missing.

3. Deploy with the canonical Compose file, which runs the published image
   (`ghcr.io/mitchins/frigate-watchdog`). The data volume is the safety
   record — do not throw it away.

```sh
mkdir -p config
cp examples/config.example.yaml config/config.yaml
# edit config/config.yaml; export CAMERA_*_PASSWORD
docker volume create watchdog_data
docker compose up -d
```

Default mode is **observe**: the watchdog records what it would do
(`would_recover` events) and never sends `SystemReboot`. Enable recovery
only after commissioning (below).

No HTTP port is published by default. The status API listens on all
interfaces *inside* the container; to inspect it from the host, uncomment
the loopback mapping in `compose.yaml`, which publishes it on the host's
loopback only:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

Then:

```sh
fwatch health
fwatch stats
fwatch history
fwatch report
```

Inside the container the same CLI is available as `fwatch`.

## Observe vs recover

| `mode`     | Behaviour |
| ---------- | --------- |
| `observe`  | Full monitoring, arming, and reporting, including a `would_recover` event whenever recover mode would have rebooted a camera. **Zero** mutating camera requests. |
| `recover`  | The same, plus at most one reserved ONVIF reboot per eligible outage. Several failing cameras are recovered one at a time. |

A camera with `recovery: none` is still a **witness** to network health. It
is never rebooted.

## Reading results

* Console: state transitions and actions, not every successful poll.
* `GET /health` — process and decision loop. A correctly inhibiting
  watchdog is **alive**. Frigate/camera/MQTT being down does not fail health.
* `GET /stats` — observations, inhibition reasons, mode, limits, last action.
  Every response includes `service_alive`, `monitoring_fresh`, `mode`,
  `recovery_currently_permitted`, `inhibition_reasons`.
* `GET /history` — bounded, sanitized event log, including when each
  outage started and ended (`frames_stopped` / `frames_restored`).
* `GET /report` (`fwatch report`) — per camera: outages, how many recovered
  on their own versus after a reboot request, observe-mode `would_recover`
  decisions, reboots sent, latched incidents, and outage durations.
* Optional MQTT (output only):
  `frigate-watchdog/<instance>/{availability,state,events}`.

There is no reboot HTTP endpoint and no remote acknowledgement API.

## Commissioning (do this once)

1. Deploy in **observe** mode.
2. Wait until every camera has an established healthy baseline (`armed` in `/stats`).
3. `fwatch probe CAMERA` — read-only ONVIF check. Never reboots.
4. Inspect `/stats` inhibition reasons under a real outage (or wait).
5. Authorise **one** controlled reboot of **one** verified camera.
6. Confirm real frames return in Frigate.
7. Set `mode: recover` for that camera only; expand only to verified targets.

Do not test by disrupting the production VLAN. Do not reboot every camera
“to check compatibility.”

## Limits (deliberate)

* Frame rate is not a picture. A camera can emit repeated frames while
  looking wrong. Recording-only failures while capture `camera_fps` is
  healthy are outside the automatic-recovery promise.
* Zero frames can be a camera fault, a network fault, bad stream credentials,
  or a Frigate-side problem. Protection is corroboration, restraint, and a
  bounded attempt — not a diagnosis.
* Stale or cached Frigate stats never become “all cameras failed.”
* An ONVIF timeout after a possible reboot delivery is **outcome unknown**.
  It is never retried automatically.
* Several cameras down → recovered **one at a time**, and only while at least
  one camera is stably healthy. The next camera waits until the previous one
  is confirmed healthy; if a reboot leaves its camera dark, no further
  cameras are rebooted (`PHASED_RECOVERY_HALTED`). Every camera down, or a
  single-camera install → no recovery (`NO_HEALTHY_PEER`).
* Three attempts per camera per 24h of **runtime**, minimum one hour between
  attempts. Restarting the host does not shorten those limits.

Policy detail and troubleshooting: [`docs/operations.md`](docs/operations.md).

## Deploying alongside Frigate

Lessons from real deployments:

* **Frigate with `network_mode: host`.** The watchdog runs on its own Docker
  network, so `http://frigate:5000` does not resolve. Use the Docker host's
  LAN address, e.g. `http://192.168.1.15:5000`.
* **`frigate_name` is Frigate's camera key**, usually lowercase (`porch`),
  matched exactly.
* **Reaching cameras on an isolated VLAN.** The watchdog only initiates
  connections, so it works from any host that can already reach the cameras
  (for example the Frigate host's camera-VLAN interface), including behind a
  firewall that blocks connections *from* the cameras.
* **Portainer stacks.** Relative paths such as `./data` resolve inside
  Portainer's own data directory. Use a named volume for `/data` (ideally
  created once and declared `external`) and an absolute host path for the
  config directory. The container runs as UID 1000: the config file and
  every parent directory must be readable/traversable by it.
* **Detection takes about 2½ minutes.** Frigate keeps reporting a camera's
  last frame rate for roughly 30 s after it goes dark, then the watchdog
  needs 120 s of zero frames. Short outages, such as a camera's own
  scheduled reboot, are recorded but never acted on. Don't shorten the
  threshold to compensate.

## CLI

```
fwatch check-config
fwatch serve
fwatch health | stats | history | report
fwatch probe CAMERA
fwatch acknowledge CAMERA --reason "..."
```

`acknowledge` is local and only permitted while the service is **stopped**
(it takes the same exclusive `/data` lock). It does not reboot, does not
erase cooldowns or budgets, and does not override global safety conditions.

## License

MIT. See [LICENSE](LICENSE).
