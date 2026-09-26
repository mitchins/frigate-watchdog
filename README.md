# frigate-watchdog

Watch Frigate for a sustained loss of camera frames. When the evidence
points to **one camera** rather than a wider outage, request **exactly one**
ONVIF reboot, verify whether frames return, and report the outcome.

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

2. Copy [`examples/.env.watchdog.example`](examples/.env.watchdog.example) to
   `.env.watchdog` next to `compose.yaml` and fill in one variable per
   `password_env` name used in your config. Empty values are treated as
   missing.

3. Deploy with the canonical Compose file. The data volume is the safety
   record — do not throw it away.

```yaml
# compose.yaml is in this repository. Keep /data.
```

```sh
mkdir -p config data
cp examples/config.example.yaml config/config.yaml
cp examples/.env.watchdog.example .env.watchdog
# edit config/config.yaml and .env.watchdog
docker compose up -d
```

Default mode is **observe**: the watchdog reports what it would do and never
sends `SystemReboot`. Enable recovery only after commissioning (below).

No HTTP port is published by default. To inspect status from the host,
uncomment the loopback mapping in `compose.yaml`:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

The example config binds `0.0.0.0` **inside** the container (Docker forwards
published ports to the container's eth0, not its loopback); the host-side
`127.0.0.1` in the mapping is what restricts exposure. A non-containerised
install should bind `127.0.0.1` directly.

Then:

```sh
fwatch health
fwatch stats
fwatch history
```

Inside the container the same CLI is available as `fwatch`.

## Observe vs recover

| `mode`     | Behaviour |
| ---------- | --------- |
| `observe`  | Full monitoring, arming, and reporting. **Zero** mutating camera requests. |
| `recover`  | The same, plus at most one reserved ONVIF reboot per eligible single-camera outage. |

A camera with `recovery: none` is still a **witness** to network health. It
is never rebooted.

## Reading results

* Console: state transitions and actions, not every successful poll.
* `GET /health` — process and decision loop. A correctly inhibiting
  watchdog is **alive**. Frigate/camera/MQTT being down does not fail health.
* `GET /stats` — observations, inhibition reasons, mode, limits, last action.
  Every response includes `service_alive`, `monitoring_fresh`, `mode`,
  `recovery_currently_permitted`, `inhibition_reasons`.
* `GET /history` — bounded, sanitized event log.
* Optional MQTT (output only):
  `frigate-watchdog/<instance>/{availability,state,events}`.

There is no reboot HTTP endpoint and no remote acknowledgement API.

## Commissioning (do this once)

1. Deploy in **observe** mode.
2. Wait until every camera has an established healthy baseline (`armed` in `/stats`).
3. `fwatch probe CAMERA` — read-only ONVIF check. Never reboots.
4. Inspect `/stats` inhibition reasons under a real outage (or wait).
5. Perform **one** controlled reboot of **one** verified camera
   **out-of-band** — power-cycle it or use the vendor app. The watchdog has
   no command for this; `fwatch probe` is read-only by design.
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
* Two cameras down → alert, no reboot. Single-camera installs → observe-only
  for recovery (`NO_HEALTHY_PEER`).
* Three attempts per camera per 24h of **runtime**, minimum one hour between
  attempts. Restarting the host does not shorten those limits.

Policy detail and troubleshooting: [`docs/operations.md`](docs/operations.md).

## CLI

```
fwatch check-config
fwatch serve
fwatch health | stats | history
fwatch probe CAMERA
fwatch acknowledge CAMERA --reason "..."
```

`acknowledge` is local and only permitted while the service is **stopped**
(it takes the same exclusive `/data` lock). It does not reboot, does not
erase cooldowns or budgets, and does not override global safety conditions.

## License

MIT. See [LICENSE](LICENSE).
