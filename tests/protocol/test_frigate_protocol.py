"""Protocol tests against a real local fake-Frigate HTTP server."""

from __future__ import annotations

import asyncio

import pytest

from frigate_watchdog.config import FrigateAuthConfig, FrigateConfig
from frigate_watchdog.frigate import FrigateClient
from tests.fakes.frigate_fake import FakeFrigate


def no_auth(base: str) -> FrigateConfig:
    return FrigateConfig(base_url=base, auth=FrigateAuthConfig(mode="none"))


def frigate_auth(base: str, password_env: str = "FRIGATE_WATCHDOG_PASSWORD") -> FrigateConfig:
    class _P:
        value = "frigate-pass"

        def __repr__(self):
            return "<secret>"

    return FrigateConfig(
        base_url=base,
        auth=FrigateAuthConfig(
            mode="frigate", username="watchdog", password_env=password_env, password=_P()
        ),  # type: ignore[arg-type]
    )


@pytest.fixture()
async def fake():
    f = FakeFrigate()
    base = await f.start()
    yield f, base
    await f.stop()


async def test_no_auth_fetches_stats_and_config(fake):
    _f, base = fake
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem is None
    assert result.stats is not None
    assert result.stats["service"]["uptime"] == 1244.0  # advanced by 10 on the read
    assert result.enabled_map == {"Porch": True, "Driveway": True, "Doorbell": True}
    assert "ffmpeg_inputs" not in result.stats  # only the config doc ever had them


async def test_disabled_camera_omitted_from_stats(fake):
    f, base = fake
    f.state.enabled["Porch"] = False
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert "Porch" not in result.stats["cameras"]
    assert result.enabled_map["Porch"] is False


async def test_cached_snapshot_served_identically(fake):
    f, base = fake
    f.state.stats_behavior = "cached"
    client = FrigateClient(no_auth(base))
    await client.start()
    r1 = await client.fetch_observations()
    r2 = await client.fetch_observations()
    await client.close()
    assert r1.stats["service"] == r2.stats["service"]
    assert r1.stats["cameras"] == r2.stats["cameras"]


async def test_stale_timestamp_flows_through_as_old_last_updated(fake):
    import time

    f, base = fake
    f.state.stats_behavior = "stale"
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem is None  # HTTP fine; staleness is judged downstream
    assert result.stats["service"]["last_updated"] < time.time() - 200


async def test_unreachable(fake):
    f, base = fake
    await f.stop()
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem in ("unreachable",)


async def test_timeout_classified(fake):
    f, base = fake
    f.state.stats_behavior = "slow"
    client = FrigateClient(no_auth(base), request_deadline_s=0.5)
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem == "timeout"


@pytest.mark.parametrize(
    ("behavior", "problem"),
    [
        ("malformed", "malformed"),
        ("oversized", "too_large"),
        ("html", "html_response"),
        ("status_500", "http_500"),
        ("status_429", "http_429"),
        ("down", "http_503"),
    ],
)
async def test_fault_behaviours(fake, behavior, problem):
    f, base = fake
    f.state.stats_behavior = behavior
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem == problem
    assert result.stats is None


async def test_connection_reset_classified(fake):
    f, base = fake
    f.state.stats_behavior = "reset"
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem in ("unreachable", "timeout")


async def test_no_auth_mode_getting_401_is_auth_problem_not_camera_failure(fake):
    f, base = fake
    f.state.auth_enabled = True  # server demands auth; client is configured 'none'
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem == "auth"


async def test_native_login_and_bearer_reads(fake):
    f, base = fake
    f.state.auth_enabled = True
    client = FrigateClient(frigate_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem is None
    assert result.stats is not None
    assert f.state.login_requests == 1
    assert f.state.authenticated_reads >= 2


async def test_wrong_password_no_login_storm(fake):
    f, base = fake
    f.state.auth_enabled = True
    f.state.users["watchdog"] = "different"
    client = FrigateClient(frigate_auth(base))
    await client.start()
    for _ in range(6):
        result = await client.fetch_observations()
        assert result.problem in ("auth", "auth_backoff")
        await asyncio.sleep(0)
    await client.close()
    assert f.state.login_requests <= 3, f"login storm: {f.state.login_requests}"
    assert f.state.authenticated_reads == 0


async def test_token_expiry_one_relogin_and_retry(fake):
    f, base = fake
    f.state.auth_enabled = True
    f.state.token_ttl_s = 0.05
    client = FrigateClient(frigate_auth(base))
    await client.start()
    r1 = await client.fetch_observations()
    assert r1.problem is None
    await asyncio.sleep(0.1)  # token expires
    r2 = await client.fetch_observations()
    await client.close()
    assert r2.problem is None  # controlled re-login and retry succeeded
    assert r2.stats is not None
    assert f.state.login_requests == 2


async def test_permission_filtered_config_visible(fake):
    f, base = fake
    f.state.config_behavior = "omit_camera:Doorbell"
    client = FrigateClient(no_auth(base))
    await client.start()
    result = await client.fetch_observations()
    await client.close()
    assert result.problem is None
    assert "Doorbell" not in result.enabled_map  # normalization flags it UNKNOWN
