"""Unit tests for configuration loading and validation."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

from frigate_watchdog.config import ConfigError, parse_config

ENV = {
    "CAMERA_PORCH_PASSWORD": "porch-pass",
    "CAMERA_DRIVEWAY_PASSWORD": "drive-pass",
    "FRIGATE_WATCHDOG_PASSWORD": "frigate-pass",
    "MQTT_PASSWORD": "mqtt-pass",
}

BASE = {
    "schema_version": 1,
    "mode": "observe",
    "frigate": {"base_url": "http://frigate:5000", "auth": {"mode": "none"}},
    "cameras": {
        "porch": {
            "frigate_name": "Porch",
            "recovery": "onvif",
            "onvif": {
                "endpoint": "http://192.168.10.40/onvif/device_service",
                "username": "admin",
                "password_env": "CAMERA_PORCH_PASSWORD",
            },
        },
        "driveway": {
            "frigate_name": "Driveway",
            "recovery": "none",
        },
    },
    "mqtt": {"enabled": False},
}


def build(**overrides):
    doc = copy.deepcopy(BASE)
    doc.update(overrides)
    return doc


def camera(doc, key):
    return doc["cameras"][key]


def dump(doc):
    import yaml

    return yaml.safe_dump(doc, sort_keys=False)


def test_valid_config_parses():
    config = parse_config(dump(build()), env=ENV)
    assert config.mode == "observe"
    assert config.cameras["porch"].onvif is not None
    assert config.cameras["porch"].onvif.endpoint.host == "192.168.10.40"
    assert config.cameras["driveway"].recovery == "none"
    assert config.cameras["driveway"].onvif is None


def test_mode_defaults_to_observe():
    doc = build()
    del doc["mode"]
    config = parse_config(dump(doc), env=ENV)
    assert config.mode == "observe"


def test_invalid_mode_rejected():
    with pytest.raises(ConfigError) as err:
        parse_config(dump(build(mode="recover!!")), env=ENV)
    assert any(code == "invalid_mode" for _, code, _ in err.value.problems)


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda d: d.update({"unexpected": 1}), "unknown_key"),
        (lambda d: d.update({"schema_version": 2}), "unsupported_schema"),
        (lambda d: d.update({"timings": {"nope": 5}}), "unknown_key"),
        (lambda d: d.update({"timings": {"startup_grace_s": 1}}), "invalid_timing"),
        (lambda d: d.update({"timings": {"startup_grace_s": 100000}}), "invalid_timing"),
        (lambda d: d.update({"timings": {"poll_interval_s": "fast"}}), "invalid_timing"),
        (lambda d: d["frigate"].update({"base_url": "ftp://x"}), "invalid_url"),
        (lambda d: d["frigate"].update({"base_url": "http://frigate:5000/path"}), "invalid_url"),
        (lambda d: d["frigate"].update({"base_url": "http://user:pw@frigate:5000"}), "invalid_url"),
        (lambda d: d["frigate"]["auth"].update({"mode": "basic"}), "invalid_auth_mode"),
        (lambda d: d["frigate"]["auth"].update({"mode": "frigate"}), "missing_value"),
        (lambda d: camera(d, "porch").update({"recovery": "ssh"}), "invalid_recovery"),
        (lambda d: camera(d, "porch").update({"frigate_name": ""}), "missing_value"),
        (
            lambda d: camera(d, "porch").update(
                {"onvif": {"endpoint": "http://camera.local/wsdl"}}
            ),
            "not_an_ip",
        ),
        (
            lambda d: camera(d, "porch")["onvif"].update(
                {"endpoint": "http://user:pw@192.168.10.40/onvif"}
            ),
            "invalid_url",
        ),
        (
            lambda d: camera(d, "porch")["onvif"].update(
                {"endpoint": "http://192.168.10.40/onvif?q=1"}
            ),
            "invalid_url",
        ),
        (
            lambda d: camera(d, "porch")["onvif"].update(
                {"endpoint": "https://192.168.10.40:70000/onvif"}
            ),
            "invalid_url",
        ),
        (lambda d: camera(d, "porch")["onvif"].update({"username": ""}), "missing_value"),
        (
            lambda d: camera(d, "porch")["onvif"].update({"password_env": "not a name"}),
            "invalid_env_name",
        ),
        (lambda d: camera(d, "porch").update({"maintenance": "yes"}), "invalid_value"),
        (
            lambda d: camera(d, "driveway").update(
                {"onvif": {"endpoint": "http://10.0.0.1/x", "username": "a", "password_env": "X"}}
            ),
            "unknown_key",
        ),
        (lambda d: d.update({"cameras": {}}), "missing_value"),
        (
            lambda d: d.update(
                {"cameras": {"bad key!": {"frigate_name": "X", "recovery": "none"}}}
            ),
            "invalid_camera_key",
        ),
        (lambda d: d.update({"mqtt": {"enabled": True}}), "missing_value"),
        (lambda d: d.update({"http": {"host": "0.0.0.0:1"}}), "insecure_bind"),
        (lambda d: d.update({"http": {"port": 70000}}), "invalid_value"),
    ],
)
def test_invalid_configurations_rejected(mutation, code):
    doc = build()
    mutation(doc)
    with pytest.raises(ConfigError) as err:
        parse_config(dump(doc), env=ENV)
    assert any(c == code for _, c, _ in err.value.problems), (
        f"expected {code} in {err.value.problems}"
    )


def test_duplicate_yaml_key_rejected():
    text = """
schema_version: 1
mode: observe
mode: recover
frigate: {base_url: "http://frigate:5000"}
cameras:
  porch:
    frigate_name: Porch
    frigate_name: Porch2
    recovery: none
"""
    with pytest.raises(ConfigError) as err:
        parse_config(text, env=ENV)
    assert any(code == "duplicate_key" for _, code, _ in err.value.problems)


def test_duplicate_frigate_name_rejected():
    doc = build()
    doc["cameras"]["copy"] = dict(doc["cameras"]["driveway"])
    with pytest.raises(ConfigError) as err:
        parse_config(dump(doc), env=ENV)
    assert any(code == "duplicate_frigate_name" for _, code, _ in err.value.problems)


def test_duplicate_normalized_endpoint_rejected():
    doc = build()
    doc["cameras"]["porch2"] = {
        "frigate_name": "Porch2",
        "recovery": "onvif",
        "onvif": {
            "endpoint": "http://192.168.10.40:80/onvif/device_service/",
            "username": "admin",
            "password_env": "CAMERA_PORCH_PASSWORD",
        },
    }
    with pytest.raises(ConfigError) as err:
        parse_config(dump(doc), env=ENV)
    assert any(code == "duplicate_target" for _, code, _ in err.value.problems)


def test_distinct_ports_are_distinct_targets():
    doc = build()
    doc["cameras"]["porch2"] = {
        "frigate_name": "Porch2",
        "recovery": "onvif",
        "onvif": {
            "endpoint": "http://192.168.10.40:8000/onvif/device_service",
            "username": "admin",
            "password_env": "CAMERA_PORCH_PASSWORD",
        },
    }
    config = parse_config(dump(doc), env=ENV)
    assert len(config.cameras) == 3


def test_missing_env_rejected():
    env = {k: v for k, v in ENV.items() if k != "CAMERA_PORCH_PASSWORD"}
    with pytest.raises(ConfigError) as err:
        parse_config(dump(build()), env=env)
    assert any(code == "missing_env" for _, code, _ in err.value.problems)


def test_empty_env_is_missing():
    env = dict(ENV)
    env["CAMERA_PORCH_PASSWORD"] = "   "
    with pytest.raises(ConfigError) as err:
        parse_config(dump(build()), env=env)
    assert any(code == "missing_env" for _, code, _ in err.value.problems)


def test_secrets_never_render():
    config = parse_config(dump(build()), env=ENV)
    onvies = config.cameras["porch"].onvif
    assert onvies is not None
    assert "porch-pass" not in repr(config)
    assert "porch-pass" not in repr(onvies)
    assert onvies.password.value == "porch-pass"  # usable explicitly
    assert repr(onvies.password) != "porch-pass"
    assert str(onvies.password) != "porch-pass"


def test_frigate_auth_mode_parses_and_requires_env():
    doc = build()
    doc["frigate"]["auth"] = {"mode": "frigate", "username": "watchdog", "password_env": "MISSING"}
    with pytest.raises(ConfigError) as err:
        parse_config(dump(doc), env=ENV)
    assert any(code == "missing_env" for _, code, _ in err.value.problems)
    doc["frigate"]["auth"] = {
        "mode": "frigate",
        "username": "watchdog",
        "password_env": "FRIGATE_WATCHDOG_PASSWORD",
    }
    config = parse_config(dump(doc), env=ENV)
    assert config.frigate.auth.mode == "frigate"
    assert config.frigate.auth.password.value == "frigate-pass"


def test_auth_none_rejects_extra_keys():
    doc = build()
    doc["frigate"]["auth"] = {"mode": "none", "username": "x"}
    with pytest.raises(ConfigError) as err:
        parse_config(dump(doc), env=ENV)
    assert any(code == "unknown_key" for _, code, _ in err.value.problems)


def test_default_timings_match_brief():
    config = parse_config(dump(build()), env=ENV)
    t = config.timings
    assert t["poll_interval_s"] == 10.0
    assert t["request_deadline_s"] == 5.0
    assert t["max_stats_age_s"] == 60.0
    assert t["startup_grace_s"] == 180.0
    assert t["frigate_restart_grace_s"] == 180.0
    assert t["post_interruption_stability_s"] == 60.0
    assert t["zero_frames_threshold_s"] == 120.0
    assert t["min_bad_snapshots"] == 6.0
    assert t["arm_healthy_s"] == 60.0
    assert t["recovery_confirm_s"] == 120.0
    assert t["boot_grace_s"] == 180.0


def test_timing_overrides_within_bounds():
    doc = build()
    doc["timings"] = {"zero_frames_threshold_s": 300, "min_bad_snapshots": 10}
    config = parse_config(dump(doc), env=ENV)
    assert config.timings["zero_frames_threshold_s"] == 300.0
    assert config.timings["min_bad_snapshots"] == 10.0


def test_non_mapping_root_rejected():
    with pytest.raises(ConfigError):
        parse_config("- a\n- b\n", env=ENV)
    with pytest.raises(ConfigError):
        parse_config("", env=ENV)


def test_invalid_yaml_rejected():
    with pytest.raises(ConfigError) as err:
        parse_config("mode: [unclosed", env=ENV)
    assert any(code == "invalid_yaml" for _, code, _ in err.value.problems)


def test_endpoint_ipv6_accepted():
    doc = build()
    doc["cameras"]["porch"]["onvif"]["endpoint"] = "http://[::1]:8080/onvif/device_service"
    config = parse_config(dump(doc), env=ENV)
    assert config.cameras["porch"].onvif.endpoint.host == "::1"


def test_maintenance_flag_parses():
    doc = build()
    doc["cameras"]["driveway"]["maintenance"] = True
    config = parse_config(dump(doc), env=ENV)
    assert config.cameras["driveway"].maintenance is True


def test_ca_bundle_missing_file_rejected(tmp_path: Path):
    doc = build()
    doc["frigate"]["auth"] = {
        "mode": "frigate",
        "username": "u",
        "password_env": "FRIGATE_WATCHDOG_PASSWORD",
        "ca_bundle": str(tmp_path / "nope.pem"),
    }
    with pytest.raises(ConfigError) as err:
        parse_config(dump(doc), env=ENV)
    assert any(code == "missing_file" for _, code, _ in err.value.problems)
