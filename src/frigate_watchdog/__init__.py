"""frigate-watchdog: a cautious camera-recovery appliance for Frigate."""

from importlib.metadata import version

# Single source of truth: pyproject.toml. A release candidate reports itself
# as one (e.g. 0.1.0rc1), so a running image identifies the exact release.
__version__ = version("frigate-watchdog")
