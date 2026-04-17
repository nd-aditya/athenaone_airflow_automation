"""
VPN connect/disconnect service for Airflow DAGs.
Controls OpenVPN CLI directly — no UI interaction needed.

Usage in DAG:
    from services.vpn_service import vpn_connect, vpn_disconnect
"""
from __future__ import annotations

import os
import subprocess
import time


OPENVPN_BIN    = "/opt/homebrew/sbin/openvpn"
OPENVPN_CONFIG = "/etc/openvpn/nd-platform.ovpn"
OPENVPN_LOG    = "/tmp/openvpn_airflow.log"
CONNECT_TIMEOUT_SEC = 60


def vpn_connect() -> dict:
    """
    Start OpenVPN as a background daemon and wait until the tunnel is up.
    Raises RuntimeError if connection does not complete within CONNECT_TIMEOUT_SEC.
    """
    # Clear previous log so we don't match old "Sequence Completed" entries
    try:
        os.remove(OPENVPN_LOG)
    except (FileNotFoundError, PermissionError):
        pass

    subprocess.run(
        [
            "sudo", OPENVPN_BIN,
            "--config", OPENVPN_CONFIG,
            "--daemon",
            "--log", OPENVPN_LOG,
        ],
        check=True,
    )

    # Poll log until tunnel is up
    deadline = time.time() + CONNECT_TIMEOUT_SEC
    while time.time() < deadline:
        time.sleep(2)
        try:
            with open(OPENVPN_LOG, "r") as f:
                content = f.read()
            if "Initialization Sequence Completed" in content:
                print("[vpn_service] VPN connected.")
                return {"status": "connected", "config": OPENVPN_CONFIG}
            if "AUTH_FAILED" in content:
                raise RuntimeError("VPN auth failed — check credentials in .ovpn profile")
        except (FileNotFoundError, PermissionError):
            pass  # log not written yet

    raise RuntimeError(f"VPN did not connect within {CONNECT_TIMEOUT_SEC} seconds")


def vpn_disconnect() -> dict:
    """
    Stop all running openvpn processes.
    Safe to call even if VPN is already disconnected.
    """
    result = subprocess.run(
        ["sudo", "killall", "openvpn"],
        capture_output=True, text=True
    )
    # killall returns non-zero if no process found — not an error
    if result.returncode not in (0, 1):
        raise RuntimeError(f"vpn_disconnect failed: {result.stderr}")
    print("[vpn_service] VPN disconnected.")
    return {"status": "disconnected"}


def vpn_is_connected() -> bool:
    """Check if openvpn process is currently running."""
    result = subprocess.run(
        ["pgrep", "-x", "openvpn"],
        capture_output=True
    )
    return result.returncode == 0
