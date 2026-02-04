#!/usr/bin/env python3
"""orchestrator-ctl - CLI for the Qubic orchestrator management API.

Usage:
    orchestrator-ctl status
    orchestrator-ctl health
    orchestrator-ctl restart
    orchestrator-ctl trigger-snapshot
    orchestrator-ctl send-key f4
    orchestrator-ctl keys

Designed to be called via:
    docker exec <container> orchestrator-ctl status
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

DEFAULT_BASE_URL = "http://127.0.0.1:8080"
TIMEOUT_SECONDS = 30


def _request(
    method: str, path: str, base_url: str, body: dict | None = None
) -> dict:
    url = f"{base_url}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, method=method, data=data)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode()
        try:
            return json.loads(resp_body)
        except json.JSONDecodeError:
            return {"status": "error", "message": resp_body}
    except urllib.error.URLError as e:
        print(
            f"Error: Cannot connect to orchestrator at {url}: {e.reason}",
            file=sys.stderr,
        )
        sys.exit(1)


def _cmd_status(args: argparse.Namespace) -> None:
    data = _request("GET", "/status", args.base_url)
    print(json.dumps(data, indent=2))


def _cmd_health(args: argparse.Namespace) -> None:
    data = _request("GET", "/health", args.base_url)
    print(json.dumps(data, indent=2))


def _cmd_restart(args: argparse.Namespace) -> None:
    data = _request("POST", "/restart", args.base_url)
    print(json.dumps(data, indent=2))
    if data.get("status") != "ok":
        sys.exit(1)


def _cmd_trigger_snapshot(args: argparse.Namespace) -> None:
    data = _request("POST", "/trigger-snapshot", args.base_url)
    print(json.dumps(data, indent=2))
    if data.get("status") != "ok":
        sys.exit(1)


def _cmd_send_key(args: argparse.Namespace) -> None:
    data = _request(
        "POST", "/send-key", args.base_url, body={"key": args.key}
    )
    print(json.dumps(data, indent=2))
    if data.get("status") != "ok":
        sys.exit(1)


def _cmd_keys(args: argparse.Namespace) -> None:
    data = _request("GET", "/keys", args.base_url)
    if data.get("status") == "ok":
        for key, desc in data.get("keys", {}).items():
            print(f"  {key:<6}  {desc}")
    else:
        print(json.dumps(data, indent=2))
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="orchestrator-ctl",
        description="CLI for the Qubic orchestrator management API",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Management API base URL (default: {DEFAULT_BASE_URL})",
    )

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="Show full orchestrator status")
    sub.add_parser("health", help="Simple health check")
    sub.add_parser("restart", help="Restart the Qubic node binary")
    sub.add_parser(
        "trigger-snapshot", help="Trigger immediate snapshot cycle"
    )
    sk = sub.add_parser(
        "send-key", help="Send a key command to the Qubic node"
    )
    sk.add_argument(
        "key",
        help="Key name (e.g. f4, f12). Use 'keys' command to list all.",
    )
    sub.add_parser("keys", help="List available key commands")

    args = parser.parse_args()

    commands = {
        "status": _cmd_status,
        "health": _cmd_health,
        "restart": _cmd_restart,
        "trigger-snapshot": _cmd_trigger_snapshot,
        "send-key": _cmd_send_key,
        "keys": _cmd_keys,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
