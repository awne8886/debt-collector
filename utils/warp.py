"""Cloudflare WARP as a local HTTP proxy, so YouTube sees Cloudflare's IP instead of the host's.

YouTube refuses requests from many datacenter IPs (Render's included) with HTTP 403.
WARP is Cloudflare's free WireGuard VPN. This module:

1. registers a free WARP device with Cloudflare (a fresh WireGuard key pair, made with
   PyNaCl), and keeps the registration in the state folder; with an account store
   (set_account_store, which the music cog points at the database) it survives
   redeploys too, so a new device is only registered when none is stored or
   Cloudflare stops accepting the stored one;
2. downloads `wireproxy` (a pinned release, checked against its SHA-256), a userspace
   WireGuard client that needs no root and exposes the tunnel as an HTTP proxy;
3. starts it on 127.0.0.1 and checks through Cloudflare's trace page that traffic
   really leaves via WARP.

yt-dlp and ffmpeg then use that proxy. YTDL_WARP picks when: "auto" (default) starts
it the first time YouTube blocks the direct route, "on" always uses it, "off" never.
A YTDL_PROXY setting always wins over WARP.

All of this blocks; call it from a worker thread.
"""

import atexit
import base64
import datetime
import hashlib
import io
import json
import logging
import os
import platform
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
from typing import Any, Callable, Dict, Optional

log = logging.getLogger("debt-collector.music")

WIREPROXY_VERSION: str = "v1.1.3"
_WIREPROXY_ASSETS: Dict[str, tuple] = {
    # machine: (release asset, sha256 published on the GitHub release)
    "x86_64": (
        "wireproxy_linux_amd64.tar.gz",
        "e88c1d090740373fc606c1bafd81d9a5eadc642cce5667616e20e9d7a444f51c",
    ),
    "aarch64": (
        "wireproxy_linux_arm64.tar.gz",
        "370e00bd2167960d1ecd1c3c1439715bbaa94a0a110a2040468670c9af6021b6",
    ),
}
_WIREPROXY_URL: str = "https://github.com/windtf/wireproxy/releases/download/{version}/{asset}"

_API: str = "https://api.cloudflareclient.com/v0a2158"
_API_HEADERS: Dict[str, str] = {
    "Content-Type": "application/json",
    "User-Agent": "okhttp/3.12.1",
    "CF-Client-Version": "a-6.30-3596",
}
_WARP_PORT: int = 2408
_TRACE_URL: str = "https://www.cloudflare.com/cdn-cgi/trace"
_RETRY_AFTER_FAILURE: float = 600.0

_lock = threading.Lock()
_process: Optional[subprocess.Popen] = None
_proxy: Optional[str] = None
_failed_at: float = 0.0


class WarpError(Exception):
    pass


class TunnelError(WarpError):
    """wireproxy ran, but no traffic got through WARP (e.g. a rejected registration)."""


_ACCOUNT_FIELDS = ("private_key", "peer_public_key", "address_v4", "endpoint")
_account_loader: Optional[Callable[[], Optional[Dict[str, Any]]]] = None
_account_saver: Optional[Callable[[Optional[Dict[str, Any]]], None]] = None
_account_source: str = "none"  # "file", "store" or "new": where the last one came from


def set_account_store(
    load: Optional[Callable[[], Optional[Dict[str, Any]]]],
    save: Optional[Callable[[Optional[Dict[str, Any]]], None]],
) -> None:
    """Keep the WARP registration somewhere durable (Render's disk is not).

    ``load()`` returns a saved registration or None; ``save(account)`` stores one, and
    ``save(None)`` forgets it. Both are called from worker threads and may block.
    The registration includes the WireGuard private key: it is never logged.
    """
    global _account_loader, _account_saver
    _account_loader, _account_saver = load, save


def valid_account(value: Any) -> bool:
    return isinstance(value, dict) and all(
        isinstance(value.get(field), str) and value.get(field) for field in _ACCOUNT_FIELDS
    )


def mode() -> str:
    """"auto", "on" or "off" (YTDL_WARP)."""
    value: str = os.getenv("YTDL_WARP", "auto").strip().lower()
    if value in ("1", "true", "yes", "always"):
        return "on"
    if value in ("0", "false", "no", "never"):
        return "off"
    return value if value in ("auto", "on", "off") else "auto"


def available() -> bool:
    """Whether WARP may be used here: allowed by settings, no YTDL_PROXY, Linux x86_64/arm64."""
    if mode() == "off" or os.getenv("YTDL_PROXY", "").strip():
        return False
    return sys.platform.startswith("linux") and _machine() in _WIREPROXY_ASSETS


def proxy_url() -> Optional[str]:
    """The running WARP proxy's URL, or None when it isn't running."""
    with _lock:
        if _proxy and _process is not None and _process.poll() is None:
            return _proxy
        return None


def status() -> Dict[str, Any]:
    """Diagnostics only: mode, whether WARP may run here, and whether it is running."""
    with _lock:
        running: bool = bool(_proxy and _process is not None and _process.poll() is None)
        failed_ago: Optional[float] = (time.monotonic() - _failed_at) if _failed_at else None
    return {
        "mode": mode(),
        "available": available(),
        "running": running,
        "last_failure_s_ago": round(failed_ago) if failed_ago is not None else None,
    }


def ensure_started() -> Optional[str]:
    """Start WARP if it isn't running. Returns the proxy URL, or None if it can't start."""
    global _process, _proxy, _failed_at
    with _lock:
        if _proxy and _process is not None and _process.poll() is None:
            return _proxy
        if _process is not None:
            log.warning("WARP proxy exited (code %s); starting it again", _process.poll())
            _process = None
            _proxy = None
        if not available():
            return None
        if _failed_at and time.monotonic() - _failed_at < _RETRY_AFTER_FAILURE:
            return None
        try:
            started: float = time.monotonic()
            log.info("Starting Cloudflare WARP for YouTube traffic")
            try:
                proxy: str = _start()
            except TunnelError as exc:
                if _account_source == "new":
                    raise
                # A reused registration that no longer carries traffic: Cloudflare
                # has probably dropped it. Register a new device once and retry.
                log.warning("The saved WARP registration doesn't work (%s); registering a new one", exc)
                _stop_locked()
                _forget_account()
                proxy = _start()
            _proxy = proxy
            _failed_at = 0.0
            log.info("WARP is up in %.1fs; YouTube traffic now goes through it", time.monotonic() - started)
            return proxy
        except Exception as exc:
            _failed_at = time.monotonic()
            log.warning("Couldn't start WARP (will retry in %d min): %s", _RETRY_AFTER_FAILURE // 60, exc)
            _stop_locked()
            return None


def stop() -> None:
    with _lock:
        _stop_locked()


def _stop_locked() -> None:
    global _process, _proxy
    if _process is not None and _process.poll() is None:
        _process.terminate()
        try:
            _process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _process.kill()
    _process = None
    _proxy = None


atexit.register(stop)


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #


def _machine() -> str:
    machine: str = platform.machine().lower()
    return {"amd64": "x86_64", "arm64": "aarch64"}.get(machine, machine)


def _state_dir() -> str:
    path: str = os.getenv("WARP_DIR", "").strip() or os.path.join(tempfile.gettempdir(), "warp")
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def _start() -> str:
    global _process
    binary: str = _wireproxy_binary()
    account: Dict[str, Any] = _account()
    port: int = _free_port()
    config_path: str = os.path.join(_state_dir(), "wireproxy.conf")
    _write_private(config_path, _wireproxy_config(account, port))

    log_path: str = os.path.join(_state_dir(), "wireproxy.log")
    log_file = open(log_path, "ab")
    try:
        _process = subprocess.Popen(
            [binary, "-c", config_path],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
    finally:
        log_file.close()

    proxy: str = f"http://127.0.0.1:{port}"
    deadline: float = time.monotonic() + 15
    while not _port_open(port):
        if _process.poll() is not None or time.monotonic() > deadline:
            raise WarpError(f"wireproxy didn't start: {_tail(log_path)}")
        time.sleep(0.2)

    trace: Dict[str, str] = {}
    for attempt in range(3):  # the WireGuard handshake can take a moment
        try:
            trace = _trace(proxy)
            break
        except Exception as exc:
            if attempt == 2:
                raise TunnelError(f"no traffic through the tunnel ({exc}); {_tail(log_path)}")
            time.sleep(2)
    if trace.get("warp") not in ("on", "plus"):
        raise TunnelError(f"traffic isn't going through WARP (warp={trace.get('warp')})")
    log.info("WARP exit: ip %s, Cloudflare location %s", trace.get("ip"), trace.get("colo"))
    return proxy


def _wireproxy_binary() -> str:
    override: str = os.getenv("WIREPROXY_PATH", "").strip()
    if override:
        return override
    asset, digest = _WIREPROXY_ASSETS[_machine()]
    target: str = os.path.join(_state_dir(), f"wireproxy-{WIREPROXY_VERSION}")
    if os.path.isfile(target) and os.access(target, os.X_OK):
        return target
    url: str = _WIREPROXY_URL.format(version=WIREPROXY_VERSION, asset=asset)
    with urllib.request.urlopen(url, timeout=60) as response:
        data: bytes = response.read()
    actual: str = hashlib.sha256(data).hexdigest()
    if actual != digest:
        raise WarpError(f"wireproxy download has SHA-256 {actual}, expected {digest}")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        member = next((m for m in archive.getmembers() if m.isfile() and m.name.endswith("wireproxy")), None)
        if member is None:
            raise WarpError("wireproxy binary not found in the release archive")
        extracted = archive.extractfile(member)
        if extracted is None:
            raise WarpError("couldn't read wireproxy from the release archive")
        binary: bytes = extracted.read()
    partial: str = target + ".part"
    with open(partial, "wb") as handle:
        handle.write(binary)
    os.chmod(partial, 0o700)
    os.replace(partial, target)
    log.info("Downloaded wireproxy %s (%d KB)", WIREPROXY_VERSION, len(binary) // 1024)
    return target


def _account_path() -> str:
    return os.path.join(_state_dir(), "account.json")


def _forget_account() -> None:
    """Drop the local and stored registration so the next start registers again."""
    try:
        os.remove(_account_path())
    except OSError:
        pass
    if _account_saver is not None:
        try:
            _account_saver(None)
        except Exception as exc:
            log.warning("Couldn't clear the saved WARP registration: %s", type(exc).__name__)


def _load_stored_account() -> Optional[Dict[str, Any]]:
    if _account_loader is None:
        return None
    try:
        stored = _account_loader()
    except Exception as exc:
        log.warning("Couldn't read the saved WARP registration: %s", type(exc).__name__)
        return None
    return dict(stored) if valid_account(stored) else None


def _save_account(account: Dict[str, Any]) -> None:
    if _account_saver is None:
        return
    try:
        _account_saver(dict(account))
    except Exception as exc:
        log.warning("Couldn't save the WARP registration: %s", type(exc).__name__)


def _account() -> Dict[str, Any]:
    """The WARP registration: from the state folder, else the account store, else new."""
    global _account_source
    path: str = _account_path()
    try:
        with open(path) as handle:
            saved: Dict[str, Any] = json.load(handle)
        if valid_account(saved):
            _account_source = "file"
            return saved
    except (OSError, ValueError):
        pass

    stored: Optional[Dict[str, Any]] = _load_stored_account()
    if stored is not None:
        _write_private(path, json.dumps(stored))
        _account_source = "store"
        log.info("Reusing the saved Cloudflare WARP registration")
        return stored

    account: Dict[str, Any] = _register()
    _write_private(path, json.dumps(account))
    _save_account(account)
    _account_source = "new"
    return account


def _register() -> Dict[str, Any]:
    """Register a new free WARP device with Cloudflare."""
    from nacl.public import PrivateKey

    private = PrivateKey.generate()
    public_b64: str = base64.b64encode(bytes(private.public_key)).decode()
    body: Dict[str, Any] = {
        "key": public_b64,
        "install_id": "",
        "fcm_token": "",
        "tos": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "model": "PC",
        "type": "Linux",
        "locale": "en_US",
    }
    registration: Dict[str, Any] = _api("POST", "/reg", body)
    config: Dict[str, Any] = registration.get("config") or {}
    peer: Dict[str, Any] = (config.get("peers") or [{}])[0]
    addresses: Dict[str, Any] = (config.get("interface") or {}).get("addresses") or {}
    endpoint: Dict[str, Any] = peer.get("endpoint") or {}
    if not peer.get("public_key") or not addresses.get("v4"):
        raise WarpError("unexpected registration response from Cloudflare")

    try:  # what the official client does after registering; not fatal if refused
        _api(
            "PATCH",
            f"/reg/{registration['id']}",
            {"warp_enabled": True},
            token=str(registration.get("token") or ""),
        )
    except Exception as exc:
        log.debug("WARP enable call failed: %s", exc)

    v4_host: str = str(endpoint.get("v4") or "").rsplit(":", 1)[0]
    account: Dict[str, Any] = {
        "private_key": base64.b64encode(bytes(private)).decode(),
        "peer_public_key": peer["public_key"],
        "address_v4": addresses["v4"],
        "address_v6": addresses.get("v6") or "",
        # An IP avoids a DNS lookup outside the tunnel; the host name is the fallback.
        "endpoint": f"{v4_host}:{_WARP_PORT}" if v4_host else str(endpoint.get("host") or ""),
    }
    if not account["endpoint"]:
        raise WarpError("Cloudflare didn't return a WARP endpoint")
    log.info("Registered a free Cloudflare WARP device")
    return account


def _api(method: str, path: str, body: Dict[str, Any], token: str = "") -> Dict[str, Any]:
    headers: Dict[str, str] = dict(_API_HEADERS)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        _API + path, data=json.dumps(body).encode(), headers=headers, method=method
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def _use_ipv6() -> bool:
    return os.getenv("WARP_IPV6", "").strip().lower() in ("1", "true", "yes", "on")


def _wireproxy_config(account: Dict[str, Any], port: int) -> str:
    # IPv4 only by default: YouTube geolocates WARP's IPv6 exits badly ("not made this
    # video available in your country") and bot-checks them more often. Without an
    # IPv6 address in the tunnel, wireproxy connects over IPv4 only.
    addresses: str = f"{account['address_v4']}/32"
    allowed: str = "0.0.0.0/0"
    if _use_ipv6() and account.get("address_v6"):
        addresses += f", {account['address_v6']}/128"
        allowed += ", ::/0"
    return (
        "[Interface]\n"
        f"PrivateKey = {account['private_key']}\n"
        f"Address = {addresses}\n"
        "DNS = 1.1.1.1\n"
        "MTU = 1280\n"
        "\n"
        "[Peer]\n"
        f"PublicKey = {account['peer_public_key']}\n"
        f"AllowedIPs = {allowed}\n"
        f"Endpoint = {account['endpoint']}\n"
        "PersistentKeepalive = 25\n"
        "\n"
        "[http]\n"
        f"BindAddress = 127.0.0.1:{port}\n"
    )


def _trace(proxy: str) -> Dict[str, str]:
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    )
    with opener.open(_TRACE_URL, timeout=15) as response:
        text: str = response.read().decode("utf-8", "replace")
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _write_private(path: str, text: str) -> None:
    descriptor: int = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(text)


def _tail(path: str, lines: int = 3) -> str:
    try:
        with open(path, "rb") as handle:
            content: str = handle.read()[-2000:].decode("utf-8", "replace")
    except OSError:
        return "no wireproxy log"
    return " | ".join(content.strip().splitlines()[-lines:]) or "no wireproxy output"
