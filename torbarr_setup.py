#!/usr/bin/env python3
"""
torbarr_setup.py — Generate TorbArr config files from a docker-compose.yml or interactively.

Usage:
    python3 torbarr_setup.py --compose ~/docker/docker-compose.yml --torbox-key YOUR_KEY
    python3 torbarr_setup.py --interactive
    python3 torbarr_setup.py --compose docker-compose.yml --torbox-key KEY --mount /mnt/decypharr --discord https://discord.com/api/webhooks/...
"""

import argparse, json, os, re, sys

try:
    import yaml
except ImportError:
    print("pyyaml required: pip install pyyaml")
    sys.exit(1)

# ── Service detection ─────────────────────────────────────────────────────────

# Known arr images and what they are
ARR_PATTERNS = {
    "radarr": {"type": "radarr", "default_port": 7878, "url_key": "radarr_url", "api_key_key": "radarr_api_key"},
    "sonarr": {"type": "sonarr", "default_port": 8989, "url_key": "sonarr_url", "api_key_key": "sonarr_api_key"},
}

# Volume mount patterns to ignore (not media paths)
IGNORE_MOUNTS = {"/config", "/app", "/dev", "/etc", "/tmp", "/var", "/run", "/proc", "/sys"}


def detect_arr_type(service_name, service_cfg):
    """Figure out if a service is a radarr or sonarr by name or image."""
    name_lower = service_name.lower()
    image = (service_cfg.get("image") or "").lower()

    for pattern, info in ARR_PATTERNS.items():
        if pattern in name_lower or pattern in image:
            return info, pattern
    return None, None


def extract_port(service_cfg, default_port):
    """Get the host port from a docker-compose port mapping."""
    ports = service_cfg.get("ports", [])
    for p in ports:
        p = str(p)
        # "7878:7878" or "7879:7878" or "0.0.0.0:7878:7878"
        parts = p.replace("/tcp", "").replace("/udp", "").split(":")
        if len(parts) == 2:
            return int(parts[0])
        elif len(parts) == 3:
            return int(parts[1])
    return default_port


def extract_volumes(service_cfg):
    """Extract volume mounts, return list of (host_path, container_path)."""
    volumes = service_cfg.get("volumes", [])
    results = []
    for v in volumes:
        v = str(v)
        # Remove :ro, :rw, :rshared etc
        v = re.sub(r':(ro|rw|rshared|rslave|rprivate|shared|slave|private|z|Z)$', '', v)
        parts = v.split(":")
        if len(parts) >= 2:
            host = parts[0]
            container = parts[1]
            # Skip config/system mounts
            if any(container.startswith(ig) for ig in IGNORE_MOUNTS):
                continue
            # Skip relative paths (./config, ./data)
            if host.startswith("./") or host.startswith("../"):
                continue
            # Skip named volumes (no / at start)
            if not host.startswith("/"):
                continue
            results.append((host, container))
    return results


def build_path_map(volumes):
    """Convert volume mounts to path_map (container_path → host_path)."""
    path_map = {}
    for host, container in volumes:
        path_map[container] = host
    return path_map


def find_decypharr_mount(compose_data, compose_dir="."):
    """Try to detect the decypharr mount path from compose."""
    services = compose_data.get("services", {})
    for name, cfg in services.items():
        if "decypharr" in name.lower() or "blackhole" in (cfg.get("image") or "").lower():
            for v in cfg.get("volumes", []):
                vs = str(v)
                # Look for rshared mounts first (that's the FUSE mount)
                if "rshared" in vs:
                    host_part = vs.split(":")[0]
                    return _resolve_path(host_part, compose_dir)
            # Fallback: look for /mnt paths
            for v in cfg.get("volumes", []):
                host_part = str(v).split(":")[0]
                if host_part.startswith("/mnt/"):
                    return host_part
            # Check environment for mount path
            env = cfg.get("environment", {})
            if isinstance(env, list):
                for e in env:
                    if "MOUNT" in str(e).upper():
                        val = str(e).split("=", 1)[-1]
                        if val.startswith("/"):
                            return val
            elif isinstance(env, dict):
                for k, val in env.items():
                    if "MOUNT" in k.upper() and str(val).startswith("/"):
                        return str(val)
    return "/mnt/decypharr"


def _resolve_path(p, compose_dir):
    """Resolve a path relative to the compose file directory."""
    if p.startswith("/"):
        return p
    return os.path.abspath(os.path.join(compose_dir, p))


def generate_config_name(service_name, arr_type):
    """Generate a config filename from the service name."""
    name = service_name.lower().replace("-", "_").replace(" ", "_")
    # If name already contains the arr type, use it as-is
    if arr_type in name:
        return name
    return name


# ── Config generation ─────────────────────────────────────────────────────────

def make_radarr_config(url, api_key, torbox_key, mount, path_map, discord="", interval=5):
    return {
        "radarr_url": url,
        "radarr_api_key": api_key,
        "torbox_api_key": torbox_key,
        "min_score": 0,
        "decypharr_mount": mount,
        "path_map": path_map,
        "threads": 2,
        "poll_interval": 30,
        "init_wait": 60,
        "prefer_dual_audio": False,
        "prefer_1080p": True,
        "rate_limit_probe_interval": 300,
        "torbox_max_slots": 3,
        "discord_interval_minutes": interval,
        "discord": {"all": discord} if discord else {"all": ""},
    }


def make_sonarr_config(url, api_key, torbox_key, mount, path_map, discord="", interval=5):
    return {
        "sonarr_url": url,
        "sonarr_api_key": api_key,
        "torbox_api_key": torbox_key,
        "min_score": 0,
        "decypharr_mount": mount,
        "path_map": path_map,
        "threads": 2,
        "poll_interval": 30,
        "init_wait": 60,
        "prefer_1080p": True,
        "rate_limit_probe_interval": 300,
        "torbox_max_slots": 3,
        "discord_interval_minutes": interval,
        "discord": {"all": discord} if discord else {"all": ""},
    }


# ── Compose mode ──────────────────────────────────────────────────────────────

def from_compose(compose_path, torbox_key, mount_override=None, discord_url="",
                 output_dir=".", api_keys=None):
    with open(compose_path) as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    if not services:
        print(f"No services found in {compose_path}")
        return []

    mount = mount_override or find_decypharr_mount(compose, os.path.dirname(os.path.abspath(compose_path)))
    print(f"Decypharr mount: {mount}")

    detected = []

    for svc_name, svc_cfg in services.items():
        info, pattern = detect_arr_type(svc_name, svc_cfg)
        if not info:
            continue

        port = extract_port(svc_cfg, info["default_port"])
        volumes = extract_volumes(svc_cfg)
        path_map = build_path_map(volumes)

        if not path_map:
            print(f"  ⚠ {svc_name}: no media volume mounts detected, skipping")
            continue

        url = f"http://localhost:{port}"
        config_name = generate_config_name(svc_name, pattern)

        # Try to get API key from provided keys or prompt
        api_key = ""
        if api_keys and svc_name in api_keys:
            api_key = api_keys[svc_name]
        elif api_keys and config_name in api_keys:
            api_key = api_keys[config_name]

        detected.append({
            "service_name": svc_name,
            "config_name": config_name,
            "type": info["type"],
            "url": url,
            "port": port,
            "path_map": path_map,
            "api_key": api_key,
            "url_key": info["url_key"],
            "api_key_key": info["api_key_key"],
        })

    if not detected:
        print("No Radarr/Sonarr services found in compose file.")
        return []

    print(f"\nFound {len(detected)} arr service(s):\n")
    generated = []

    for d in detected:
        print(f"  {d['service_name']}")
        print(f"    Type:     {d['type']}")
        print(f"    URL:      {d['url']}")
        print(f"    Path map: {d['path_map']}")

        api_key = d["api_key"]
        if not api_key:
            api_key = input(f"    API key for {d['service_name']}: ").strip()
            if not api_key:
                api_key = "REPLACE_ME"
                print(f"    ⚠ No key provided, wrote placeholder")

        if d["type"] == "radarr":
            # Detect if this might be a foreign language instance
            name_lower = d["service_name"].lower()
            dual = any(x in name_lower for x in ("fr", "french", "vf", "multi"))
            cfg = make_radarr_config(d["url"], api_key, torbox_key, mount,
                                     d["path_map"], discord_url)
            if dual:
                cfg["prefer_dual_audio"] = True
                print(f"    ℹ Detected foreign language instance, enabled prefer_dual_audio")
        else:
            cfg = make_sonarr_config(d["url"], api_key, torbox_key, mount,
                                     d["path_map"], discord_url)

        out_path = os.path.join(output_dir, f"{d['config_name']}.json")
        with open(out_path, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"    → Wrote {out_path}")
        generated.append((out_path, d["type"]))
        print()

    return generated


# ── Interactive mode ──────────────────────────────────────────────────────────

def interactive(output_dir="."):
    print("TorbArr interactive setup\n")

    torbox_key = input("TorBox API key: ").strip()
    if not torbox_key:
        print("Need a TorBox key.")
        return

    mount = input("Decypharr mount path [/mnt/decypharr]: ").strip() or "/mnt/decypharr"
    discord_url = input("Discord webhook URL (blank to skip): ").strip()

    generated = []

    while True:
        print(f"\n--- Add an arr instance (or 'done' to finish) ---")
        arr_type = input("Type (radarr/sonarr): ").strip().lower()
        if arr_type in ("done", "q", "quit", "exit", ""):
            break
        if arr_type not in ("radarr", "sonarr"):
            print("Must be 'radarr' or 'sonarr'")
            continue

        info = ARR_PATTERNS[arr_type]
        name = input(f"Config name [{arr_type}]: ").strip() or arr_type
        url = input(f"URL [http://localhost:{info['default_port']}]: ").strip()
        url = url or f"http://localhost:{info['default_port']}"
        api_key = input("API key: ").strip() or "REPLACE_ME"

        print("\nPath map — maps container paths to host paths.")
        print("Example: container=/movies host=/mnt/media/movies")
        path_map = {}
        while True:
            cp = input("  Container path (blank to finish): ").strip()
            if not cp:
                break
            hp = input(f"  Host path for {cp}: ").strip()
            if hp:
                path_map[cp] = hp

        if not path_map:
            print("  ⚠ No path map entries, you'll need to add them manually")
            path_map = {f"/{arr_type.replace('arr', '')}": f"/mnt/media/{arr_type.replace('arr', '')}"}
            print(f"  Using default: {path_map}")

        if arr_type == "radarr":
            dual = input("Prefer dual audio? (y/N): ").strip().lower() == "y"
            cfg = make_radarr_config(url, api_key, torbox_key, mount, path_map, discord_url)
            if dual:
                cfg["prefer_dual_audio"] = True
        else:
            cfg = make_sonarr_config(url, api_key, torbox_key, mount, path_map, discord_url)

        out_path = os.path.join(output_dir, f"{name}.json")
        with open(out_path, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"  → Wrote {out_path}")
        generated.append((out_path, arr_type))

    if generated:
        print(f"\nDone. Generated {len(generated)} config(s).")
        for path, cfg_type in generated:
            script = "radarr_auto.py" if cfg_type == "radarr" else "sonarr_auto.py"
            print(f"Test: python3 {script} --config {path} --dry-run --max 3 --no-ui")
    else:
        print("No configs generated.")


# ── Fetch API keys automatically ──────────────────────────────────────────────

def try_fetch_api_key(url):
    """Try to fetch the API key from an arr instance (requires no auth or form auth)."""
    try:
        import requests
        # Try the initialize endpoint (works on some setups)
        r = requests.get(f"{url}/initialize.json", timeout=5)
        if r.status_code == 200:
            data = r.json()
            key = data.get("apiKey")
            if key:
                return key
        # Try system/status with no key (some have auth disabled)
        r = requests.get(f"{url}/api/v3/system/status", timeout=5)
        if r.status_code == 200:
            return "(no key needed?)"
    except:
        pass
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate TorbArr config files from docker-compose.yml or interactively.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 torbarr_setup.py --compose docker-compose.yml --torbox-key KEY
  python3 torbarr_setup.py --compose docker-compose.yml --torbox-key KEY --mount /mnt/decypharr
  python3 torbarr_setup.py --interactive
  python3 torbarr_setup.py --compose docker-compose.yml --torbox-key KEY --key radarr=XXXX --key sonarr=YYYY
""")
    parser.add_argument("--compose", type=str, help="Path to docker-compose.yml")
    parser.add_argument("--torbox-key", type=str, help="TorBox API key")
    parser.add_argument("--mount", type=str, help="Override decypharr mount path")
    parser.add_argument("--discord", type=str, default="", help="Discord webhook URL for all categories")
    parser.add_argument("--output", type=str, default=".", help="Output directory for configs")
    parser.add_argument("--interactive", action="store_true", help="Interactive setup (no compose needed)")
    parser.add_argument("--key", action="append", type=str, default=[],
                        help="API key as service=key (repeatable), e.g. --key radarr=XXXX --key sonarr=YYYY")
    parser.add_argument("--auto-detect-keys", action="store_true",
                        help="Try to fetch API keys from running arr instances")
    args = parser.parse_args()

    if args.interactive:
        interactive(args.output)
        return

    if not args.compose:
        print("Need --compose or --interactive")
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(args.compose):
        print(f"File not found: {args.compose}")
        sys.exit(1)

    if not args.torbox_key:
        args.torbox_key = input("TorBox API key: ").strip()
        if not args.torbox_key:
            print("Need a TorBox key.")
            sys.exit(1)

    # Parse --key flags
    api_keys = {}
    for kv in args.key:
        if "=" in kv:
            k, v = kv.split("=", 1)
            api_keys[k.strip()] = v.strip()

    os.makedirs(args.output, exist_ok=True)

    generated = from_compose(args.compose, args.torbox_key, args.mount, args.discord,
                             args.output, api_keys)

    if generated:
        print(f"\nGenerated {len(generated)} config(s).")
        print("Next steps:")
        print(f"  1. Check the configs and fill in any REPLACE_ME API keys")
        print(f"  2. Test each config:")
        for path, cfg_type in generated:
            script = "radarr_auto.py" if cfg_type == "radarr" else "sonarr_auto.py"
            print(f"     python3 {script} --config {path} --dry-run --max 3")
        print(f"  3. Run for real:")
        for path, cfg_type in generated:
            script = "radarr_auto.py" if cfg_type == "radarr" else "sonarr_auto.py"
            print(f"     nohup python3 {script} --config {path} --no-ui --force > /dev/null 2>&1 &")


if __name__ == "__main__":
    main()