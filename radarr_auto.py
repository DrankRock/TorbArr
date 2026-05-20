#!/usr/bin/env python3
"""
radarr_auto.py — Automated missing movie downloader (TorBox + Radarr)

Usage:
    python3 radarr_auto.py --config radarr.json --no-ui --force
    python3 radarr_auto.py --config radarr_fr.json --no-ui
    python3 radarr_auto.py --config radarr.json --retry-deferred --no-ui
"""

import argparse, json, os, sys, time, threading, traceback, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from rich.console import Console

from arr_core import (
    VIDEO_EXT, DiscordNotifier, SkipCache, DashboardState, render_dashboard,
    TorBoxClient, RateLimitError, DeferredItem, DeferredQueue,
    find_files, find_single_file, mk_symlinks, check_mount_health,
    build_search_names, wait_for_download,
    add_common_args, handle_cache_commands, run_with_ui, print_summary, send_completed,
)

SAMPLE_CONFIG = {
    "radarr_url": "http://localhost:7878",
    "radarr_api_key": "YOUR_RADARR_API_KEY",
    "torbox_api_key": "YOUR_TORBOX_API_KEY",
    "min_score": 0,
    "decypharr_mount": "/mnt/decypharr",
    "path_map": {"/movies": "/mnt/media/movies"},
    "threads": 2,
    "poll_interval": 30,
    "init_wait": 60,
    "prefer_dual_audio": False,
    "prefer_1080p": True,
    "rate_limit_probe_interval": 300,
    "torbox_max_slots": 3,
    "discord_interval_minutes": 5,
    "discord": {"all": "", "started": "", "completed": "", "rate_limit": "",
                "error": "", "progress": "", "health": "", "mount_down": ""}
}


@dataclass
class RadarrConfig:
    radarr_url: str = ""
    radarr_api_key: str = ""
    torbox_api_key: str = ""
    min_score: int = 0
    decypharr_mount: str = "/mnt/decypharr"
    path_map: dict = None
    threads: int = 2
    poll_interval: int = 30
    init_wait: int = 60
    max_items: int = 0
    dry_run: bool = False
    prefer_dual_audio: bool = False
    prefer_1080p: bool = True
    rate_limit_probe_interval: int = 300
    torbox_max_slots: int = 3
    discord_interval_minutes: int = 5
    discord: object = None

    def __post_init__(self):
        if self.path_map is None:
            self.path_map = {}

    @classmethod
    def from_json(cls, path):
        with open(path) as f:
            data = json.load(f)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def validate(self):
        errors = []
        if not self.radarr_url:
            errors.append("radarr_url required")
        if not self.radarr_api_key:
            errors.append("radarr_api_key required")
        if not self.torbox_api_key:
            errors.append("torbox_api_key required")
        if not self.path_map:
            errors.append("path_map required")
        return errors


# ── Radarr Client ─────────────────────────────────────────────────────────────

class RadarrClient:
    def __init__(self, url, key):
        self.url = url.rstrip("/")
        self.s = requests.Session()
        self.s.params = {"apikey": key}

    def get_missing(self):
        r = self.s.get(f"{self.url}/api/v3/movie")
        r.raise_for_status()
        return [m for m in r.json() if not m.get("hasFile") and m.get("monitored")]

    def search(self, mid):
        r = self.s.get(f"{self.url}/api/v3/release", params={"movieId": mid}, timeout=90)
        r.raise_for_status()
        return r.json()

    def rescan(self, mid):
        self.s.post(f"{self.url}/api/v3/command", json={"name": "RescanMovie", "movieId": mid})

    def get_movie_path(self, mid):
        r = self.s.get(f"{self.url}/api/v3/movie/{mid}")
        return r.json().get("path", "") if r.status_code == 200 else ""


# ── Harvester ─────────────────────────────────────────────────────────────────

class HarvestItem:
    def __init__(self, movie, tid, info_hash, release_title, search_names,
                 movie_path, radarr, cfg, skip):
        self.movie = movie
        self.tid = tid
        self.info_hash = info_hash
        self.release_title = release_title
        self.search_names = search_names
        self.movie_path = movie_path
        self.radarr = radarr
        self.cfg = cfg
        self.skip = skip
        self.submit_time = time.time()
        self.retry_count = 0
        self.done = False
        self.label = f"{movie['title']} ({movie['year']})"

    @property
    def age_minutes(self):
        return (time.time() - self.submit_time) / 60


class Harvester:
    FIRST = 12
    RETRY = 25
    TIMEOUT = 35
    INTERVAL = 60

    def __init__(self, dash, notifier=None):
        self.pending = []
        self.lock = threading.Lock()
        self.dash = dash
        self.notifier = notifier
        self._stop = threading.Event()
        self.harvested = 0
        self.timed_out = 0

    def add(self, item):
        with self.lock:
            self.pending.append(item)
        self.dash.add_log("INFO", f"[harvester] Queued: {item.label} (check in {self.FIRST}min)")
        with self.dash.lock:
            self.dash._harvester_pending = self.pending_count

    def run(self):
        while not self._stop.is_set():
            self._stop.wait(self.INTERVAL)
            if self._stop.is_set():
                break
            self._check()
        self._check()

    def stop(self):
        self._stop.set()

    @property
    def pending_count(self):
        with self.lock:
            return sum(1 for i in self.pending if not i.done)

    def _check(self):
        with self.lock:
            items = [i for i in self.pending if not i.done]
        for item in items:
            age = item.age_minutes
            if age < self.FIRST:
                continue
            fp = find_single_file(item.cfg.decypharr_mount, item.info_hash,
                                  item.release_title, item.search_names)
            if fp:
                count = mk_symlinks([fp], item.movie_path, item.cfg.path_map)
                if count:
                    item.radarr.rescan(item.movie["id"])
                    self.dash.add_log("SUCCESS", f"[harvester] ✅ {item.label} (after {age:.0f}min)")
                    item.skip.mark_success(str(item.movie["id"]))
                    self.harvested += 1
                    self.dash.inc_success(item.label)
                else:
                    self.dash.add_log("ERROR", f"[harvester] Symlink failed: {item.label}")
                    self.dash.inc_failed()
                item.done = True
                with self.dash.lock:
                    self.dash._harvester_done += 1
                    self.dash._harvester_pending = self.pending_count
                continue
            if age >= self.RETRY and item.retry_count == 0:
                item.retry_count += 1
                self.dash.add_log("WARNING", f"[harvester] Not found after {age:.0f}min: {item.label}")
                continue
            if age >= self.TIMEOUT:
                self.dash.add_log("ERROR", f"[harvester] Timed out after {age:.0f}min: {item.label}")
                item.skip.mark_failed(str(item.movie["id"]), item.label, "mount_timeout")
                self.timed_out += 1
                self.dash.inc_failed()
                item.done = True
                with self.dash.lock:
                    self.dash._harvester_done += 1
                    self.dash._harvester_pending = self.pending_count


# ── Helpers ───────────────────────────────────────────────────────────────────

def sort_key(r, prefer_dual_audio=False, prefer_1080p=True):
    s = r.get("customFormatScore", 0)
    q = r.get("quality", {}).get("quality", {}).get("name", "")
    p = 1 if prefer_1080p and "1080p" in q else 0
    t = r.get("title", "").lower()
    da = 1 if prefer_dual_audio and any(x in t for x in ("multi", "dual", "vff", "vfi", "truefrench")) else 0
    return (s, da, p)


def check_mount_for_movie(mount, title, year):
    st = title.lower()
    for sd in ("__all__", "TorBox", "torrents"):
        sp = os.path.join(mount, sd)
        if not os.path.isdir(sp):
            continue
        try:
            for entry in os.listdir(sp):
                if st in entry.lower() and str(year) in entry:
                    p = os.path.join(sp, entry)
                    if os.path.isfile(p) and p.endswith(VIDEO_EXT):
                        return p
                    if os.path.isdir(p):
                        for root, _, files in os.walk(p):
                            for f in files:
                                if f.endswith(VIDEO_EXT):
                                    fp = os.path.join(root, f)
                                    try:
                                        with open(fp, "rb") as fh:
                                            if len(fh.read(64)) > 0:
                                                return fp
                                    except:
                                        continue
        except OSError:
            continue
    return None


# ── Process ───────────────────────────────────────────────────────────────────

def process(movie, radarr, tb, cfg, dash, skip, harvester, deferred, notifier):
    tn = threading.current_thread().name
    mid, title, year = movie["id"], movie["title"], movie["year"]
    label = f"{title} ({year})"
    dash.set_thread(tn, movie=label, status="checking_mount", detail="", release="", progress=0.0)

    if not check_mount_health(cfg.decypharr_mount, notifier):
        dash.set_thread(tn, status="failed", detail="Mount down!")
        dash.add_log("ERROR", f"[{tn}] Mount down, skipping {label}")
        dash.inc_processed()
        dash.inc_failed()
        return False

    # Mount pre-check
    mount_fp = check_mount_for_movie(cfg.decypharr_mount, title, year)
    if mount_fp:
        movie_path = radarr.get_movie_path(mid)
        if mk_symlinks([mount_fp], movie_path, cfg.path_map):
            radarr.rescan(mid)
            dash.set_thread(tn, status="success", detail="Mount hit ✓")
            dash.inc_movies_done(tn)
            dash.add_log("SUCCESS", f"[{tn}] ✅ {label} already on mount")
            skip.mark_success(str(mid))
            dash.inc_processed()
            dash.inc_success(label)
            return True

    # Search
    dash.set_thread(tn, status="searching", detail="Fetching...")
    try:
        rels = radarr.search(mid)
    except Exception as e:
        dash.set_thread(tn, status="failed", detail=str(e)[:50])
        dash.inc_processed()
        dash.inc_failed()
        return False

    ok = [r for r in rels if r.get("approved") and r.get("customFormatScore", 0) >= cfg.min_score
          and r.get("infoHash")]
    if not ok:
        dash.set_thread(tn, status="no_releases", detail=f"0/{len(rels)}")
        skip.mark_failed(str(mid), label, "no_releases")
        dash.inc_processed()
        dash.inc_skipped()
        dash.inc_no_releases()
        return False

    ok.sort(key=lambda r: sort_key(r, cfg.prefer_dual_audio, cfg.prefer_1080p), reverse=True)
    dash.add_log("INFO", f"[{tn}] {label}: {len(ok)} releases")

    # Cache check
    dash.set_thread(tn, status="checking_cache", detail=f"{len(ok)} hashes")
    cm = {}
    hs = [r["infoHash"] for r in ok]
    for i in range(0, len(hs), 50):
        cm.update(tb.cached(hs[i:i + 50]))

    cached = [r for r in ok if cm.get(r["infoHash"])]
    if cached:
        dash.add_log("INFO", f"[{tn}] {len(cached)}/{len(ok)} cached")
        cached.sort(key=lambda r: sort_key(r, cfg.prefer_dual_audio, cfg.prefer_1080p), reverse=True)
        candidates = cached + [r for r in ok if not cm.get(r["infoHash"])]
    else:
        dash.add_log("INFO", f"[{tn}] 0/{len(ok)} cached")
        candidates = ok

    # Submit
    for i, rel in enumerate(candidates):
        if cfg.dry_run:
            dash.set_thread(tn, status="success", detail="Dry run")
            dash.inc_processed()
            dash.inc_success(label)
            return True

        h = rel["infoHash"]
        rt = rel.get("title", h[:16])
        is_cached = cm.get(h, False)
        dash.set_thread(tn, status="downloading", release=rt[:55],
                        detail="CACHED!" if is_cached else f"Try {i + 1}/{len(candidates)}")
        dash.add_log("SUCCESS" if is_cached else "INFO",
                     f"[{tn}] {'Cache hit' if is_cached else 'Trying'}: {rt[:45]}")
        if is_cached:
            dash.inc_cached()

        tid = None
        if is_cached:
            tid = tb._find_by_hash(h)

        if not tid:
            if tb.is_rate_limited():
                dash.add_log("INFO", f"[{tn}] Rate limited, deferring: {rt[:40]}")
                deferred.add(DeferredItem(movie, rel, is_cached))
                with dash.lock:
                    dash._deferred_count = deferred.count
                dash.set_thread(tn, status="rate_limited", detail="Deferred")
                dash.inc_processed()
                return "deferred"
            try:
                tid = tb.add(h)
            except RateLimitError:
                dash.add_log("WARNING", f"[{tn}] Hit 429, deferring: {rt[:40]}")
                deferred.add(DeferredItem(movie, rel, is_cached))
                with dash.lock:
                    dash._deferred_count = deferred.count
                dash.set_thread(tn, status="rate_limited", detail="Deferred")
                dash.inc_processed()
                return "deferred"

        if not tid:
            dash.add_log("WARNING", f"[{tn}] TB add failed ({tb._last_error}): {rt[:35]}")
            continue

        dash.add_log("INFO", f"[{tn}] TB accepted {rt[:40]} (id={tid})")

        if not is_cached:
            info = wait_for_download(tb, tid, h, rt, cfg.poll_interval, cfg.init_wait, dash, tn)
            if not info:
                continue

        time.sleep(10)
        info = tb.info(tid)
        search_names = build_search_names(info, h, rt) if info else [rt, h]
        fp = find_single_file(cfg.decypharr_mount, h, rt, search_names)
        if fp:
            movie_path = radarr.get_movie_path(mid)
            if mk_symlinks([fp], movie_path, cfg.path_map):
                radarr.rescan(mid)
                dash.set_thread(tn, status="success", detail="TorBox ✓")
                dash.inc_movies_done(tn)
                dash.add_log("SUCCESS", f"[{tn}] ✅ {label}")
                skip.mark_success(str(mid))
                dash.inc_processed()
                dash.inc_success(label)
                return True

        movie_path = radarr.get_movie_path(mid)
        harvester.add(HarvestItem(movie, tid, h, rt, search_names, movie_path, radarr, cfg, skip))
        dash.set_thread(tn, status="success", detail="Queued for harvest")
        dash.inc_processed()
        return True

    dash.set_thread(tn, status="failed", detail="All failed")
    dash.add_log("WARNING", f"[{tn}] ✗ {label}")
    skip.mark_failed(str(mid), label, "all_failed")
    dash.inc_processed()
    dash.inc_failed()
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Radarr Auto Downloader (TorBox)")
    add_common_args(parser)
    args = parser.parse_args()
    con = Console()

    if not os.path.exists(args.config):
        con.print(f"[yellow]Config not found: {args.config}[/yellow]")
        with open(args.config, "w") as f:
            json.dump(SAMPLE_CONFIG, f, indent=2)
        con.print(f"[green]Sample written to {args.config}[/green]")
        sys.exit(1)

    cfg = RadarrConfig.from_json(args.config)
    errors = cfg.validate()
    if errors:
        for e in errors:
            con.print(f"[red]{e}[/red]")
        sys.exit(1)

    if args.dry_run:
        cfg.dry_run = True
    if args.max > 0:
        cfg.max_items = args.max
    if args.threads > 0:
        cfg.threads = args.threads

    notifier = DiscordNotifier(cfg.discord)
    config_dir = os.path.dirname(os.path.abspath(args.config))
    config_name = os.path.splitext(os.path.basename(args.config))[0]
    skip = SkipCache(os.path.join(config_dir, f"skip_{config_name}.json"), cooldown_days=args.cooldown)
    deferred_path = os.path.join(config_dir, f"deferred_{config_name}.json")

    if handle_cache_commands(args, skip, deferred_path, con):
        return

    con.print("[bold cyan]Fetching missing movies...[/bold cyan]")
    radarr = RadarrClient(cfg.radarr_url, cfg.radarr_api_key)
    tb = TorBoxClient(cfg.torbox_api_key, max_slots=cfg.torbox_max_slots)

    con.print("[dim]Cleaning TorBox queue...[/dim]")
    dead = tb.cleanup()
    if dead:
        con.print(f"[dim]Cleaned {dead} dead torrents[/dim]")

    missing = radarr.get_missing()

    if args.retry_deferred:
        dq = DeferredQueue(persist_path=deferred_path)
        ids = set(dq.get_saved_ids())
        if not ids:
            con.print("[dim]No deferred movies[/dim]")
            return
        missing = [m for m in missing if m["id"] in ids]
        con.print(f"[bold yellow]Retrying {len(missing)} deferred movies[/bold yellow]")
        dq.clear_saved()
    else:
        if cfg.max_items > 0:
            missing = missing[:cfg.max_items]
        if not args.force:
            before = len(missing)
            missing = [m for m in missing if not skip.should_skip(str(m["id"]))[0]]
            sk = before - len(missing)
            if sk:
                con.print(f"[dim]Skipping {sk} movies (cooldown)[/dim]")
        else:
            con.print("[yellow]Force mode[/yellow]")

    con.print(f"[bold]{len(missing)}[/bold] missing movies")
    if not missing:
        con.print("[green]Nothing to do![/green]")
        return

    if not check_mount_health(cfg.decypharr_mount, notifier):
        con.print(f"[bold red]Mount down! Run: sudo umount -l {cfg.decypharr_mount} && docker restart decypharr[/bold red]")
        sys.exit(1)

    dash = DashboardState(len(missing), cfg.threads, title=f"Radarr Auto ({config_name})")
    dash.set_logfile(os.path.join(config_dir, f"{config_name}.log"))
    dash.set_notifier(notifier)
    dash.add_log("INFO", f"Started — {len(missing)} movies, {cfg.threads} threads")
    tb = TorBoxClient(cfg.torbox_api_key, max_slots=cfg.torbox_max_slots, dash=dash, notifier=notifier)

    notifier.send("started", f"Radarr ({config_name}) started",
                  fields={"Movies": str(len(missing)), "Threads": str(cfg.threads)})
    dash.start_status_ticker(cfg.discord_interval_minutes)

    harvester = Harvester(dash, notifier)
    threading.Thread(target=harvester.run, name="harvester", daemon=True).start()
    deferred = DeferredQueue(persist_path=deferred_path)

    def process_deferred():
        items = deferred.drain()
        if not items:
            return
        with dash.lock:
            dash._deferred_count = 0
        dash.add_log("INFO", f"Retrying {len(items)} deferred...")
        with ThreadPoolExecutor(max_workers=cfg.threads, thread_name_prefix="worker") as pool:
            futs = {pool.submit(process, di.item, radarr, tb, cfg, dash, skip, harvester, deferred, notifier): di
                    for di in items}
            for f in as_completed(futs):
                try:
                    f.result()
                except:
                    dash.inc_processed()
                    dash.inc_failed()

    def run():
        try:
            with ThreadPoolExecutor(max_workers=cfg.threads, thread_name_prefix="worker") as pool:
                futs = {pool.submit(process, m, radarr, tb, cfg, dash, skip, harvester, deferred, notifier): m
                        for m in missing}
                for f in as_completed(futs):
                    try:
                        f.result()
                    except:
                        dash.inc_processed()
                        dash.inc_failed()

            while deferred.count > 0:
                dash.add_log("INFO", f"⏸ Waiting for rate limit ({deferred.count} deferred)...")
                for tn in dash.threads:
                    dash.set_thread(tn, status="rate_limited", movie="", detail=f"{deferred.count} deferred")
                tb.wait_for_rate_limit(cfg.rate_limit_probe_interval)
                dash.add_log("INFO", "Rate limit lifted, retrying...")
                process_deferred()

            if harvester.pending_count > 0:
                dash.add_log("INFO", f"[harvester] Waiting for {harvester.pending_count} pending...")
                while harvester.pending_count > 0:
                    time.sleep(10)
            harvester.stop()
        except Exception as e:
            tb_str = traceback.format_exc()
            dash.add_log("ERROR", f"FATAL: {e}")
            notifier.send("error", "Radarr script crashed!",
                          f"```\n{tb_str[-1500:]}\n```\n"
                          f"Restart: `python3 radarr_auto.py --config {args.config} --retry-deferred --no-ui`")
            raise

    run_with_ui(run, dash, args, con)
    dash.stop_status_ticker()
    sn = print_summary(dash, harvester, con)
    send_completed(notifier, sn, harvester, deferred)


if __name__ == "__main__":
    main()
