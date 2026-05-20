#!/usr/bin/env python3
"""
sonarr_auto.py — Automated missing TV show downloader (TorBox + Sonarr)

3-pass architecture: multi-season packs → season packs → singles.

Usage:
    python3 sonarr_auto.py --config sonarr.json --packs-only --no-ui --force
    python3 sonarr_auto.py --config sonarr.json --series "South Park" --max 5
    python3 sonarr_auto.py --config sonarr.json --retry-deferred --no-ui
"""

import argparse, json, os, re, sys, time, threading, traceback, requests
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
    "sonarr_url": "http://localhost:8989",
    "sonarr_api_key": "YOUR_SONARR_API_KEY",
    "torbox_api_key": "YOUR_TORBOX_API_KEY",
    "min_score": 0,
    "decypharr_mount": "/mnt/decypharr",
    "path_map": {"/tv": "/mnt/media/tv", "/anime": "/mnt/media/anime"},
    "threads": 2,
    "poll_interval": 30,
    "init_wait": 60,
    "prefer_1080p": True,
    "rate_limit_probe_interval": 300,
    "torbox_max_slots": 3,
    "discord_interval_minutes": 5,
    "discord": {"all": "", "started": "", "completed": "", "rate_limit": "",
                "error": "", "progress": "", "health": "", "mount_down": ""}
}


@dataclass
class SonarrConfig:
    sonarr_url: str = ""
    sonarr_api_key: str = ""
    torbox_api_key: str = ""
    min_score: int = 0
    decypharr_mount: str = "/mnt/decypharr"
    path_map: dict = None
    threads: int = 2
    poll_interval: int = 30
    init_wait: int = 60
    max_items: int = 0
    dry_run: bool = False
    prefer_1080p: bool = True
    packs_only: bool = False
    rate_limit_probe_interval: int = 300
    torbox_max_slots: int = 3
    discord_interval_minutes: int = 5
    discord: object = None

    def __post_init__(self):
        if self.path_map is None:
            self.path_map = {"/tv": "/mnt/media/tv", "/anime": "/mnt/media/anime"}

    @classmethod
    def from_json(cls, path):
        with open(path) as f:
            data = json.load(f)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def validate(self):
        errors = []
        if not self.sonarr_url:
            errors.append("sonarr_url required")
        if not self.sonarr_api_key:
            errors.append("sonarr_api_key required")
        if not self.torbox_api_key:
            errors.append("torbox_api_key required")
        if not self.path_map:
            errors.append("path_map required")
        return errors


# ── Sonarr Client ─────────────────────────────────────────────────────────────

class SonarrClient:
    def __init__(self, url, key):
        self.url = url.rstrip("/")
        self.s = requests.Session()
        self.s.params = {"apikey": key}

    def get_series(self):
        r = self.s.get(f"{self.url}/api/v3/series")
        r.raise_for_status()
        return r.json()

    def get_episodes(self, series_id):
        r = self.s.get(f"{self.url}/api/v3/episode", params={"seriesId": series_id})
        r.raise_for_status()
        return r.json()

    def search_releases(self, episode_id):
        r = self.s.get(f"{self.url}/api/v3/release", params={"episodeId": episode_id}, timeout=90)
        r.raise_for_status()
        return r.json()

    def rescan_series(self, series_id):
        self.s.post(f"{self.url}/api/v3/command", json={"name": "RescanSeries", "seriesId": series_id})
        self.s.post(f"{self.url}/api/v3/command", json={"name": "RefreshSeries", "seriesId": series_id})

    def get_missing_seasons(self):
        series_list = self.get_series()
        results = []
        for show in series_list:
            if not show.get("monitored"):
                continue
            stats = show.get("statistics", {})
            if stats.get("episodeCount", 0) <= stats.get("episodeFileCount", 0):
                continue
            sid = show["id"]
            try:
                episodes = self.get_episodes(sid)
            except:
                continue
            missing_eps = [e for e in episodes
                          if not e.get("hasFile") and e.get("monitored") and e.get("seasonNumber", 0) > 0]
            seasons = {}
            for e in missing_eps:
                sn = e["seasonNumber"]
                if sn not in seasons:
                    seasons[sn] = []
                seasons[sn].append(e)
            for sn, eps in sorted(seasons.items()):
                results.append({
                    "series_id": sid, "series_title": show["title"],
                    "series_year": show.get("year", 0), "series_path": show.get("path", ""),
                    "season_number": sn, "missing_count": len(eps),
                    "first_episode_id": eps[0]["id"],
                    "imdb_rating": show.get("ratings", {}).get("value", 0),
                })
        return results


# ── Harvester ─────────────────────────────────────────────────────────────────

class HarvestItem:
    def __init__(self, sid, sn, stitle, spath, tid, info_hash, release_title,
                 search_names, is_pack, sonarr, cfg, skip):
        self.sid = sid
        self.sn = sn
        self.stitle = stitle
        self.spath = spath
        self.tid = tid
        self.info_hash = info_hash
        self.release_title = release_title
        self.search_names = search_names
        self.is_pack = is_pack
        self.sonarr = sonarr
        self.cfg = cfg
        self.skip = skip
        self.submit_time = time.time()
        self.retry_count = 0
        self.done = False
        self.label = f"{stitle} S{sn:02d}"

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
            if item.is_pack:
                files = find_files(item.cfg.decypharr_mount, item.info_hash,
                                   item.release_title, item.search_names)
            else:
                f = find_single_file(item.cfg.decypharr_mount, item.info_hash,
                                     item.release_title, item.search_names)
                files = [f] if f else []
            if files:
                count = mk_symlinks(files, item.spath, item.cfg.path_map)
                if count:
                    item.sonarr.rescan_series(item.sid)
                    self.dash.add_log("SUCCESS",
                                      f"[harvester] ✅ {item.label} — {len(files)} files (after {age:.0f}min)")
                    item.skip.mark_success(f"{item.sid}_S{item.sn:02d}")
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
                item.skip.mark_failed(f"{item.sid}_S{item.sn:02d}", item.label, "mount_timeout")
                self.timed_out += 1
                self.dash.inc_failed()
                item.done = True
                with self.dash.lock:
                    self.dash._harvester_done += 1
                    self.dash._harvester_pending = self.pending_count


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_multi_season_pack(title):
    if re.search(r'S\d{1,2}[\s._-]+S\d{1,2}', title, re.IGNORECASE):
        return True
    if re.search(r'S\d{1,2}-S\d{1,2}', title, re.IGNORECASE):
        return True
    if re.search(r'complete\s*(series|collection)', title, re.IGNORECASE):
        return True
    return False


def sort_key(r, prefer_1080p=True):
    s = r.get("customFormatScore", 0)
    q = r.get("quality", {}).get("quality", {}).get("name", "")
    p = 1 if prefer_1080p and "1080p" in q else 0
    title = r.get("title", "")
    tier = 2 if r.get("fullSeason") and is_multi_season_pack(title) else (1 if r.get("fullSeason") else 0)
    return (tier, s, p, -r.get("size", 0))


def check_mount_for_season(mount, series_title, season_number):
    st = series_title.lower()
    tag = f"s{season_number:02d}"
    for sd in ("__all__", "TorBox", "torrents"):
        sp = os.path.join(mount, sd)
        if not os.path.isdir(sp):
            continue
        try:
            for entry in os.listdir(sp):
                el = entry.lower()
                if st in el and tag in el:
                    p = os.path.join(sp, entry)
                    if os.path.isdir(p):
                        vids = []
                        for root, _, files in os.walk(p):
                            for f in files:
                                if f.endswith(VIDEO_EXT):
                                    fp = os.path.join(root, f)
                                    try:
                                        with open(fp, "rb") as fh:
                                            if len(fh.read(64)) > 0:
                                                vids.append(fp)
                                    except:
                                        continue
                        if vids:
                            return vids
        except OSError:
            continue
    return []


# ── Process Season ────────────────────────────────────────────────────────────

def process_season(item, sonarr, tb, cfg, dash, skip, harvester, deferred, notifier, tier_filter=None):
    tn = threading.current_thread().name
    sid = item["series_id"]
    sn = item["season_number"]
    stitle = item["series_title"]
    spath = item["series_path"]
    skip_key = f"{sid}_S{sn:02d}"
    label = f"{stitle} S{sn:02d}" if tier_filter != 2 else f"{stitle} (full series)"
    dash.set_thread(tn, movie=label, status="checking_mount", detail="", release="", progress=0.0)

    if not check_mount_health(cfg.decypharr_mount, notifier):
        dash.set_thread(tn, status="failed", detail="Mount down!")
        dash.inc_processed()
        dash.inc_failed()
        return False

    # Mount pre-check
    mount_files = check_mount_for_season(cfg.decypharr_mount, stitle, sn)
    if mount_files:
        if mk_symlinks(mount_files, spath, cfg.path_map):
            sonarr.rescan_series(sid)
            dash.set_thread(tn, status="success", detail=f"Mount ✓ {len(mount_files)} files")
            dash.add_log("SUCCESS", f"[{tn}] ✅ {label} on mount — {len(mount_files)} files")
            skip.mark_success(skip_key)
            dash.inc_processed()
            dash.inc_success(label)
            return True

    # Search
    dash.set_thread(tn, status="searching", detail="Searching...")
    try:
        rels = sonarr.search_releases(item["first_episode_id"])
    except Exception as e:
        dash.set_thread(tn, status="failed", detail=str(e)[:50])
        dash.inc_processed()
        dash.inc_failed()
        return False

    ok = [r for r in rels if r.get("infoHash") and r.get("customFormatScore", 0) >= cfg.min_score
          and (r.get("approved") or r.get("fullSeason"))]
    if not ok:
        dash.set_thread(tn, status="no_releases", detail=f"0/{len(rels)}")
        skip.mark_failed(skip_key, label, "no_releases")
        dash.inc_processed()
        dash.inc_skipped()
        dash.inc_no_releases()
        return False

    ok.sort(key=lambda r: sort_key(r, cfg.prefer_1080p), reverse=True)

    multi = [r for r in ok if r.get("fullSeason") and is_multi_season_pack(r.get("title", ""))]
    season = [r for r in ok if r.get("fullSeason") and not is_multi_season_pack(r.get("title", ""))]
    singles = [r for r in ok if not r.get("fullSeason")]

    dash.add_log("INFO", f"[{tn}] {label}: {len(ok)} releases "
                 f"({len(multi)} multi, {len(season)} season, {len(singles)} singles)")

    if tier_filter == 2:
        candidates_pool = multi
    elif tier_filter == 1:
        candidates_pool = multi + season
    elif tier_filter == 0:
        candidates_pool = singles
    else:
        candidates_pool = multi + season + ([] if cfg.packs_only else singles)

    if not candidates_pool:
        dash.set_thread(tn, status="no_releases", detail="No matching tier")
        dash.inc_processed()
        dash.inc_skipped()
        return False

    # Cache check
    dash.set_thread(tn, status="checking_cache", detail=f"{len(candidates_pool)} hashes")
    cm = {}
    hs = [r["infoHash"] for r in candidates_pool]
    for i in range(0, len(hs), 50):
        cm.update(tb.cached(hs[i:i + 50]))

    cached = [r for r in candidates_pool if cm.get(r["infoHash"])]
    uncached = [r for r in candidates_pool if not cm.get(r["infoHash"])]
    if cached:
        dash.add_log("INFO", f"[{tn}] {len(cached)}/{len(candidates_pool)} cached")
    cached.sort(key=lambda r: sort_key(r, cfg.prefer_1080p), reverse=True)
    uncached.sort(key=lambda r: sort_key(r, cfg.prefer_1080p), reverse=True)
    candidates = cached + uncached

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
        is_pack = rel.get("fullSeason", False)
        is_multi = is_multi_season_pack(rt)
        tag = "📚" if is_multi else ("📦" if is_pack else "📄")

        dash.set_thread(tn, status="downloading", release=rt[:55],
                        detail=f"{tag} {'CACHED' if is_cached else f'Try {i+1}/{len(candidates)}'}")
        dash.add_log("SUCCESS" if is_cached else "INFO",
                     f"[{tn}] {tag} {'Cache hit' if is_cached else 'Trying'}: {rt[:50]}")
        if is_cached:
            dash.inc_cached()

        tid = None
        if is_cached:
            tid = tb._find_by_hash(h)

        if not tid:
            if tb.is_rate_limited():
                dash.add_log("INFO", f"[{tn}] Rate limited, deferring: {rt[:40]}")
                deferred.add(DeferredItem(item, rel, is_cached))
                with dash.lock:
                    dash._deferred_count = deferred.count
                dash.set_thread(tn, status="rate_limited", detail="Deferred")
                dash.inc_processed()
                return "deferred"
            try:
                tid = tb.add(h)
            except RateLimitError:
                dash.add_log("WARNING", f"[{tn}] Hit 429, deferring: {rt[:40]}")
                deferred.add(DeferredItem(item, rel, is_cached))
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

        if is_pack:
            files = find_files(cfg.decypharr_mount, h, rt, search_names)
        else:
            f = find_single_file(cfg.decypharr_mount, h, rt, search_names)
            files = [f] if f else []

        if files:
            count = mk_symlinks(files, spath, cfg.path_map)
            if count:
                sonarr.rescan_series(sid)
                dash.set_thread(tn, status="success", detail=f"{tag} ✓ {len(files)} files")
                dash.add_log("SUCCESS", f"[{tn}] ✅ {label} — {len(files)} files symlinked")
                skip.mark_success(skip_key)
                dash.inc_processed()
                dash.inc_success(label)
                return "multi_resolved" if is_multi else True

        harvester.add(HarvestItem(sid, sn, stitle, spath, tid, h, rt, search_names,
                                  is_pack, sonarr, cfg, skip))
        with dash.lock:
            dash._harvester_pending = harvester.pending_count
        dash.set_thread(tn, status="success", detail="Queued for harvest")
        dash.inc_processed()
        return "multi_resolved" if is_multi else True

    dash.set_thread(tn, status="failed", detail="All failed")
    dash.add_log("WARNING", f"[{tn}] ✗ {label}")
    skip.mark_failed(skip_key, label, "all_failed")
    dash.inc_processed()
    dash.inc_failed()
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sonarr Auto Downloader (TorBox)")
    add_common_args(parser)
    parser.add_argument("--packs-only", action="store_true", help="Skip individual episodes")
    parser.add_argument("--series", type=str, default=None, help="Filter by series name")
    parser.add_argument("--min-score", type=int, default=None, help="Override min score")
    args = parser.parse_args()
    con = Console()

    if not os.path.exists(args.config):
        con.print(f"[yellow]Config not found: {args.config}[/yellow]")
        with open(args.config, "w") as f:
            json.dump(SAMPLE_CONFIG, f, indent=2)
        con.print(f"[green]Sample written to {args.config}[/green]")
        sys.exit(1)

    cfg = SonarrConfig.from_json(args.config)
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
    if args.packs_only:
        cfg.packs_only = True
    if args.min_score is not None:
        cfg.min_score = args.min_score

    notifier = DiscordNotifier(cfg.discord)
    config_dir = os.path.dirname(os.path.abspath(args.config))
    config_name = os.path.splitext(os.path.basename(args.config))[0]
    skip = SkipCache(os.path.join(config_dir, f"skip_{config_name}.json"), cooldown_days=args.cooldown)
    deferred_path = os.path.join(config_dir, f"deferred_{config_name}.json")

    if handle_cache_commands(args, skip, deferred_path, con):
        return

    con.print("[bold cyan]Fetching missing seasons...[/bold cyan]")
    sonarr = SonarrClient(cfg.sonarr_url, cfg.sonarr_api_key)
    tb = TorBoxClient(cfg.torbox_api_key, max_slots=cfg.torbox_max_slots)

    con.print("[dim]Cleaning TorBox queue...[/dim]")
    dead = tb.cleanup()
    if dead:
        con.print(f"[dim]Cleaned {dead} dead torrents[/dim]")

    all_items = sonarr.get_missing_seasons()

    if args.series:
        all_items = [i for i in all_items if args.series.lower() in i["series_title"].lower()]
        con.print(f"[dim]Filtered to '{args.series}': {len(all_items)} seasons[/dim]")

    if args.retry_deferred:
        dq = DeferredQueue(persist_path=deferred_path)
        ids = set(dq.get_saved_ids())
        if not ids:
            con.print("[dim]No deferred items[/dim]")
            return
        all_items = [i for i in all_items if i["series_id"] in ids]
        con.print(f"[bold yellow]Retrying {len(all_items)} deferred[/bold yellow]")
        dq.clear_saved()
    else:
        if not args.force:
            before = len(all_items)
            all_items = [i for i in all_items
                         if not skip.should_skip(f"{i['series_id']}_S{i['season_number']:02d}")[0]]
            sk = before - len(all_items)
            if sk:
                con.print(f"[dim]Skipping {sk} seasons (cooldown)[/dim]")
        else:
            con.print("[yellow]Force mode[/yellow]")

    if not all_items:
        con.print("[green]Nothing to do![/green]")
        return

    all_items.sort(key=lambda x: (-x.get("imdb_rating", 0), x["series_title"], x["season_number"]))

    # Group by series for 3-pass
    series_map = {}
    for item in all_items:
        sid = item["series_id"]
        if sid not in series_map:
            series_map[sid] = []
        series_map[sid].append(item)

    # Pass 1 candidates: one per series with 2+ missing seasons
    pass1 = []
    for sid, sitems in series_map.items():
        if len(sitems) >= 2:
            s01 = next((i for i in sitems if i["season_number"] == 1), sitems[0])
            pass1.append(s01)
    pass1.sort(key=lambda x: (-x.get("imdb_rating", 0), x["series_title"]))

    resolved_series = set()
    total = min(len(all_items), cfg.max_items) if cfg.max_items > 0 else len(all_items)

    # Enforce --max by slicing the input list upfront
    if cfg.max_items > 0:
        all_items = all_items[:cfg.max_items]
        allowed_sids = {i["series_id"] for i in all_items}
        pass1 = [p for p in pass1 if p["series_id"] in allowed_sids]

    con.print(f"[bold]{len(all_items)}[/bold] seasons across [bold]{len(series_map)}[/bold] series")
    con.print(f"[dim]Pass 1: {len(pass1)} series for multi-season packs[/dim]")

    if not check_mount_health(cfg.decypharr_mount, notifier):
        con.print(f"[bold red]Mount down![/bold red]")
        sys.exit(1)

    dash = DashboardState(total, cfg.threads, title=f"Sonarr Auto ({config_name})")
    dash.set_logfile(os.path.join(config_dir, f"{config_name}.log"))
    dash.set_notifier(notifier)
    dash.add_log("INFO", f"Started — {len(all_items)} seasons, {len(series_map)} series, {cfg.threads} threads")
    tb = TorBoxClient(cfg.torbox_api_key, max_slots=cfg.torbox_max_slots, dash=dash, notifier=notifier)

    notifier.send("started", f"Sonarr ({config_name}) started",
                  fields={"Seasons": str(len(all_items)), "Series": str(len(series_map)),
                          "Threads": str(cfg.threads)})
    dash.start_status_ticker(cfg.discord_interval_minutes)

    harvester = Harvester(dash, notifier)
    threading.Thread(target=harvester.run, name="harvester", daemon=True).start()
    deferred = DeferredQueue(persist_path=deferred_path)
    processed_count = [0]

    def should_continue():
        return cfg.max_items <= 0 or processed_count[0] < cfg.max_items

    def process_deferred():
        items = deferred.drain()
        if not items:
            return
        with dash.lock:
            dash._deferred_count = 0
        dash.add_log("INFO", f"Retrying {len(items)} deferred...")
        with ThreadPoolExecutor(max_workers=cfg.threads, thread_name_prefix="worker") as pool:
            futs = {pool.submit(process_season, di.item, sonarr, tb, cfg, dash, skip,
                                harvester, deferred, notifier): di for di in items}
            for f in as_completed(futs):
                try:
                    f.result()
                except:
                    dash.inc_processed()
                    dash.inc_failed()
                processed_count[0] += 1

    def run():
        try:
            # Pass 1: Multi-season packs
            dash.add_log("INFO", "═══ Pass 1: Multi-season packs ═══")
            with ThreadPoolExecutor(max_workers=cfg.threads, thread_name_prefix="worker") as pool:
                futs = {}
                for item in pass1:
                    if not should_continue():
                        break
                    futs[pool.submit(process_season, item, sonarr, tb, cfg, dash, skip,
                                    harvester, deferred, notifier, tier_filter=2)] = item
                for f in as_completed(futs):
                    try:
                        result = f.result()
                        it = futs[f]
                        if result == "multi_resolved":
                            resolved_series.add(it["series_id"])
                            processed_count[0] += len(series_map[it["series_id"]])
                        else:
                            processed_count[0] += 1
                    except:
                        dash.inc_processed()
                        dash.inc_failed()
                        processed_count[0] += 1

            # Pass 2: Season packs
            dash.add_log("INFO", "═══ Pass 2: Season packs ═══")
            remaining = [i for i in all_items if i["series_id"] not in resolved_series
                         and not skip.should_skip(f"{i['series_id']}_S{i['season_number']:02d}")[0]]
            remaining.sort(key=lambda x: (-x.get("imdb_rating", 0), x["series_title"], x["season_number"]))
            with ThreadPoolExecutor(max_workers=cfg.threads, thread_name_prefix="worker") as pool:
                futs = {}
                for item in remaining:
                    if not should_continue():
                        break
                    futs[pool.submit(process_season, item, sonarr, tb, cfg, dash, skip,
                                    harvester, deferred, notifier, tier_filter=1)] = item
                for f in as_completed(futs):
                    try:
                        f.result()
                    except:
                        dash.inc_processed()
                        dash.inc_failed()
                    processed_count[0] += 1

            # Pass 3: Singles
            if cfg.packs_only:
                dash.add_log("INFO", "═══ Pass 3: Skipped (packs-only) ═══")
            else:
                dash.add_log("INFO", "═══ Pass 3: Single episodes ═══")
                remaining = [i for i in all_items if i["series_id"] not in resolved_series
                             and not skip.should_skip(f"{i['series_id']}_S{i['season_number']:02d}")[0]]
                remaining.sort(key=lambda x: (-x.get("imdb_rating", 0), x["series_title"], x["season_number"]))
                with ThreadPoolExecutor(max_workers=cfg.threads, thread_name_prefix="worker") as pool:
                    futs = {}
                    for item in remaining:
                        if not should_continue():
                            break
                        futs[pool.submit(process_season, item, sonarr, tb, cfg, dash, skip,
                                        harvester, deferred, notifier, tier_filter=0)] = item
                    for f in as_completed(futs):
                        try:
                            f.result()
                        except:
                            dash.inc_processed()
                            dash.inc_failed()
                        processed_count[0] += 1

            # Deferred retry loop
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
            notifier.send("error", "Sonarr script crashed!",
                          f"```\n{tb_str[-1500:]}\n```\n"
                          f"Restart: `python3 sonarr_auto.py --config {args.config} --retry-deferred --no-ui`")
            raise

    run_with_ui(run, dash, args, con)
    dash.stop_status_ticker()
    sn = print_summary(dash, harvester, con)
    send_completed(notifier, sn, harvester, deferred)


if __name__ == "__main__":
    main()