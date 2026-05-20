"""
arr_core.py — Shared core for radarr_auto.py and sonarr_auto.py

TorBox client (rate limiting, deferred queue), Discord webhooks,
skip cache, dashboard, harvester, file helpers.
"""

import json, os, re, sys, time, threading, requests
from dataclasses import dataclass
from datetime import datetime, timedelta
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box

VIDEO_EXT = (".mkv", ".mp4", ".avi", ".m4v")

# ── Discord Webhook ───────────────────────────────────────────────────────────

DISCORD_CATEGORIES = {
    "started": "🚀 Run started", "completed": "🏁 Run completed",
    "rate_limit": "⏸️ Rate limited (429)", "rate_lifted": "▶️ Rate limit lifted",
    "error": "❌ Error / crash", "progress": "📊 Progress milestone",
    "health": "🏥 Health check", "mount_down": "💀 Mount down",
}


class DiscordNotifier:
    COLORS = {"started": 0x3498db, "completed": 0x2ecc71, "rate_limit": 0xe67e22,
              "rate_lifted": 0x1abc9c, "error": 0xe74c3c, "progress": 0x9b59b6,
              "health": 0xf1c40f, "mount_down": 0xe74c3c}

    def __init__(self, cfg):
        self.urls = {}
        if not cfg:
            return
        if isinstance(cfg, str):
            if cfg:
                for c in DISCORD_CATEGORIES:
                    self.urls[c] = cfg
        elif isinstance(cfg, dict):
            fb = cfg.get("all", "")
            for c in DISCORD_CATEGORIES:
                url = cfg.get(c, "") or fb
                if url:
                    self.urls[c] = url

    @property
    def enabled(self):
        return bool(self.urls)

    def send(self, category, title, description="", fields=None):
        url = self.urls.get(category)
        if not url:
            return
        threading.Thread(target=self._send, args=(url, category, title, description, fields),
                         daemon=True).start()

    def _send(self, url, cat, title, desc, fields):
        try:
            embed = {"title": f"{DISCORD_CATEGORIES.get(cat, '📌')} {title}",
                     "color": self.COLORS.get(cat, 0x95a5a6),
                     "timestamp": datetime.utcnow().isoformat()}
            if desc:
                embed["description"] = desc[:4096]
            if fields:
                embed["fields"] = [{"name": k, "value": str(v)[:1024], "inline": True}
                                   for k, v in fields.items()][:25]
            r = requests.post(url, json={"embeds": [embed]}, timeout=10)
            if r.status_code == 429:
                time.sleep(r.json().get("retry_after", 5))
                requests.post(url, json={"embeds": [embed]}, timeout=10)
        except:
            pass


# ── Skip Cache ────────────────────────────────────────────────────────────────

class SkipCache:
    def __init__(self, path, cooldown_days=7):
        self.path = path
        self.cooldown = cooldown_days * 86400
        self.lock = threading.Lock()
        self.data = {}
        if os.path.exists(path):
            try:
                with open(path) as f:
                    self.data = json.load(f)
            except:
                pass

    def _save(self):
        try:
            with open(self.path, "w") as f:
                json.dump(self.data, f, indent=2)
        except:
            pass

    def should_skip(self, key):
        key = str(key)
        with self.lock:
            if key not in self.data:
                return False, ""
            age = time.time() - self.data[key].get("timestamp", 0)
            if age > self.cooldown:
                del self.data[key]
                self._save()
                return False, ""
            return True, f"{(self.cooldown - age) / 86400:.1f}d ({self.data[key].get('reason', '?')})"

    def mark_failed(self, key, title, reason):
        key = str(key)
        with self.lock:
            self.data[key] = {"timestamp": time.time(), "reason": reason, "title": title,
                              "date": datetime.now().strftime("%Y-%m-%d %H:%M")}
            self._save()

    def mark_success(self, key):
        key = str(key)
        with self.lock:
            self.data.pop(key, None)
            self._save()

    def clear_all(self):
        with self.lock:
            self.data = {}
            self._save()

    def stats(self):
        return len(self.data), 0


# ── Dashboard ─────────────────────────────────────────────────────────────────

STATUS_STYLES = {
    "searching": ("yellow", "🔍 Search"), "checking_cache": ("cyan", "💾 Cache"),
    "checking_mount": ("cyan", "📂 Mount"), "downloading": ("green", "⬇ DL"),
    "waiting_seeds": ("yellow", "🌱 Seeds"), "waiting_init": ("dim", "⏳ Init"),
    "success": ("bold green", "✅ Done"), "failed": ("red", "❌ Fail"),
    "no_releases": ("dim red", "🚫 None"), "rate_limited": ("bold red", "⏸ 429"),
}


class DashboardState:
    def __init__(self, total, num_threads, title="Arr Auto"):
        self.total = total
        self.lock = threading.Lock()
        self.title = title
        self.threads = {f"worker_{i}": {"movie": "", "status": "idle", "detail": "",
                        "release": "", "progress": 0.0} for i in range(num_threads)}
        self._processed = 0
        self._success = 0
        self._failed = 0
        self._skipped = 0
        self._start = time.time()
        self._logs = []
        self._logfile = None
        self._rate_limited = False
        self._rate_limit_until = ""
        self._deferred_count = 0
        self._notifier = None
        self._last_milestone = 0
        self._harvester_pending = 0
        self._harvester_done = 0
        self._cached_hits = 0
        self._no_releases = 0
        self._last_success_title = ""
        self._discord_interval = 0
        self._status_stop = threading.Event()

    def set_logfile(self, p):
        self._logfile = p

    def set_notifier(self, n):
        self._notifier = n

    def set_thread(self, tn, **kw):
        with self.lock:
            if tn in self.threads:
                self.threads[tn].update(kw)

    def add_log(self, level, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"{ts} | {level:<8} | {msg}"
        with self.lock:
            self._logs.append(entry)
            if len(self._logs) > 200:
                self._logs = self._logs[-150:]
        if self._logfile:
            try:
                with open(self._logfile, "a") as f:
                    f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} | {level:<8} | {msg}\n")
            except:
                pass

    def inc_processed(self):
        with self.lock:
            self._processed += 1
        self._check_milestone()

    def inc_success(self, title=""):
        with self.lock:
            self._success += 1
            if title:
                self._last_success_title = title

    def inc_failed(self):
        with self.lock:
            self._failed += 1

    def inc_skipped(self):
        with self.lock:
            self._skipped += 1

    def inc_cached(self):
        with self.lock:
            self._cached_hits += 1

    def inc_no_releases(self):
        with self.lock:
            self._no_releases += 1

    def inc_movies_done(self, tn):
        with self.lock:
            if tn in self.threads:
                self.threads[tn]["movies_done"] = self.threads[tn].get("movies_done", 0) + 1

    def _check_milestone(self):
        if not self._notifier:
            return
        with self.lock:
            pct = int((self._processed / max(1, self.total)) * 100)
        ms = (pct // 25) * 25
        if ms > 0 and ms > self._last_milestone and ms < 100:
            self._last_milestone = ms
            sn = self.get_snapshot()
            self._notifier.send("progress", f"{ms}% complete",
                                fields={"Processed": f"{sn['processed']}/{sn['total']}",
                                        "Success": str(sn['success']), "Failed": str(sn['failed']),
                                        "Rate": f"{sn['rate']:.1f}/min"})

    def get_snapshot(self):
        with self.lock:
            e = max(1, time.time() - self._start)
            return {"total": self.total, "processed": self._processed, "success": self._success,
                    "failed": self._failed, "skipped": self._skipped, "elapsed": e,
                    "rate": self._processed / (e / 60), "rate_limited": self._rate_limited,
                    "rate_limit_until": self._rate_limit_until, "deferred": self._deferred_count,
                    "cached_hits": self._cached_hits, "no_releases": self._no_releases,
                    "last_success": self._last_success_title,
                    "harvester_pending": self._harvester_pending,
                    "harvester_done": self._harvester_done}

    def start_status_ticker(self, interval_minutes):
        if interval_minutes <= 0 or not self._notifier:
            return
        self._discord_interval = interval_minutes * 60

        def _ticker():
            while not self._status_stop.wait(self._discord_interval):
                self._send_status_update()

        t = threading.Thread(target=_ticker, name="status_ticker", daemon=True)
        t.start()

    def stop_status_ticker(self):
        self._status_stop.set()

    def _send_status_update(self):
        if not self._notifier:
            return
        sn = self.get_snapshot()
        e = int(sn["elapsed"])
        m, s = divmod(e, 60)
        pct = int((sn["processed"] / max(1, sn["total"])) * 100)

        # Workers summary
        workers = []
        with self.lock:
            for tn, t in self.threads.items():
                if t["movie"]:
                    workers.append(f"`{t['movie'][:30]}` → {t['status']}")
        worker_str = "\n".join(workers[:4]) if workers else "_idle_"

        self._notifier.send("progress", f"Status — {pct}% ({sn['processed']}/{sn['total']})",
                            description=f"**Last success:** {sn['last_success'][:50] or 'none yet'}\n"
                                        f"**Workers:**\n{worker_str}",
                            fields={
                                "✅ Success": str(sn["success"]),
                                "❌ Failed": str(sn["failed"]),
                                "🚫 No releases": str(sn["no_releases"]),
                                "💾 Cache hits": str(sn["cached_hits"]),
                                "⚡ Rate": f"{sn['rate']:.1f}/min",
                                "⏱ Elapsed": f"{m}m{s:02d}s",
                                "📋 Deferred": str(sn["deferred"]),
                                "🌾 Harvester": f"{sn['harvester_done']}✓ {sn['harvester_pending']}⏳",
                            })


def render_dashboard(dash):
    sn = dash.get_snapshot()
    layout = Layout()
    layout.split_column(Layout(name="header", size=4), Layout(name="body"), Layout(name="footer", size=3))
    e = int(sn["elapsed"])
    m, s = divmod(e, 60)
    rl = f"  🚫 PAUSED until {sn['rate_limit_until']}" if sn["rate_limited"] else ""
    df = f"  📋 {sn['deferred']} deferred" if sn["deferred"] > 0 else ""
    header = Panel(
        Text(f"📊 {sn['processed']}/{sn['total']}  │  ✅ {sn['success']}  ❌ {sn['failed']}  "
             f"🚫 {sn['skipped']}  │  ⚡ {sn['rate']:.1f}/min  │  ⏱ {m}m{s:02d}s{rl}{df}",
             justify="center"),
        title=f"[bold]{dash.title}[/bold]", border_style="blue")
    layout["header"].update(header)
    body = Layout()
    body.split_row(Layout(name="threads", ratio=1), Layout(name="logs", ratio=2))
    tt = Table(box=box.SIMPLE, expand=True, show_header=True, header_style="bold")
    tt.add_column("Thread", width=9)
    tt.add_column("Item", width=25)
    tt.add_column("Status", width=14)
    tt.add_column("Detail", width=25)
    with dash.lock:
        for tn, t in dash.threads.items():
            ss, sl = STATUS_STYLES.get(t["status"], ("dim", t["status"]))
            tt.add_row(tn, Text(t["movie"][:24], overflow="ellipsis"),
                       Text(sl, style=ss), Text(t["detail"][:24], overflow="ellipsis"))
    body["threads"].update(Panel(tt, title="Threads"))
    with dash.lock:
        ll = dash._logs[-30:]
    lt = Text()
    for l in reversed(ll):
        st = "green" if "SUCCESS" in l else "red" if "ERROR" in l else "yellow" if "WARNING" in l else "default"
        lt.append(l + "\n", style=st)
    body["logs"].update(Panel(lt, title="Log"))
    layout["body"].update(body)
    hvst = f"🌾 Harvester: {dash._harvester_pending} pending, {dash._harvester_done} done"
    layout["footer"].update(Panel(Text(hvst, justify="center"), border_style="dim"))
    return layout


# ── TorBox Client ─────────────────────────────────────────────────────────────

class RateLimitError(Exception):
    pass


class TorBoxClient:
    B = "https://api.torbox.app/v1/api"

    def __init__(self, key, max_slots=3, dash=None, notifier=None):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {key}"
        self.max_slots = max_slots
        self._lock = threading.Lock()
        self._last_error = ""
        self.dash = dash
        self.notifier = notifier
        self._rate_limited = threading.Event()
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_until = 0

    def _signal_rate_limit(self):
        with self._rate_limit_lock:
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=2, second=0, microsecond=0)
            self._rate_limit_until = next_hour.timestamp()
            self._rate_limited.set()
            wait_min = (self._rate_limit_until - time.time()) / 60
            if self.dash:
                self.dash._rate_limited = True
                self.dash._rate_limit_until = next_hour.strftime("%H:%M")
                self.dash.add_log("WARNING",
                                  f"⏸ Rate limited! Pausing creates until {next_hour.strftime('%H:%M')} ({wait_min:.0f}min)")
            if self.notifier:
                self.notifier.send("rate_limit", "TorBox 429 — Rate Limited",
                                   f"Pausing until **{next_hour.strftime('%H:%M')}** ({wait_min:.0f} min).")

    def _clear_rate_limit(self):
        with self._rate_limit_lock:
            was = self._rate_limited.is_set()
            self._rate_limited.clear()
            self._rate_limit_until = 0
            if self.dash:
                self.dash._rate_limited = False
                self.dash._rate_limit_until = ""
            if was and self.notifier:
                self.notifier.send("rate_lifted", "Rate limit lifted",
                                   "Resuming torrent creation.")

    def is_rate_limited(self):
        return self._rate_limited.is_set()

    def wait_for_rate_limit(self, probe_interval=300):
        while self._rate_limited.is_set():
            remaining = self._rate_limit_until - time.time()
            if remaining <= 0:
                self._clear_rate_limit()
                return
            time.sleep(min(probe_interval, remaining))

    def cached(self, hashes):
        if not hashes:
            return {}
        r = self.s.get(f"{self.B}/torrents/checkcached",
                       params={"hash": ",".join(hashes), "format": "list"})
        r.raise_for_status()
        ch = {i["hash"].lower() for i in (r.json().get("data") or [])}
        return {h: h.lower() in ch for h in hashes}

    def add(self, h):
        with self._lock:
            self._clear_queue()
            self._free_dead_slots()
        r = self.s.post(f"{self.B}/torrents/createtorrent",
                        data={"magnet": f"magnet:?xt=urn:btih:{h}"})
        d = r.json() if r.status_code in (200, 400, 409) else {}
        if r.status_code == 429:
            self._signal_rate_limit()
            self._last_error = "429 rate limited"
            raise RateLimitError("createtorrent rate limited")
        if r.status_code not in (200,):
            detail = d.get("detail", d.get("error", f"HTTP {r.status_code}"))
            if "already" in str(detail).lower() or "exists" in str(detail).lower():
                tid = self._find_by_hash(h)
                if tid:
                    return tid
            self._last_error = str(detail)
            return None
        if not d.get("success"):
            self._last_error = d.get("detail", d.get("error", "unknown"))
            return None
        self._clear_rate_limit()
        tid = d.get("data", {}).get("torrent_id")
        if tid:
            time.sleep(2)
        with self._lock:
            self._clear_queue()
        self._last_error = ""
        return tid

    def _find_by_hash(self, h):
        r = self.s.get(f"{self.B}/torrents/mylist")
        if r.status_code != 200:
            return None
        for t in (r.json().get("data") or []):
            if t.get("hash", "").lower() == h.lower():
                return t.get("id")
        return None

    def info(self, tid):
        r = self.s.get(f"{self.B}/torrents/mylist", params={"id": tid})
        if r.status_code != 200:
            return None
        data = r.json().get("data")
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            for t in data:
                if t.get("id") == tid:
                    return t
        return None

    def delete(self, tid):
        return self.s.post(f"{self.B}/torrents/controltorrent",
                           json={"torrent_id": tid, "operation": "delete"}).status_code == 200

    def get_queued(self):
        r = self.s.get(f"{self.B}/queued/getqueued", params={"type": "torrent"})
        return (r.json().get("data") or []) if r.status_code == 200 else []

    def delete_queued(self, qid):
        return self.s.post(f"{self.B}/queued/controlqueued",
                           json={"queued_id": qid, "operation": "delete"}).status_code == 200

    def _clear_queue(self):
        for q in self.get_queued():
            self.delete_queued(q["id"])

    def _free_dead_slots(self):
        r = self.s.get(f"{self.B}/torrents/mylist")
        if r.status_code != 200:
            return
        active = r.json().get("data") or []
        dling = [t for t in active if t.get("download_state") not in ("completed", "cached")]
        if len(dling) < self.max_slots:
            return
        for t in dling:
            if t.get("seeds", 0) == 0 and t.get("progress", 0) < 0.05:
                self.delete(t["id"])

    def cleanup(self):
        self._clear_queue()
        r = self.s.get(f"{self.B}/torrents/mylist")
        if r.status_code != 200:
            return 0
        dead = [t for t in (r.json().get("data") or [])
                if t.get("download_state") not in ("completed", "cached")
                and t.get("seeds", 0) == 0 and t.get("progress", 0) < 0.05]
        for t in dead:
            self.delete(t["id"])
        return len(dead)


# ── Deferred Queue ────────────────────────────────────────────────────────────

class DeferredItem:
    def __init__(self, item, release, is_cached):
        self.item = item
        self.release = release
        self.is_cached = is_cached


class DeferredQueue:
    def __init__(self, persist_path=None):
        self.items = []
        self.lock = threading.Lock()
        self.persist_path = persist_path
        self._saved = []
        if persist_path and os.path.exists(persist_path):
            try:
                with open(persist_path) as f:
                    self._saved = json.load(f)
            except:
                pass

    def add(self, item):
        with self.lock:
            self.items.append(item)
            self._persist()

    def drain(self):
        with self.lock:
            items = list(self.items)
            self.items.clear()
            self._persist()
            return items

    @property
    def count(self):
        with self.lock:
            return len(self.items)

    def _persist(self):
        if not self.persist_path:
            return
        try:
            entries = []
            seen = set()
            for di in self.items:
                item = di.item
                # Support both movie dicts and season item dicts
                mid = item.get("id", item.get("series_id", ""))
                title = item.get("title", item.get("series_title", "?"))
                if mid not in seen:
                    seen.add(mid)
                    entries.append({"id": mid, "title": title,
                                    "release": di.release.get("title", "?")[:80],
                                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")})
            for saved in self._saved:
                if saved.get("id") not in seen:
                    entries.append(saved)
                    seen.add(saved.get("id"))
            with open(self.persist_path, "w") as f:
                json.dump(entries, f, indent=2)
        except:
            pass

    def get_saved_ids(self):
        if not self.persist_path or not os.path.exists(self.persist_path):
            return []
        try:
            with open(self.persist_path) as f:
                return [e["id"] for e in json.load(f)]
        except:
            return []

    def clear_saved(self):
        if self.persist_path and os.path.exists(self.persist_path):
            os.remove(self.persist_path)
        self._saved = []


# ── File Helpers ──────────────────────────────────────────────────────────────

def find_files(mount, h, name, extra_names=None):
    """Find ALL video files for a hash/name on the DFS mount."""
    names = [h, name] + (extra_names or [])
    seen = set()
    unique = [n for n in names if n and n not in seen and not seen.add(n)]
    SUBDIRS = ("TorBox", "__all__", "torrents")

    def _collect(p):
        results = []
        if os.path.isfile(p) and p.endswith(VIDEO_EXT):
            return [p]
        if os.path.isdir(p):
            for root, _, files in os.walk(p):
                for f in sorted(files):
                    if f.endswith(VIDEO_EXT):
                        results.append(os.path.join(root, f))
        return results

    for sn in unique:
        for sd in SUBDIRS:
            p = os.path.join(mount, sd, sn)
            if os.path.exists(p):
                vids = _collect(p)
                if vids:
                    return vids

    for sn in unique:
        if len(sn) < 15:
            continue
        for sd in SUBDIRS:
            sp = os.path.join(mount, sd)
            if not os.path.isdir(sp):
                continue
            try:
                for entry in os.listdir(sp):
                    if entry.startswith(sn[:40]):
                        vids = _collect(os.path.join(sp, entry))
                        if vids:
                            return vids
            except OSError:
                continue
    return []


def find_single_file(mount, h, name, extra_names=None):
    """Find first video file."""
    files = find_files(mount, h, name, extra_names)
    return files[0] if files else None


def mk_symlinks(src_files, dest_dir, path_map):
    """Symlink files into library folder. Applies path_map to convert container paths to host paths."""
    if not dest_dir or not src_files:
        return 0
    host_path = dest_dir
    for cp, hp in (path_map or {}).items():
        if dest_dir.startswith(cp):
            host_path = dest_dir.replace(cp, hp, 1)
            break
    os.makedirs(host_path, exist_ok=True)
    count = 0
    for src in src_files:
        dst = os.path.join(host_path, os.path.basename(src))
        if os.path.exists(dst):
            count += 1
            continue
        try:
            os.symlink(src, dst)
            count += 1
        except OSError:
            pass
    return count


def check_mount_health(mount, notifier=None):
    try:
        os.listdir(mount)
        return True
    except OSError:
        if notifier:
            notifier.send("mount_down", "Decypharr mount is down!",
                          f"Mount `{mount}` not accessible.\n"
                          f"Run: `sudo umount -l {mount} && docker restart decypharr`")
        return False


def build_search_names(info, h, title):
    """Build list of possible folder/file names from TB torrent info."""
    tb_name = info.get("name", "") if info else ""
    dl_path = info.get("download_path", "") if info else ""
    extra = [tb_name, title, dl_path]
    if info:
        for f in (info.get("files") or []):
            fname = f.get("name", "")
            s3 = f.get("s3_path", "")
            if "/" in fname:
                extra.append(fname.split("/")[0])
            if "/" in s3:
                parts = s3.split("/")
                if len(parts) >= 2:
                    extra.append(parts[1] if parts[0] == h else parts[0])
            extra.append(f.get("short_name", ""))
    return list(dict.fromkeys(n for n in extra if n))


def wait_for_download(tb, tid, h, title, cfg_poll, cfg_init_wait, dash, tn):
    """Wait for uncached torrent to finish downloading. Returns info dict or None."""
    dash.set_thread(tn, status="waiting_seeds", detail="Checking seeds...")
    time.sleep(5)
    info = tb.info(tid)
    if info:
        seeds = info.get("seeds", 0)
        state = info.get("download_state", "")
        if state in ("completed", "cached"):
            return info
        if seeds == 0:
            dash.add_log("WARNING", f"[{tn}] No seeds: {title[:40]}, deleting")
            tb.delete(tid)
            return None
        dash.set_thread(tn, status="downloading", detail=f"Seeds:{seeds}")
    else:
        dash.set_thread(tn, status="waiting_init", detail=f"Wait {cfg_init_wait}s...")
        time.sleep(cfg_init_wait)

    for _ in range(120):
        info = tb.info(tid)
        if not info:
            tb.delete(tid)
            return None
        p = info.get("progress", 0)
        state = info.get("download_state", "")
        dash.set_thread(tn, progress=p, detail=f"{p * 100:.1f}% seeds:{info.get('seeds', 0)}")
        if state in ("completed", "cached") or p >= 1.0:
            return info
        if state in ("error", "dead"):
            dash.add_log("WARNING", f"[{tn}] TB error: {state}")
            tb.delete(tid)
            return None
        if info.get("seeds", 0) == 0 and p < 0.05:
            dash.add_log("WARNING", f"[{tn}] Seeds lost, deleting")
            tb.delete(tid)
            return None
        time.sleep(cfg_poll)
    dash.add_log("WARNING", f"[{tn}] Download timeout")
    tb.delete(tid)
    return None


# ── Common CLI Helpers ────────────────────────────────────────────────────────

def add_common_args(parser):
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max", type=int, default=0, help="Max items to process (0=all)")
    parser.add_argument("--threads", type=int, default=0, help="Override thread count")
    parser.add_argument("--no-ui", action="store_true", help="Disable Rich TUI (for nohup/cron)")
    parser.add_argument("--force", action="store_true", help="Ignore skip cache")
    parser.add_argument("--clear-skip", action="store_true")
    parser.add_argument("--show-skip", action="store_true")
    parser.add_argument("--cooldown", type=int, default=7, help="Days to skip failed items")
    parser.add_argument("--retry-deferred", action="store_true", help="Retry 429'd items only")
    parser.add_argument("--show-deferred", action="store_true")
    parser.add_argument("--clear-deferred", action="store_true")


def handle_cache_commands(args, skip, deferred_path, con):
    """Handle --clear-skip, --show-skip, --show-deferred, --clear-deferred. Returns True if handled."""
    if args.clear_skip:
        t, _ = skip.stats()
        skip.clear_all()
        con.print(f"[bold green]Cleared {t} entries[/bold green]")
        return True
    if args.show_skip:
        if not skip.data:
            con.print("[dim]Skip cache is empty[/dim]")
            return True
        t = Table(title="Skipped Items", box=box.ROUNDED)
        t.add_column("Key", width=20)
        t.add_column("Title", width=40)
        t.add_column("Reason", width=20)
        t.add_column("Date", width=16)
        for key, entry in sorted(skip.data.items(), key=lambda x: x[1].get("timestamp", 0)):
            t.add_row(key, entry.get("title", "?")[:39], entry.get("reason", "?")[:19], entry.get("date", "?"))
        con.print(t)
        return True
    if args.show_deferred:
        if not os.path.exists(deferred_path):
            con.print("[dim]No deferred items[/dim]")
            return True
        with open(deferred_path) as f:
            entries = json.load(f)
        if not entries:
            con.print("[dim]No deferred items[/dim]")
            return True
        t = Table(title=f"Deferred ({len(entries)})", box=box.ROUNDED)
        t.add_column("ID", width=8)
        t.add_column("Title", width=35)
        t.add_column("Release", width=50)
        t.add_column("When", width=16)
        for e in entries:
            t.add_row(str(e.get("id", "")), e.get("title", "?")[:34],
                      e.get("release", "?")[:49], e.get("timestamp", "?"))
        con.print(t)
        return True
    if args.clear_deferred:
        if os.path.exists(deferred_path):
            os.remove(deferred_path)
        con.print("[bold green]Cleared deferred list[/bold green]")
        return True
    return False


def run_with_ui(run_func, dash, args, con):
    """Run the main loop with or without Rich TUI."""
    if args.no_ui:
        run_func()
    else:
        t = threading.Thread(target=run_func, daemon=True)
        t.start()
        with Live(render_dashboard(dash), console=con, refresh_per_second=2, screen=True) as live:
            while t.is_alive():
                live.update(render_dashboard(dash))
                time.sleep(0.5)
            live.update(render_dashboard(dash))
            time.sleep(1)


def print_summary(dash, harvester, con):
    sn = dash.get_snapshot()
    elapsed = int(sn["elapsed"])
    m, s = divmod(elapsed, 60)
    con.print(f"\n[bold green]Done![/bold green] ✅ {sn['success']} | ❌ {sn['failed']} | "
              f"🚫 {sn['skipped']} | {sn['processed']}/{sn['total']} | ⏱ {m}m{s:02d}s")
    if harvester.harvested or harvester.timed_out:
        con.print(f"[dim]Harvester: {harvester.harvested} found, {harvester.timed_out} timed out[/dim]")
    return sn


def send_completed(notifier, sn, harvester, deferred):
    elapsed = int(sn["elapsed"])
    m, s = divmod(elapsed, 60)
    notifier.send("completed", "Run completed",
                  fields={"✅ Success": str(sn["success"]), "❌ Failed": str(sn["failed"]),
                          "🚫 No releases": str(sn.get("no_releases", 0)),
                          "💾 Cache hits": str(sn.get("cached_hits", 0)),
                          "Total": f"{sn['processed']}/{sn['total']}",
                          "⏱ Duration": f"{m}m{s}s",
                          "🌾 Harvested": str(harvester.harvested),
                          "📋 Deferred": str(deferred.count)})
