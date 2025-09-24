"""
Generate CSV files:
1) 1_my_songs.csv — ONLY Liked tracks + tracks from playlists YOU OWN
2) 3_recent_plays.csv — last ~50 played tracks
3) 4_albums_listened.csv — albums that contain your Liked / your-own-playlists tracks
4) 5_playlists_by_me/<Playlist>.csv — one per playlist you own
5) New/<MadeForYou>.csv — "Made For You" playlists (Discover Weekly, Release Radar, Daily Mixes, On Repeat, Repeat Rewind, Your Top Songs YYYY) if present in your library

Scopes used:
- user-library-read
- playlist-read-private
- playlist-read-collaborative
- user-read-recently-played

Robustness:
- Handles 429 rate limits (Retry-After) + network hiccups with backoff
- Skips podcast episodes, local/None items
- Sanitizes filenames
- Market filter retained (unused by default)
"""

import argparse
import csv
import os
import re
import time
from collections import OrderedDict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.panel import Panel

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException, SpotifyOauthError
import requests

console = Console()
SAFE_FN_RE = re.compile(r'[^\w\s\-\.\(\)&]')  # for filenames

# ====== YOUR APP CREDS ======
client_id = "" # Add here
client_secret = "" # And here btw.
client_uri = "https://127.0.0.1:8888/songs"


# ---------- small utils ----------
def ts() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")

def mkdirp(path: str):
    os.makedirs(path, exist_ok=True)

def sanitize(name: str) -> str:
    if not name:
        return "untitled"
    cleaned = SAFE_FN_RE.sub("_", name)
    return cleaned.strip().strip("._") or "untitled"

def write_csv(path: str, fieldnames: List[str], rows_iter: Iterable[dict]):
    mkdirp(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows_iter:
            w.writerow(r)

def write_csv_rows(path: str, header: List[str], rows_iter: Iterable[Iterable]):
    mkdirp(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows_iter:
            w.writerow(row)


# ---------- exporter ----------
class ListsMaker:
    """
    Builds the requested outputs with the corrected behavior.
    """

    def __init__(self, outdir: str, artist_market: Optional[str] = None, cache_path=".cache-make-lists"):
        load_dotenv()
        self.outdir = outdir
        self.artist_market = artist_market
        self.cache_path = cache_path

        scope = "user-library-read playlist-read-private playlist-read-collaborative user-read-recently-played"
        try:
            self.sp = spotipy.Spotify(
                auth_manager=SpotifyOAuth(
                    scope=scope,
                    client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=client_uri,
                    cache_path=self.cache_path,
                    open_browser=True,
                    show_dialog=False,
                )
            )
        except SpotifyOauthError as e:
            console.print(f"[bold red]OAuth error:[/bold red] {e}")
            self._env_hint()
            raise

        self.user = self._retry(self.sp.current_user)
        self.user_id = self.user.get("id") if self.user else None

        # data stores
        self.tracks: Dict[str, dict] = OrderedDict()   # track_id -> normalized track dict
        self.albums: Dict[str, dict] = OrderedDict()   # album_id -> normalized album dict
        self.playlists: Dict[str, dict] = OrderedDict()  # playlist_id -> dict
        self.playlist_tracks: List[Tuple[str, str, str, Optional[str]]] = []  # (pid, tid, added_at, added_by)
        self.saved_tracks: Dict[str, str] = OrderedDict()    # track_id -> added_at
        self.track_to_album: Dict[str, str] = {}   # track_id -> album_id

    # ---------- terminal UI ----------
    def interactive(self):
        console.print(Panel.fit(
            "[bold]Make Lists (Spotify)[/bold]\nThis will produce CSV outputs with your corrected rules.",
            title="Interactive"
        ))
        if not Confirm.ask("Proceed and gather data (Liked, your playlists, recent plays)?", default=True):
            console.print("[yellow]Cancelled.[/yellow]"); return

        market = Prompt.ask("Artist catalog market (e.g., IN/US). Leave empty to skip", default="")
        if market:
            self.artist_market = market

        self.run_all()

    # ---------- main pipeline ----------
    def run_all(self):
        console.print("\n[bold]Step 1/5: Fetch Liked (Saved) tracks[/bold]")
        self._harvest_liked()

        console.print("[bold]Step 2/5: Fetch ALL playlists in your library[/bold]")
        self._harvest_all_playlists()

        console.print("[bold]Step 3/5: Fetch your recent plays (~50)[/bold]")
        recent = self._fetch_recently_played()

        console.print("[bold]Step 4/5: Write output files[/bold]")
        outbase = os.path.join(self.outdir, f"lists_{ts()}")
        mkdirp(outbase)

        # 1) Songs = ONLY Liked + playlists YOU OWN
        self._write_my_songs(os.path.join(outbase, "1_my_songs.csv"))

        # 3) Recent plays
        self._write_recent(os.path.join(outbase, "3_recent_plays.csv"), recent)

        # 4) Albums that contain your listened tracks
        self._write_albums_listened(os.path.join(outbase, "4_albums_listened.csv"))

        # 5) One CSV per playlist you own
        self._write_playlists_by_me(os.path.join(outbase, "5_playlists_by_me"))

        # New) "Made For You" exports
        self._write_made_for_you(os.path.join(outbase, "New"))

        console.print(Panel.fit(f"[green]Done.[/green]\nOutput folder: [bold]{outbase}[/bold]"))

    # ---------- harvesting ----------
    def _harvest_liked(self):
        limit, offset, total = 50, 0, 0
        while True:
            page = self._retry(lambda: self.sp.current_user_saved_tracks(limit=limit, offset=offset))
            items = page.get("items", [])
            if not items: break
            for it in items:
                t = it.get("track")
                if not t or t.get("type") != "track" or not t.get("id"):  # skip episodes/local/None
                    continue
                self._add_track_full(t)
                self.saved_tracks[t["id"]] = it.get("added_at")
                total += 1
            if len(items) < limit: break
            offset += limit
        console.print(f"  • saved tracks: {total}")

    def _harvest_all_playlists(self):
        count_p = 0
        for p in self._iter_pages(lambda **kw: self.sp.current_user_playlists(**kw), key="items", limit=50):
            self._add_playlist_basic(p)
            count = 0
            for it in self._iter_pages(lambda **kw: self.sp.playlist_items(playlist_id=p["id"], **kw), key="items", limit=100):
                t = it.get("track")
                if not t or t.get("type") != "track" or not t.get("id"):
                    continue
                self._add_track_full(t)
                self.playlist_tracks.append((
                    p["id"], t["id"], it.get("added_at"), (it.get("added_by") or {}).get("id")
                ))
                count += 1
            count_p += 1
            console.print(f"  • playlist '{p.get('name')}' → {count} tracks")
        console.print(f"  • total playlists processed: {count_p}")

    def _fetch_recently_played(self) -> List[dict]:
        try:
            rp = self._retry(lambda: self.sp.current_user_recently_played(limit=50))
            return rp.get("items", [])
        except SpotifyException as e:
            console.print(f"[yellow]  • Could not fetch recently played ({e}).[/yellow]")
            return []

    # ---------- writers ----------
    def _write_my_songs(self, path: str):
        """
        DISTINCT union of:
          - Saved (Liked) tracks
          - Tracks from playlists YOU OWN (owner_id == current user)
        """
        # figure out which playlist IDs are owned by me
        owned_pids = {pid for pid, meta in ((p["playlist_id"], p) for p in self.playlists.values())
                      if meta.get("owner_id") == self.user_id}

        song_ids = set(self.saved_tracks.keys())
        song_ids.update(tid for (pid, tid, _, _) in self.playlist_tracks if pid in owned_pids)

        rows = []
        for tid in sorted(song_ids):
            t = self.tracks.get(tid)
            if not t: 
                continue
            rows.append(self._row_track(t))

        write_csv(
            path,
            list(rows[0].keys()) if rows else
            ["track_id","track_name","artist_names","album_name","album_release_date","isrc","duration_ms","explicit","spotify_url"],
            rows
        )
        console.print(f"  • wrote {path} ({len(rows)} rows)")

    def _write_recent(self, path: str, recent_items: List[dict]):
        rows = []
        for it in recent_items:
            t = it.get("track")
            if not t or t.get("type") != "track" or not t.get("id"):
                continue
            rows.append({
                "played_at": it.get("played_at"),
                **self._row_track(self._ensure_track_full(t.get("id"), t))
            })
        write_csv(path,
                  ["played_at","track_id","track_name","artist_names","album_name","album_release_date","isrc","duration_ms","explicit","spotify_url"],
                  rows)
        console.print(f"  • wrote {path} ({len(rows)} rows)")

    def _write_albums_listened(self, path: str):
        """Albums that contain any song from Liked + your OWN playlists."""
        # owned playlist IDs
        owned_pids = {pid for pid, meta in ((p["playlist_id"], p) for p in self.playlists.values())
                      if meta.get("owner_id") == self.user_id}

        listened_tids = set(self.saved_tracks.keys())
        listened_tids.update(tid for (pid, tid, _, _) in self.playlist_tracks if pid in owned_pids)

        album_ids = set(self.track_to_album.get(tid) for tid in listened_tids if self.track_to_album.get(tid))
        rows = []
        for aid in sorted(album_ids):
            a = self.albums.get(aid)
            if not a: continue
            rows.append({
                "album_id": aid,
                "album_name": a.get("name"),
                "album_type": a.get("album_type"),
                "total_tracks": a.get("total_tracks"),
                "release_date": a.get("release_date"),
                "release_date_precision": a.get("release_date_precision"),
            })
        write_csv(path,
                  ["album_id","album_name","album_type","total_tracks","release_date","release_date_precision"],
                  rows)
        console.print(f"  • wrote {path} ({len(rows)} rows)")

    def _write_playlists_by_me(self, dirpath: str):
        """One CSV per playlist owned by me."""
        mkdirp(dirpath)
        mine = [p for p in self.playlists.values() if (p.get("owner_id") == self.user_id)]
        index_rows = []
        for p in mine:
            pid = p["playlist_id"]
            pname = p.get("name") or pid
            safe = sanitize(pname)
            tracks_rows = []
            for (ppid, tid, added_at, added_by) in self.playlist_tracks:
                if ppid != pid: continue
                t = self.tracks.get(tid)
                if not t: continue
                rec = self._row_track(t)
                rec = {"added_at": added_at, **rec}
                tracks_rows.append(rec)
            file_path = os.path.join(dirpath, f"{safe}.csv")
            write_csv(file_path,
                      ["added_at","track_id","track_name","artist_names","album_name","album_release_date","isrc","duration_ms","explicit","spotify_url"],
                      tracks_rows)
            index_rows.append([pname, file_path])
            console.print(f"  • wrote playlist file: {file_path}")

        # small index
        write_csv_rows(os.path.join(dirpath, "_index.csv"),
                       ["playlist_name","file_path"], index_rows)

    def _write_made_for_you(self, dirpath: str):
        """
        Export "Made For You" playlists present in your library.
        Heuristic: owner is 'spotify' and name matches known patterns.
        """
        mkdirp(dirpath)
        patterns = [
            r"^Discover Weekly$",
            r"^Release Radar$",
            r"^Daily Mix \d+$",
            r"^On Repeat$",
            r"^Repeat Rewind$",
            r"^Your Top Songs \d{4}$",
        ]
        regexes = [re.compile(p, re.IGNORECASE) for p in patterns]

        mfys = []
        for p in self.playlists.values():
            owner_id = (p.get("owner_id") or "").lower()
            name = p.get("name") or ""
            if owner_id != "spotify":
                continue
            if any(rx.search(name) for rx in regexes):
                mfys.append(p)

        if not mfys:
            console.print("  • 'Made For You': none found in your library (skipping).")
            return

        for p in mfys:
            pid = p["playlist_id"]
            pname = p.get("name") or pid
            safe = sanitize(pname)
            rows = []
            for (ppid, tid, added_at, added_by) in self.playlist_tracks:
                if ppid != pid: 
                    continue
                t = self.tracks.get(tid)
                if not t: 
                    continue
                rec = self._row_track(t)
                rec = {"added_at": added_at, **rec}
                rows.append(rec)
            out = os.path.join(dirpath, f"{safe}.csv")
            write_csv(
                out,
                ["added_at","track_id","track_name","artist_names","album_name","album_release_date","isrc","duration_ms","explicit","spotify_url"],
                rows
            )
            console.print(f"  • 'Made For You' → wrote {out}")

    # ---------- normalization ----------
    def _row_track(self, t: dict) -> dict:
        artists = ", ".join(a.get("name","") for a in (t.get("artists") or []))
        album = t.get("album") or {}
        return {
            "track_id": t.get("id"),
            "track_name": t.get("name"),
            "artist_names": artists,
            "album_name": album.get("name"),
            "album_release_date": album.get("release_date"),
            "isrc": (t.get("external_ids") or {}).get("isrc"),
            "duration_ms": t.get("duration_ms"),
            "explicit": bool(t.get("explicit")),
            "spotify_url": (t.get("external_urls") or {}).get("spotify"),
        }

    def _ensure_track_full(self, tid: str, fallback: Optional[dict] = None) -> dict:
        if tid in self.tracks:
            return self.tracks[tid]
        try:
            full = self._retry(lambda: self.sp.track(tid))
            self._add_track_full(full)
            return full
        except Exception:
            return fallback or {}

    def _add_track_full(self, t: dict):
        tid = t.get("id")
        if not tid: return
        if tid not in self.tracks:
            album = t.get("album") or {}
            if album:
                self._add_album_basic(album)
            self.tracks[tid] = t
            if album and album.get("id"):
                self.track_to_album[tid] = album["id"]
        # ensure album normalized
        if t.get("album"):
            self._add_album_basic(t["album"])

    def _add_album_basic(self, album: dict):
        aid = album.get("id")
        if not aid: return
        if aid not in self.albums:
            self.albums[aid] = {
                "album_id": aid,
                "name": album.get("name"),
                "album_type": album.get("album_type"),
                "total_tracks": album.get("total_tracks"),
                "release_date": album.get("release_date"),
                "release_date_precision": album.get("release_date_precision"),
            }

    def _add_playlist_basic(self, p: dict):
        pid = p.get("id")
        if not pid: return
        if pid not in self.playlists:
            self.playlists[pid] = {
                "playlist_id": pid,
                "name": p.get("name"),
                "owner_id": (p.get("owner") or {}).get("id"),
                "public": p.get("public"),
                "collaborative": p.get("collaborative"),
                "snapshot_id": p.get("snapshot_id"),
            }

    # ---------- network helpers ----------
    def _retry(self, fn, *args, **kwargs):
        backoff = 1.0
        while True:
            try:
                return fn(*args, **kwargs)
            except SpotifyException as e:
                if e.http_status == 429:
                    wait = float(e.headers.get("Retry-After", "2"))
                    time.sleep(wait + 0.25)
                    continue
                # transient auth/network/server issues: backoff + retry
                if e.http_status >= 500 or e.http_status in (401, 403, 408):
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 16)
                    continue
                raise
            except (requests.exceptions.RequestException, requests.exceptions.ConnectionError):
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue

    def _iter_pages(self, call, key: str, **kwargs):
        kwargs.setdefault("offset", 0)
        kwargs.setdefault("limit", 50)
        while True:
            page = self._retry(lambda: call(**kwargs))
            items = page.get(key, [])
            for it in items:
                yield it
            if not page.get("next"): break
            kwargs["offset"] += kwargs["limit"]

    def _env_hint(self):
        console.print(
            "\n[bold yellow]Check your .env:[/bold yellow]\n"
            "  SPOTIPY_CLIENT_ID=...\n"
            "  SPOTIPY_CLIENT_SECRET=...\n"
            "  SPOTIPY_REDIRECT_URI=http://localhost:8080/callback\n"
            "Make sure the same URI is saved on your Spotify app. If issues persist, delete .cache* and retry."
        )

# ---------- CLI ----------
def build_argparser():
    p = argparse.ArgumentParser(description="Build Spotify CSV lists (corrected behavior).")
    p.add_argument("--outdir", default="spotify_lists_out", help="Output base directory")
    p.add_argument("--artist-market", default=None, help="(Unused here) Market/country code for artist catalogs")
    p.add_argument("--interactive", action="store_true", help="Guided prompts")
    p.add_argument("--fresh-auth", action="store_true", help="Delete .cache* before starting (fix stale tokens)")
    return p

def main():
    args = build_argparser().parse_args()

    if args.fresh_auth:
        for f in os.listdir("."):
            if f.startswith(".cache"):
                try: os.remove(f)
                except Exception: pass

    maker = ListsMaker(outdir=args.outdir, artist_market=args.artist_market)

    if args.interactive:
        maker.interactive()
    else:
        maker.run_all()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[red]Aborted by user.[/red]")
