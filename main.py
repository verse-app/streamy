import os
import time
import string
import hashlib
import yt_dlp
import tempfile
import requests
import random
import asyncio
import concurrent.futures
from functools import lru_cache
from fastapi import FastAPI, HTTPException, Request, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from ytmusicapi import YTMusic
from mutagen.id3 import ID3, APIC, TIT2, TPE1, TALB
from typing import Optional, Dict, List

app = FastAPI()
ytm = YTMusic()

# ===== SIMPLE TTL CACHE =====
class TTLCache:
    def __init__(self, ttl=300, max_size=200):
        self._store = {}
        self._ttl = ttl
        self._max_size = max_size

    def get(self, key):
        if key in self._store:
            val, ts = self._store[key]
            if time.time() - ts < self._ttl:
                return val
            del self._store[key]
        return None

    def set(self, key, val):
        if len(self._store) >= self._max_size:
            oldest = min(self._store, key=lambda k: self._store[k][1])
            del self._store[oldest]
        self._store[key] = (val, time.time())

search_cache = TTLCache(ttl=600, max_size=100)
artist_cache = TTLCache(ttl=1800, max_size=50)
album_cache = TTLCache(ttl=1800, max_size=50)

# ===== SPOTIFY TOKEN MANAGER =====
class SpotifyTokenManager:
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    SEARCH_URL = "https://api.spotify.com/v1/search"

    def __init__(self, client_id: str = "", client_secret: str = ""):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._expires = 0.0

    def _has_credentials(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def _refresh_token(self):
        if not self._has_credentials():
            return
        try:
            resp = requests.post(self.TOKEN_URL, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                self._token = data["access_token"]
                self._expires = time.time() + data.get("expires_in", 3600) - 60
        except Exception as e:
            print(f"Spotify token refresh failed: {e}")

    def get_token(self) -> Optional[str]:
        if not self._has_credentials():
            return None
        if not self._token or time.time() >= self._expires:
            self._refresh_token()
        return self._token

    def search_track_art(self, title: str, artist: str) -> str:
        token = self.get_token()
        if not token:
            return ""
        try:
            query = f"track:{title} artist:{artist}"
            resp = requests.get(self.SEARCH_URL, params={
                "q": query, "type": "track", "limit": 1
            }, headers={"Authorization": f"Bearer {token}"}, timeout=8)
            if resp.status_code == 200:
                tracks = resp.json().get("tracks", {}).get("items", [])
                if tracks:
                    images = tracks[0].get("album", {}).get("images", [])
                    if images:
                        return images[0].get("url", "")
        except Exception as e:
            print(f"Spotify search art failed: {e}")
        return ""

    async def get_track_artwork(self, title: str, artist: str) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.search_track_art, title, artist)

spotify = SpotifyTokenManager(
    client_id=os.environ.get("SPOTIFY_CLIENT_ID", ""),
    client_secret=os.environ.get("SPOTIFY_CLIENT_SECRET", ""),
)

# ===== ROOM MANAGER (WebSocket Listen Together) =====
SAFE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

class RoomManager:
    def __init__(self):
        self.rooms: Dict[str, dict] = {}

    def _gen_code(self) -> str:
        while True:
            code = "".join(random.choices(SAFE_CHARS, k=4))
            if code not in self.rooms:
                return code

    def create_room(self, host_ws: WebSocket) -> str:
        code = self._gen_code()
        self.rooms[code] = {
            "host": host_ws,
            "listeners": [],
            "queue": [],
            "current_track": None,
            "start_time": time.time(),
            "position": 0.0,
            "status": "stopped",
        }
        return code

    def join_room(self, code: str, ws: WebSocket) -> dict:
        room = self.rooms.get(code.upper())
        if not room:
            return None
        room["listeners"].append(ws)
        pos = room["position"]
        if room["status"] == "playing" and room["start_time"]:
            pos += time.time() - room["start_time"]
        return {
            "current_track": room["current_track"],
            "start_time": room["start_time"],
            "position": pos,
            "status": room["status"],
            "queue": room["queue"],
            "listener_count": len(room["listeners"]),
        }

    def leave_room(self, ws: WebSocket):
        for code, room in list(self.rooms.items()):
            if ws in room["listeners"]:
                room["listeners"].remove(ws)
            if room["host"] is ws:
                del self.rooms[code]
            elif not room["listeners"] and room["host"] is None:
                del self.rooms[code]

    def update_state(self, code: str, track=None, position=0.0, status="playing"):
        room = self.rooms.get(code)
        if not room:
            return
        if track is not None:
            room["current_track"] = track
        room["position"] = position
        room["status"] = status
        room["start_time"] = time.time()

    def get_current_position(self, code: str) -> float:
        room = self.rooms.get(code)
        if not room:
            return 0.0
        pos = room["position"]
        if room["status"] == "playing" and room["start_time"]:
            pos += time.time() - room["start_time"]
        return pos

room_mgr = RoomManager()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if os.path.exists(os.path.join(BASE_DIR, "static")):
    app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

YTM_MOOD_CATEGORIES = [
    {"id": "FEmusic_moods_and_genres", "name": "All Moods & Genres"},
    {"id": "FEmusic_mood_category_bright", "name": "Feel Good"},
    {"id": "FEmusic_mood_category_chill", "name": "Chill"},
    {"id": "FEmusic_mood_category_romance", "name": "Romance"},
    {"id": "FEmusic_mood_category_sad", "name": "Sad"},
    {"id": "FEmusic_mood_category_workout", "name": "Workout"},
    {"id": "FEmusic_mood_category_focus", "name": "Focus"},
    {"id": "FEmusic_mood_category_sleep", "name": "Sleep"},
    {"id": "FEmusic_mood_category_party", "name": "Party"},
    {"id": "FEmusic_mood_category_commute", "name": "Commute"},
    {"id": "FEmusic_mood_category_energy_boosters", "name": "Energy Boosters"},
]

YTM_GENRE_CATEGORIES = [
    {"id": "FEmusic_genre_pop", "name": "Pop"},
    {"id": "FEmusic_genre_hip_hop", "name": "Hip Hop"},
    {"id": "FEmusic_genre_rock", "name": "Rock"},
    {"id": "FEmusic_genre_rnb", "name": "R&B"},
    {"id": "FEmusic_genre_electronic", "name": "Electronic"},
    {"id": "FEmusic_genre_jazz", "name": "Jazz"},
    {"id": "FEmusic_genre_classical", "name": "Classical"},
    {"id": "FEmusic_genre_country", "name": "Country"},
    {"id": "FEmusic_genre_alternative", "name": "Alternative"},
    {"id": "FEmusic_genre_indie", "name": "Indie"},
    {"id": "FEmusic_genre_metal", "name": "Metal"},
    {"id": "FEmusic_genre_latin", "name": "Latin"},
    {"id": "FEmusic_genre_reggae", "name": "Reggae"},
    {"id": "FEmusic_genre_blues", "name": "Blues"},
    {"id": "FEmusic_genre_folk", "name": "Folk"},
    {"id": "FEmusic_genre_soul", "name": "Soul"},
    {"id": "FEmusic_genre_funk", "name": "Funk"},
    {"id": "FEmusic_genre_dance", "name": "Dance"},
]

@app.get("/sw.js")
async def sw_js():
    sw_code = """const CACHE = 'verse-v2';
const URLS = ['/'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(URLS)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(clients.claim());
});

self.addEventListener('fetch', e => {
  const u = e.request.url;
  if (u.includes('/ws/') || e.request.headers.get('upgrade')?.toLowerCase() === 'websocket') return;
  e.respondWith(
    caches.match(e.request).then(r =>
      r ||
      fetch(e.request).then(res => {
        if (res.ok && e.request.url.startsWith(self.location.origin)) {
          const cachePromise = caches.open(CACHE);
          cachePromise.then(cache => {
            cache.put(e.request, res.clone());
          });
        }
        return res;
      }).catch(() => caches.match('/'))
    )
  );
});"""
    return Response(content=sw_code, media_type="application/javascript")

@app.get("/manifest.json")
async def manifest():
    icons = []
    logo_path = os.path.join(BASE_DIR, "verse-logo.jpg")
    if os.path.exists(logo_path):
        icons = [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
        ]
    manifest = {
        "name": "Verse",
        "short_name": "Verse",
        "description": "Music streaming app",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#000000",
        "theme_color": "#3b5bdb",
        "icons": icons,
    }
    return manifest

@app.get("/", response_class=HTMLResponse)
async def root():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(script_dir, "static", "index.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"<h1>Error: index.html not found at {html_path}</h1>"

def parse_duration(duration_str):
    if not duration_str:
        return "0:00"
    return duration_str

def get_high_res_thumbnail(thumbnails_list, video_id=None):
    if thumbnails_list and len(thumbnails_list) > 0:
        url = thumbnails_list[-1].get('url', '')
        if url:
            if "=" in url:
                return url.split('=')[0] + "=w300-h300"
            return url
    if video_id:
        return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    return ""

def format_track(track):
    artists = ", ".join([a['name'] for a in track.get('artists', [])]) if track.get('artists') else "Unknown Artist"
    album_raw = track.get('album', {})
    if isinstance(album_raw, dict):
        album = album_raw.get('name', 'Single')
    elif isinstance(album_raw, str):
        album = album_raw or 'Single'
    else:
        album = 'Single'
    vid = track.get('videoId', '')
    
    # Get thumbnail from ytmusicapi (search uses 'thumbnails', watch playlist uses 'thumbnail')
    thumb = get_high_res_thumbnail(track.get('thumbnails') or track.get('thumbnail', []), video_id=vid)
    
    return {
        "id": vid,
        "title": track.get('title', 'Unknown'),
        "artist": artists,
        "album": album,
        "thumbnail": thumb,
        "duration": track.get('duration') or track.get('length', '3:30')
    }

async def enrich_with_spotify_artwork(tracks):
    """Get Spotify covers for all tracks in parallel"""
    async def enrich_one(track):
        thumb = await spotify.get_track_artwork(
            track.get('title', ''), 
            track.get('artist', '')
        )
        if thumb:
            track['thumbnail'] = thumb
        return track
    
    return await asyncio.gather(*[enrich_one(t) for t in tracks])

async def enrich_sections(sections: list) -> list:
    all_tracks = []
    for section in sections:
        all_tracks.extend(section.get('tracks', []))
    if all_tracks:
        enriched = await enrich_with_spotify_artwork(all_tracks)
    return sections

async def enrich_single_track(track: dict) -> dict:
    if track and track.get('title'):
        thumb = await spotify.get_track_artwork(track.get('title', ''), track.get('artist', ''))
        if thumb:
            track['thumbnail'] = thumb
    return track

@app.get("/search")
async def search(q: str):
    try:
        raw_results = ytm.search(q, filter="songs")
        tracks = [format_track(res) for res in raw_results if res.get('videoId')]
        return await enrich_with_spotify_artwork(tracks)
    except Exception as e:
        print(f"Search Error: {e}")
        return []

@app.get("/search_all")
async def search_all(q: str = Query(""), filter_type: str = Query("songs")):
    """Unified search returning songs, artists, albums, playlists."""
    cache_key = f"s:{q}:{filter_type}"
    cached = search_cache.get(cache_key)
    if cached:
        return cached
    try:
        if filter_type == "artists":
            raw = ytm.search(q, filter="artists", limit=20)
            artists = []
            for item in raw:
                if item.get('resultType') == 'artist':
                    artists.append({
                        "name": (item.get('artists', [{}])[0].get('name', '') if item.get('artists') else item.get('title', '')),
                        "browseId": item.get('browseId', ''),
                        "subscribers": item.get('subscribers', ''),
                        "thumbnail": item.get('thumbnails', [{}])[-1].get('url', '') if item.get('thumbnails') else '',
                    })
            return {"artists": artists}
        elif filter_type == "playlists":
            raw = ytm.search(q, filter="playlists", limit=20)
            playlists = []
            for item in raw:
                if item.get('resultType') == 'playlist':
                    playlists.append({
                        "title": item.get('title', ''),
                        "browseId": item.get('browseId', ''),
                        "itemCount": item.get('itemCount', ''),
                        "author": item.get('author', ''),
                        "thumbnail": item.get('thumbnails', [{}])[-1].get('url', '') if item.get('thumbnails') else '',
                    })
            return {"playlists": playlists}
        elif filter_type == "albums":
            raw = ytm.search(q, filter="albums", limit=20)
            albums = []
            for item in raw:
                if item.get('resultType') == 'album':
                    artists = [a.get('name', '') for a in item.get('artists', [])]
                    albums.append({
                        "title": item.get('title', ''),
                        "browseId": item.get('browseId', ''),
                        "artists": artists,
                        "year": item.get('year', ''),
                        "thumbnail": item.get('thumbnails', [{}])[-1].get('url', '') if item.get('thumbnails') else '',
                    })
            return {"albums": albums}
        else:
            raw = ytm.search(q, filter="songs", limit=30)
            songs = []
            artists = []
            playlists = []
            albums = []
            seen_vids = set()
            seen_artists = set()
            for item in raw:
                rt = item.get('resultType', '')
                if rt == 'song':
                    vid = item.get('videoId', '')
                    if vid and vid not in seen_vids:
                        seen_vids.add(vid)
                        songs.append(format_track(item))
                elif rt == 'artist':
                    name = (item.get('artists', [{}])[0].get('name', '') if item.get('artists') else item.get('title', ''))
                    bid = item.get('browseId', '')
                    if bid and name not in seen_artists:
                        seen_artists.add(name)
                        artists.append({
                            "name": name,
                            "browseId": bid,
                            "subscribers": item.get('subscribers', ''),
                            "thumbnail": item.get('thumbnails', [{}])[-1].get('url', '') if item.get('thumbnails') else '',
                        })
                elif rt == 'playlist':
                    playlists.append({
                        "title": item.get('title', ''),
                        "browseId": item.get('browseId', ''),
                        "itemCount": item.get('itemCount', ''),
                        "author": item.get('author', ''),
                        "thumbnail": item.get('thumbnails', [{}])[-1].get('url', '') if item.get('thumbnails') else '',
                    })
                elif rt == 'album':
                    albums.append({
                        "title": item.get('title', ''),
                        "browseId": item.get('browseId', ''),
                        "artists": [a.get('name', '') for a in item.get('artists', [])],
                        "year": item.get('year', ''),
                        "thumbnail": item.get('thumbnails', [{}])[-1].get('url', '') if item.get('thumbnails') else '',
                    })
            if songs:
                songs = await enrich_with_spotify_artwork(songs)
            result = {"songs": songs, "artists": artists, "playlists": playlists, "albums": albums}
            search_cache.set(cache_key, result)
            return result
    except Exception as e:
        print(f"Search all error: {e}")
        return {"songs": [], "artists": [], "playlists": [], "albums": []}

@app.get("/artist_details")
async def artist_details(browseId: str = Query("")):
    """Return artist top songs + albums for search hero card."""
    if not browseId:
        return {"error": "browseId required"}
    cache_key = f"artist:{browseId}"
    cached = artist_cache.get(cache_key)
    if cached:
        return cached
    try:
        a = ytm.get_artist(browseId)
        top_songs = []
        for s in a.get('songs', {}).get('results', []):
            top_songs.append(format_track(s))
        albums = []
        for al in a.get('singles', {}).get('results', []):
            albums.append({
                "title": al.get('title', ''),
                "browseId": al.get('browseId', ''),
                "year": al.get('year', ''),
                "thumbnail": get_high_res_thumbnail(al.get('thumbnails', [])),
            })
        result = {
            "name": a.get('name', ''),
            "subscribers": a.get('subscribers', ''),
            "thumbnail": get_high_res_thumbnail(a.get('thumbnails', [])),
            "songs": top_songs,
            "albums": albums,
        }
        artist_cache.set(cache_key, result)
        return result
    except Exception as e:
        print(f"Artist details error: {e}")
        return {"name": "", "subscribers": "", "thumbnail": "", "songs": [], "albums": []}

@app.get("/album_tracks")
async def album_tracks(browseId: str = Query("")):
    """Return album tracks + metadata for inline expansion."""
    if not browseId:
        return {"error": "browseId required"}
    cache_key = f"album:{browseId}"
    cached = album_cache.get(cache_key)
    if cached:
        return cached
    try:
        a = ytm.get_album(browseId)
        tracks = [format_track(t) for t in a.get('tracks', []) if t.get('videoId')]
        artists = [ar.get('name', '') for ar in a.get('artists', [])]
        result = {
            "title": a.get('title', ''),
            "year": a.get('year', ''),
            "artists": artists,
            "thumbnail": get_high_res_thumbnail(a.get('thumbnails', [])),
            "trackCount": a.get('trackCount', len(tracks)),
            "duration": a.get('duration', ''),
            "tracks": tracks,
        }
        album_cache.set(cache_key, result)
        return result
    except Exception as e:
        print(f"Album tracks error: {e}")
        return {"title": "", "year": "", "artists": [], "thumbnail": "", "trackCount": 0, "duration": "", "tracks": []}

@app.get("/recommendations")
async def get_recommendations(history: str = Query("")):
    try:
        results = []
        seen_ids = set()
        seen_title_artist = set()

        def dedup(track):
            key = (track.get('title','').lower(), track.get('artist','').lower())
            if key in seen_title_artist:
                return False
            seen_title_artist.add(key)
            return True

        seeds = [s.strip() for s in history.split(",") if s.strip()][:3]
        if seeds:
            for seed_id in seeds:
                try:
                    watch = ytm.get_watch_playlist(videoId=seed_id, limit=10)
                    for track in watch.get('tracks', []):
                        vid = track.get('videoId')
                        if vid and vid not in seen_ids and dedup(track):
                            seen_ids.add(vid)
                            results.append(format_track(track))
                except Exception:
                    continue

        if len(results) < 12:
            try:
                charts = ytm.get_charts()
                for source in ['songs', 'trending']:
                    items = charts.get(source, {}).get('items', [])
                    for track in items:
                        vid = track.get('videoId')
                        if vid and vid not in seen_ids and dedup(track) and len(results) < 20:
                            seen_ids.add(vid)
                            results.append(format_track(track))
            except Exception:
                pass

        # Fill remaining with artist search from first result
        if len(results) < 20 and results:
            artists_used = set()
            for r in results:
                artist = r.get('artist', '').split(',')[0].strip()
                if artist and artist not in artists_used:
                    artists_used.add(artist)
                    try:
                        search = ytm.search(artist, filter="songs", limit=5)
                        for track in search:
                            vid = track.get('videoId')
                            if vid and vid not in seen_ids and dedup(track) and len(results) < 20:
                                seen_ids.add(vid)
                                results.append(format_track(track))
                    except Exception:
                        pass
                    if len(results) >= 20:
                        break

        return await enrich_with_spotify_artwork(results[:20])
    except Exception as e:
        print(f"Recommendations error: {e}")
        return []

@app.get("/recommend_by_track")
async def recommend_by_track(id: str = Query(...), count: int = Query(12)):
    """AI-style similar track recommendations using ytmusicapi watch playlist + search."""
    try:
        results = []
        seen_ids = {id}
        watch = ytm.get_watch_playlist(videoId=id, limit=count + 5)
        for track in watch.get('tracks', []):
            vid = track.get('videoId')
            if vid and vid not in seen_ids and len(results) < count:
                seen_ids.add(vid)
                results.append(format_track(track))

        if len(results) < count and results:
            seed_artist = results[0].get('artist', '').split(',')[0].strip()
            if seed_artist:
                try:
                    search_results = ytm.search(seed_artist, filter="songs", limit=count - len(results) + 5)
                    for track in search_results:
                        vid = track.get('videoId')
                        if vid and vid not in seen_ids and len(results) < count:
                            seen_ids.add(vid)
                            results.append(format_track(track))
                except Exception:
                    pass

        return await enrich_with_spotify_artwork(results[:count])
    except Exception as e:
        print(f"Recommend by track error: {e}")
        return []

@app.get("/smart_queue")
async def smart_queue(id: str, count: int = Query(20)):
    try:
        watch = ytm.get_watch_playlist(videoId=id, limit=count + 5)
        recommendations = []
        seen_ids = {id}
        for track in watch.get('tracks', []):
            vid = track.get('videoId')
            if vid and vid not in seen_ids and len(recommendations) < count:
                seen_ids.add(vid)
                recommendations.append(format_track(track))
        
        # Fallback: search by first result's artist if not enough
        if len(recommendations) < count and recommendations:
            seed_artist = recommendations[0].get('artist', '').split(',')[0].strip()
            if seed_artist:
                try:
                    search = ytm.search(seed_artist, filter="songs", limit=count - len(recommendations) + 5)
                    for track in search:
                        vid = track.get('videoId')
                        if vid and vid not in seen_ids and len(recommendations) < count:
                            seen_ids.add(vid)
                            recommendations.append(format_track(track))
                except Exception:
                    pass
        
        return await enrich_with_spotify_artwork(recommendations)
    except Exception as e:
        print(f"Smart queue error: {e}")
        return []


@app.get("/discover")
async def discover(mood: Optional[str] = Query(None), genre: Optional[str] = Query(None)):
    """Returns curated sections for the Discover tab."""
    sections = []

    def make_section(title, items):
        return {"title": title, "tracks": items}

    try:
        charts = ytm.get_charts()
        song_items = charts.get('songs', {}).get('items', [])
        if song_items:
            sections.append(make_section("Top Charts", [format_track(t) for t in song_items[:20] if t.get('videoId')]))

        trending = charts.get('trending', {}).get('items', [])
        if trending:
            sections.append(make_section("Trending Now", [format_track(t) for t in trending[:20] if t.get('videoId')]))
    except Exception:
        pass

    if mood or genre:
        category_id = mood or genre
        try:
            playlists = ytm.get_mood_playlists(category_id)
            if playlists:
                for pl in playlists[:3]:
                    pid = pl.get('playlistId')
                    if pid:
                        try:
                            pl_data = ytm.get_playlist(pid, limit=8)
                            tracks = [format_track(t) for t in pl_data.get('tracks', []) if t.get('videoId')]
                            if tracks:
                                sections.append(make_section(pl.get('title', category_id), tracks))
                        except Exception:
                            continue
        except Exception:
            pass

    if not mood and not genre:
        try:
            explore = ytm.get_mood_categories()
            if explore:
                categories = list(explore.keys())[:4]
                for cat in categories:
                    try:
                        playlists = ytm.get_mood_playlists(cat)
                        if playlists:
                            pid = playlists[0].get('playlistId')
                            if pid:
                                pl_data = ytm.get_playlist(pid, limit=6)
                                tracks = [format_track(t) for t in pl_data.get('tracks', []) if t.get('videoId')]
                                if tracks:
                                    cat_name = cat.replace('FEmusic_', '').replace('_', ' ').title()
                                    sections.append(make_section(cat_name, tracks))
                    except Exception:
                        continue
        except Exception:
            pass

    if not sections:
        try:
            fallback = ytm.search("popular music", filter="songs", limit=12)
            if fallback:
                sections.append(make_section("Popular Now", [format_track(t) for t in fallback if t.get('videoId')]))
        except Exception:
            pass
    return await enrich_sections(sections)


@app.get("/home_feed")
async def home_feed(history: str = Query(""), recently_played_ids: str = Query("")):
    """Returns a personalized home feed: recent recommendations + fresh picks."""
    feed = []
    seen_ids = set()

    recent_ids = [r.strip() for r in recently_played_ids.split(",") if r.strip()][:5]
    if recent_ids:
        results = []
        for rid in recent_ids:
            try:
                watch = ytm.get_watch_playlist(videoId=rid, limit=10)
                for track in watch.get('tracks', []):
                    vid = track.get('videoId')
                    if vid and vid not in seen_ids:
                        seen_ids.add(vid)
                        results.append(format_track(track))
            except Exception:
                continue
        if results:
            feed.append({"title": "Recommended For You", "tracks": results[:15]})

    if len(seen_ids) < 25:
        try:
            charts = ytm.get_charts()
            song_items = charts.get('songs', {}).get('items', [])
            fresh = []
            for t in song_items:
                vid = t.get('videoId')
                if vid and vid not in seen_ids and len(fresh) < 15:
                    seen_ids.add(vid)
                    fresh.append(format_track(t))
            if fresh:
                feed.append({"title": "Trending Tracks", "tracks": fresh})
        except Exception:
            pass

    if not feed:
        try:
            fallback = ytm.search("new music", filter="songs", limit=12)
            if fallback:
                feed.append({"title": "New Music", "tracks": [format_track(t) for t in fallback if t.get('videoId')]})
        except Exception:
            pass

    try:
        chill = ytm.search("chill vibes", filter="songs", limit=10)
        chill_tracks = [format_track(t) for t in chill if t.get('videoId') and t.get('videoId') not in seen_ids]
        if chill_tracks:
            feed.append({"title": "Chill Vibes", "tracks": chill_tracks[:10]})
    except Exception:
        pass

    try:
        throwback = ytm.search("throwback hits", filter="songs", limit=10)
        throwback_tracks = [format_track(t) for t in throwback if t.get('videoId') and t.get('videoId') not in seen_ids]
        if throwback_tracks:
            feed.append({"title": "Throwback Hits", "tracks": throwback_tracks[:10]})
    except Exception:
        pass

    return await enrich_sections(feed)


@app.get("/mood_categories")
async def mood_categories():
    """Returns mood and genre categories for the Discover tab."""
    try:
        moods = ytm.get_mood_categories()
        return {"moods": list(moods.keys())[:20]} if moods else {"moods": [m["id"] for m in YTM_MOOD_CATEGORIES]}
    except Exception:
        return {"moods": [m["id"] for m in YTM_MOOD_CATEGORIES]}

@app.get("/mood_playlists")
async def mood_playlists(category: str = Query(...)):
    """Returns playlists for a given mood or genre category."""
    try:
        playlists = ytm.get_mood_playlists(category)
        if not playlists:
            return []
        results = []
        for pl in playlists[:5]:
            pid = pl.get('playlistId')
            if pid:
                try:
                    data = ytm.get_playlist(pid, limit=8)
                    tracks = [format_track(t) for t in data.get('tracks', []) if t.get('videoId')]
                    if tracks:
                        results.append({
                            "title": pl.get('title', 'Playlist'),
                            "tracks": tracks
                        })
                except Exception:
                    continue
        return await enrich_sections(results)
    except Exception as e:
        print(f"Mood playlists error: {e}")
        return []

@app.get("/thumb")
async def thumb_proxy(url: str = Query(...)):
    """Proxies thumbnail images to avoid CORS/blocking issues."""
    loop = asyncio.get_event_loop()
    try:
        def fetch():
            return requests.get(url, stream=True, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resp = await loop.run_in_executor(None, fetch)
        if resp.status_code == 200:
            return StreamingResponse(resp.iter_content(chunk_size=4096), media_type=resp.headers.get("content-type", "image/jpeg"))
        raise HTTPException(status_code=404)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Thumbnail proxy failed")

@app.get("/autocomplete")
async def autocomplete(q: str = Query("")):
    """Returns search suggestions for autocomplete."""
    if not q or len(q) < 2:
        return []
    try:
        suggestions = ytm.get_search_suggestions(q)
        return suggestions[:8]
    except Exception:
        return []

@app.get("/search_videos")
async def search_videos(q: str = Query(""), limit: int = Query(20)):
    """Searches for music videos."""
    try:
        raw = ytm.search(q, filter="videos", limit=limit)
        return [format_track(t) for t in raw if t.get('videoId')]
    except Exception as e:
        print(f"Video search error: {e}")
        return []

@app.get("/youtube_playlist")
async def youtube_playlist(id: str = Query(...), limit: int = Query(50)):
    """Fetches a YouTube Music playlist."""
    try:
        data = ytm.get_playlist(id, limit=limit)
        tracks = [format_track(t) for t in data.get('tracks', []) if t.get('videoId')]
        return {
            "title": data.get('title', 'Playlist'),
            "description": data.get('description', ''),
            "thumbnail": get_high_res_thumbnail(data.get('thumbnails', [])),
            "trackCount": data.get('trackCount', len(tracks)),
            "tracks": await enrich_with_spotify_artwork(tracks)
        }
    except Exception as e:
        print(f"YouTube playlist error: {e}")
        return {"title": "Error", "tracks": [], "trackCount": 0}

@app.get("/resolve_url")
async def resolve_url(url: str = Query(...)):
    """Resolves a YouTube URL to extract playlist or video info."""
    import re
    pl_match = re.search(r'[?&]list=([A-Za-z0-9_-]+)', url)
    if pl_match:
        return {"type": "playlist", "id": pl_match.group(1)}
    vid_match = re.search(r'(?:v=|youtu\.be/|/v/|shorts/)([A-Za-z0-9_-]{11})', url)
    if vid_match:
        return {"type": "video", "id": vid_match.group(1)}
    return {"type": "unknown"}

@app.get("/import_spotify")
async def import_spotify(url: str = Query(...)):
    """Import a Spotify playlist - fetches track names and searches on YTMusic."""
    import re
    pl_match = re.search(r'playlist/([A-Za-z0-9]+)', url)
    if not pl_match:
        return {"error": "Invalid Spotify playlist URL", "tracks": []}
    pl_id = pl_match.group(1)
    token = spotify.get_token()
    if not token:
        return {"error": "Spotify credentials not configured", "tracks": []}
    try:
        resp = requests.get(
            f"https://api.spotify.com/v1/playlists/{pl_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        if resp.status_code != 200:
            return {"error": "Failed to fetch Spotify playlist", "tracks": []}
        data = resp.json()
        pl_title = data.get('name', 'Imported Playlist')
        items = data.get('tracks', {}).get('items', [])
        tracks = []
        for item in items[:50]:
            t = item.get('track')
            if not t:
                continue
            title = t.get('name', '')
            artist = ', '.join([a.get('name', '') for a in t.get('artists', [])])
            if title and artist:
                try:
                    results = ytm.search(f"{title} {artist}", filter="songs", limit=1)
                    if results and results[0].get('videoId'):
                        tracks.append(format_track(results[0]))
                except Exception:
                    continue
        return {"title": pl_title, "tracks": await enrich_with_spotify_artwork(tracks)}
    except Exception as e:
        print(f"Spotify import error: {e}")
        return {"error": str(e), "tracks": []}

@app.get("/artist_radio")
async def artist_radio(artist_name: str = Query(...), limit: int = Query(30)):
    """Generate a radio mix from an artist."""
    try:
        results = []
        seen_ids = set()
        search_res = ytm.search(artist_name, filter="songs", limit=5)
        for t in search_res:
            vid = t.get('videoId')
            if vid and vid not in seen_ids:
                seen_ids.add(vid)
                results.append(format_track(t))
        if results:
            watch = ytm.get_watch_playlist(videoId=results[0]['id'], limit=limit)
            for t in watch.get('tracks', []):
                vid = t.get('videoId')
                if vid and vid not in seen_ids and len(results) < limit:
                    seen_ids.add(vid)
                    results.append(format_track(t))
        return await enrich_with_spotify_artwork(results[:limit])
    except Exception as e:
        print(f"Artist radio error: {e}")
        return []

_video_cache = {}
_VIDEO_CACHE_TTL = 300

@app.get("/get_video")
async def get_video(id: str, quality: int = Query(1080)):
    """Extract video-only stream URL via yt-dlp (no audio, for visual sync)."""
    now = time.time()
    cache_key = f"{id}_{quality}"
    cached = _video_cache.get(cache_key)
    if cached and now - cached['time'] < _VIDEO_CACHE_TTL:
        return {"url": cached['url'], "quality": quality}
    video_url = f"https://www.youtube.com/watch?v={id}"
    fmt = '22/18/best[ext=mp4]/best'
    cookie_path = os.path.join(os.path.dirname(__file__), "cookies.txt")
    ydl_opts = {
    'format': 'bestaudio/best',
    'cookiefile': cookie_path,
    'quiet': True,
    'extractor_args': {
        'youtube': {
            'player_client': ['web_safari', 'web', 'android', 'ios'],

    'noplaylist': True,
    'cachedir': False,
    'youtube_include_dash_manifest': False,
    'impersonate': 'android',  # Crucial for cloud hosting bypass
    'http_headers': {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }
}
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        try:
            def extract():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(video_url, download=False)
            info = await loop.run_in_executor(pool, extract)
            _video_cache[cache_key] = {'url': info['url'], 'time': now}
            return {"url": info['url'], "quality": quality}
        except Exception as e:
            print(f"Video extraction failed: {e}")
            raise HTTPException(status_code=500, detail="Video stream unavailable")

@app.get("/get_loudness")
async def get_loudness(id: str):
    """Measure track loudness via ffmpeg for normalization."""
    cache_key = f"loud_{id}"
    cached = _audio_cache.get(cache_key)
    if cached:
        return {"loudness": cached['loudness']}
    try:
        audio_data = await get_audio(id)
        url = audio_data['url']
        loop = asyncio.get_event_loop()
        def measure():
            import subprocess
            cmd = ['ffmpeg', '-i', url, '-af', 'loudnorm=print_format=json', '-f', 'null', '-']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            import json as _json
            stderr = result.stderr
            start = stderr.rfind('{')
            end = stderr.rfind('}') + 1
            if start >= 0 and end > start:
                data = _json.loads(stderr[start:end])
                return float(data.get('input_i', -14))
            return -14.0
        loudness = await loop.run_in_executor(None, measure)
        _audio_cache[cache_key] = {'loudness': loudness, 'time': time.time()}
        return {"loudness": loudness}
    except Exception:
        return {"loudness": -14.0}

@app.get("/video_info")
async def video_info(id: str):
    """Returns metadata (title, artist, thumbnail) for a video ID."""
    try:
        results = ytm.search(f"https://www.youtube.com/watch?v={id}", filter="songs", limit=1)
        if results and results[0].get('videoId') == id:
            return await enrich_single_track(format_track(results[0]))
        results = ytm.search(id, filter="songs", limit=5)
        for r in results:
            if r.get('videoId') == id:
                return await enrich_single_track(format_track(r))
        results = ytm.search(id, limit=5)
        for r in results:
            if r.get('videoId') == id:
                return await enrich_single_track(format_track(r))
        return {"id": id, "title": "Unknown", "artist": "Unknown Artist", "thumbnail": "", "duration": "0:00"}
    except Exception:
        return {"id": id, "title": "Unknown", "artist": "Unknown Artist", "thumbnail": "", "duration": "0:00"}

# Simple cache for get_audio to avoid repeated yt-dlp extractions
_audio_cache = {}
_AUDIO_CACHE_TTL = 1800  # 30 minutes

@app.get("/get_audio")
async def get_audio(id: str):
    now = time.time()
    cached = _audio_cache.get(id)
    if cached and now - cached['time'] < _AUDIO_CACHE_TTL:
        return {"url": cached['url']}

    video_url = f"https://www.youtube.com/watch?v={id}"
    ydl_opts = {
        'format': 'bestaudio/best',
        'quiet': True,
        'noplaylist': True,
        'cachedir': False,
        'youtube_include_dash_manifest': False,
    }
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        try:
            def extract():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(video_url, download=False)
            info = await loop.run_in_executor(pool, extract)
            _audio_cache[id] = {'url': info['url'], 'time': now}
            return {"url": info['url']}
        except Exception as e:
            print(f"Extraction failed: {e}")
            raise HTTPException(status_code=500, detail="Audio stream unavailable")

@app.get("/proxy_stream")
async def proxy_stream(url: str = Query(..., description="The raw googlevideo audio URL")):
    try:
        resp = requests.get(url, stream=True, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*',
            'Range': 'bytes=0-',
        })
        if resp.status_code == 200 or resp.status_code == 206:
            media_type = resp.headers.get('content-type', 'audio/mpeg')
            return StreamingResponse(resp.iter_content(chunk_size=8192), media_type=media_type)
        raise HTTPException(status_code=404, detail="Stream not available")
    except requests.RequestException as e:
        print(f"Stream proxy failed: {e}")
        raise HTTPException(status_code=500, detail="Stream proxy failed")
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")

@app.get("/proxy_video")
async def proxy_video(url: str = Query(..., description="The raw googlevideo video URL")):
    try:
        range_header = None
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*',
        }
        range_match = None
        resp = requests.get(url, stream=True, timeout=30, headers=headers)
        if resp.status_code in (200, 206):
            media_type = resp.headers.get('content-type', 'video/mp4')
            content_length = resp.headers.get('content-length')
            content_range = resp.headers.get('content-range')
            resp_headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Content-Type': media_type,
            }
            if content_length:
                resp_headers['Content-Length'] = content_length
            if content_range:
                resp_headers['Content-Range'] = content_range
            def iter_content():
                try:
                    for chunk in resp.iter_content(chunk_size=65536):
                        yield chunk
                finally:
                    resp.close()
            from starlette.responses import StreamingResponse
            status = 206 if resp.status_code == 206 else 200
            return StreamingResponse(iter_content(), status_code=status, headers=resp_headers, media_type=media_type)
        raise HTTPException(status_code=404, detail="Video stream not available")
    except requests.RequestException as e:
        print(f"Video proxy failed: {e}")
        raise HTTPException(status_code=500, detail="Video proxy failed")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")

@app.get("/download")
async def download_audio(
    id: str,
    title: str,
    artist: str = Query("Unknown Artist"),
    album: str = Query("Verse Archive"),
    thumb: str = Query("")
):
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, f"{id}.mp3")
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': file_path.replace('.mp3', ''),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'quiet': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={id}"])
        if os.path.exists(file_path):
            try:
                audio_tags = ID3(file_path)
            except Exception:
                audio_tags = ID3()
            audio_tags.add(TIT2(encoding=3, text=title))
            audio_tags.add(TPE1(encoding=3, text=artist))
            audio_tags.add(TALB(encoding=3, text=album))
            if thumb:
                try:
                    clean_thumb_url = thumb.split('=')[0] + "=w300-h300" if "=" in thumb else thumb
                    img_response = requests.get(clean_thumb_url, timeout=8)
                    if img_response.status_code == 200:
                        audio_tags.add(
                            APIC(
                                encoding=3,
                                mime='image/jpeg' if '.jpg' in clean_thumb_url or '=w' in clean_thumb_url else 'image/png',
                                type=3,
                                desc='Front Cover',
                                data=img_response.content
                            )
                        )
                except Exception as img_err:
                    print(f"Album art failed: {img_err}")
            audio_tags.save(file_path)
            safe_filename = f"{artist} - {title}.mp3".replace('/', '_').replace('\\', '_')
            return FileResponse(path=file_path, filename=safe_filename, media_type='audio/mpeg')
    except Exception as e:
        print(f"Download error: {e}")
        raise HTTPException(status_code=500, detail="Download failed")

# ===== WEB SOCKET (Listen Together Multi-Room Sync) =====
@app.websocket("/ws/streamy-sync")
async def websocket_sync(ws: WebSocket):
    await ws.accept()
    room_code = None
    is_host = False
    try:
        data = await ws.receive_json()
        action = data.get("action")
        print(f"WS action: {action}")

        if action == "CREATE":
            room_code = room_mgr.create_room(ws)
            is_host = True
            await ws.send_json({"type": "room_created", "code": room_code})

        elif action == "JOIN":
            code = data.get("code", "").upper()
            state = room_mgr.join_room(code, ws)
            if state is None:
                await ws.send_json({"type": "error", "message": "Room not found"})
                await ws.close()
                return
            room_code = code
            await ws.send_json({"type": "room_joined", "code": code, "state": state})
            room = room_mgr.rooms.get(room_code)
            if room and room["host"]:
                await room["host"].send_json({"type": "listener_joined", "count": len(room["listeners"])})

        while True:
            msg = await ws.receive_json()
            if not room_code:
                continue
            action = msg.get("action")
            room = room_mgr.rooms.get(room_code)
            if not room:
                continue

            if action == "PLAY":
                pos = msg.get("position", 0.0)
                room_mgr.update_state(room_code, track=msg.get("track"), position=pos, status="playing")
                payload = {"type": "CLIENT_PLAY", "track": room["current_track"], "position": pos}
                for listener in room["listeners"]:
                    await listener.send_json(payload)

            elif action == "PAUSE":
                pos = msg.get("position", 0.0)
                room_mgr.update_state(room_code, position=pos, status="paused")
                payload = {"type": "CLIENT_PAUSE", "position": pos}
                for listener in room["listeners"]:
                    await listener.send_json(payload)

            elif action == "RESUME":
                pos = msg.get("position", 0.0)
                room_mgr.update_state(room_code, position=pos, status="playing")
                payload = {"type": "CLIENT_RESUME", "position": pos}
                for listener in room["listeners"]:
                    await listener.send_json(payload)

            elif action == "SEEK":
                pos = msg.get("position", 0.0)
                room_mgr.update_state(room_code, position=pos)
                payload = {"type": "CLIENT_SEEK", "position": pos}
                for listener in room["listeners"]:
                    await listener.send_json(payload)

            elif action == "SYNC":
                pos = msg.get("position", 0.0)
                status = msg.get("status", "playing")
                room_mgr.update_state(room_code, position=pos, status=status)
                payload = {"type": "CLIENT_SYNC", "position": pos, "status": status}
                for listener in room["listeners"]:
                    await listener.send_json(payload)

            elif action == "QUEUE":
                room["queue"].append(msg.get("track"))
                payload = {"type": "CLIENT_QUEUE_UPDATE", "queue": room["queue"]}
                for listener in room["listeners"]:
                    await listener.send_json(payload)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        if room_code:
            room = room_mgr.rooms.get(room_code)
            if room:
                if is_host:
                    for listener in room["listeners"]:
                        try:
                            await listener.send_json({"type": "host_disconnected"})
                            await listener.close(code=1000)
                        except Exception:
                            pass
                    del room_mgr.rooms[room_code]
                else:
                    if ws in room["listeners"]:
                        room["listeners"].remove(ws)
                    if room["host"]:
                        try:
                            await room["host"].send_json({"type": "listener_left", "count": len(room["listeners"])})
                        except Exception:
                            pass

# ===== v3.0 SMART ENDPOINTS =====

@app.get("/mix/mood")
async def mood_mix(type: str = Query("study")):
    """Generate a mood-based playlist using YTMusic mood categories."""
    MOOD_MAP = {
        "study": "FEmusic_mood_category_focus",
        "gym": "FEmusic_mood_category_workout",
        "sad": "FEmusic_mood_category_sad",
        "driving": "FEmusic_mood_category_commute",
        "party": "FEmusic_mood_category_party",
        "focus": "FEmusic_mood_category_focus",
    }
    category = MOOD_MAP.get(type.lower(), "FEmusic_mood_category_focus")
    tracks = []
    seen_ids = set()
    try:
        playlists = ytm.get_mood_playlists(category)
        if playlists:
            for pl in playlists[:3]:
                pid = pl.get('playlistId')
                if pid:
                    try:
                        data = ytm.get_playlist(pid, limit=15)
                        for t in data.get('tracks', []):
                            vid = t.get('videoId')
                            if vid and vid not in seen_ids and len(tracks) < 30:
                                seen_ids.add(vid)
                                tracks.append(format_track(t))
                    except Exception:
                        continue
    except Exception as e:
        print(f"Mood mix playlist error: {e}")

    if not tracks:
        search_terms = {
            "study": "lo-fi study beats",
            "gym": "workout motivation",
            "sad": "sad songs",
            "driving": "driving playlist",
            "party": "party hits",
            "focus": "focus instrumental",
        }
        query = search_terms.get(type.lower(), f"{type} music")
        try:
            search = ytm.search(query, filter="songs", limit=25)
            for t in search:
                vid = t.get('videoId')
                if vid and vid not in seen_ids and len(tracks) < 25:
                    seen_ids.add(vid)
                    tracks.append(format_track(t))
        except Exception as e:
            print(f"Mood mix search error: {e}")

    return {"tracks": await enrich_with_spotify_artwork(tracks), "title": f"{type.title()} Mix"}


@app.get("/radio/time")
async def time_travel_radio(year: int = Query(2016)):
    """Generate a radio from a specific year using YTMusic search."""
    tracks = []
    try:
        search = ytm.search(f"{year} hits", filter="songs", limit=25)
        for t in search:
            if t.get('videoId') and len(tracks) < 25:
                tracks.append(format_track(t))
        if len(tracks) < 10:
            search2 = ytm.search(f"top songs {year}", filter="songs", limit=15)
            for t in search2:
                vid = t.get('videoId')
                if vid and vid not in {tr['id'] for tr in tracks} and len(tracks) < 25:
                    tracks.append(format_track(t))
    except Exception as e:
        print(f"Time travel radio error: {e}")
    return {"tracks": await enrich_with_spotify_artwork(tracks), "title": f"Radio from {year}", "year": year}


@app.post("/ai/playlist")
async def ai_playlist(request: Request):
    """AI-generated playlist using Groq API with YTMusic search fallback."""
    try:
        body = await request.json()
        prompt = body.get("prompt", "")
        if not prompt:
            return {"error": "No prompt provided", "tracks": [], "message": "Please enter a prompt."}

        groq_key = os.environ.get("GROQ_API_KEY", "")

        tracks = []
        title = "AI Playlist"
        ai_message = ""

        if groq_key:
            try:
                resp = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.3-70b-versatile",
                        "messages": [
                            {"role": "system", "content": (
                                "You are a music curator. Given a user prompt, output a JSON array of search queries "
                                "(one per song recommendation, 8-15 items). Each query should be a short YouTube Music search string "
                                "like 'Artist Name Song Title'. Output ONLY valid JSON, no explanation. "
                                "Example: [\"Radiohead Creep\", \"Muse Hysteria\", \"Arctic Monkeys Do I Wanna Know\"]"
                            )},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.7,
                        "max_tokens": 500
                    },
                    timeout=15
                )
                if resp.ok:
                    data = resp.json()
                    raw = data["choices"][0]["message"]["content"].strip()
                    import json as _json
                    queries = _json.loads(raw) if raw.startswith("[") else []
                    seen = set()
                    for q in queries[:15]:
                        try:
                            results = ytm.search(q, filter="songs", limit=3)
                            for t in results:
                                vid = t.get("videoId")
                                if vid and vid not in seen:
                                    seen.add(vid)
                                    tracks.append(format_track(t))
                                    break
                        except Exception:
                            pass
                    title_words = prompt.split()[:6]
                    title = " ".join(title_words).title() if title_words else "AI Playlist"
                    ai_message = f"Here's your playlist: \"{title}\" curated by Verse AI!"
            except Exception as e:
                print(f"Groq API error: {e}")

        if not tracks:
            keywords = prompt.lower().replace("playlist", "").replace("for", "").replace("a", "").strip()
            words = [w for w in keywords.split() if len(w) > 2][:5]
            search_query = " ".join(words) if words else "popular music"
            try:
                search = ytm.search(search_query, filter="songs", limit=20)
                for t in search:
                    if t.get("videoId") and len(tracks) < 20:
                        tracks.append(format_track(t))
            except Exception:
                pass
            if len(tracks) < 5:
                try:
                    fallback = ytm.search(prompt[:50], filter="songs", limit=15)
                    for t in fallback:
                        vid = t.get("videoId")
                        if vid and vid not in {tr["id"] for tr in tracks} and len(tracks) < 15:
                            tracks.append(format_track(t))
                except Exception:
                    pass
            title_words = prompt.split()[:6]
            title = " ".join(title_words).title() if title_words else "AI Playlist"
            ai_message = f"Here's your playlist: \"{title}\" based on your request!"

        enriched = await enrich_with_spotify_artwork(tracks)
        return {
            "tracks": enriched,
            "title": title,
            "message": ai_message,
            "prompt": prompt,
        }
    except Exception as e:
        print(f"AI playlist error: {e}")
        return {"error": str(e), "tracks": []}


# ===== v2.0 SOCIAL ENDPOINTS =====

MOCK_USERS = {
    "alex": {
        "display_name": "Alex Rivera",
        "avatar": "",
        "bio": "Music producer & vinyl collector",
        "followers": 234,
        "following": 89,
        "top_artists": [
            {"name": "Kendrick Lamar", "image": ""},
            {"name": "Frank Ocean", "image": ""},
            {"name": "Radiohead", "image": ""},
        ],
        "recently_played": [],
        "public_playlists": [
            {"name": "Late Night Vibes", "tracks": []},
        ],
    },
    "jordan": {
        "display_name": "Jordan Kim",
        "avatar": "",
        "bio": "R&B enthusiast",
        "followers": 156,
        "following": 42,
        "top_artists": [
            {"name": "SZA", "image": ""},
            {"name": "Daniel Caesar", "image": ""},
        ],
        "recently_played": [],
        "public_playlists": [],
    },
    "sam": {
        "display_name": "Sam Patel",
        "avatar": "",
        "bio": "Guitar nerd & indie lover",
        "followers": 312,
        "following": 134,
        "top_artists": [
            {"name": "Mac DeMarco", "image": ""},
            {"name": "Tame Impala", "image": ""},
            {"name": "Clairo", "image": ""},
        ],
        "recently_played": [],
        "public_playlists": [
            {"name": "Indie Essentials", "tracks": []},
        ],
    },
}


@app.get("/users/{username}")
async def get_user_profile(username: str):
    """Returns a public user profile."""
    user = MOCK_USERS.get(username.lower())
    if not user:
        return {"error": "User not found"}
    return user


@app.get("/friends/activity")
async def friends_activity():
    """Returns mock friend activity for social feed."""
    activities = [
        {"user": "Alex", "avatar": "", "status": "Listening to Kendrick Lamar - HUMBLE.", "room": "ABCD", "listening": True},
        {"user": "Jordan", "avatar": "", "status": "Listening to SZA - Kill Bill", "room": None, "listening": True},
        {"user": "Sam", "avatar": "", "status": "In Room: Late Night Vibes", "room": "EFGH", "listening": False},
    ]
    return {"activities": activities}


@app.post("/rooms/create")
async def create_room_http():
    """Create a room via HTTP (returns code, actual WS connection needed for sync)."""
    code = room_mgr._gen_code()
    return {"code": code, "message": "Connect via WebSocket to /ws/streamy-sync with action CREATE to activate"}


@app.get("/rooms/list")
async def list_rooms():
    """List active rooms."""
    room_list = []
    for code, room in room_mgr.rooms.items():
        track_info = None
        if room.get("current_track"):
            track_info = room["current_track"].get("title", "Playing")
        room_list.append({
            "code": code,
            "listeners": len(room.get("listeners", [])),
            "track": track_info,
            "status": room.get("status", "stopped"),
        })
    return {"rooms": room_list}


if __name__ == "__main__":
    import uvicorn
    if spotify._has_credentials():
        print("[OK] Spotify album art integration active (SPOTIFY_CLIENT_ID/SPOTIFY_CLIENT_SECRET set)")
    port = int(os.environ.get("PORT", os.environ.get("STREAMY_PORT", 8000)))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
