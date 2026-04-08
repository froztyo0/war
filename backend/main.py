"""
Global Conflict Intelligence API v3
─────────────────────────────────────────────────────
NEW in v3:
  • OpenSky Network  (free, no key)  → /api/flights   — live aircraft over conflict zones
  • VesselFinder/AIS fallback        → /api/ships      — oil tanker status + shadow fleet flag
  • /api/shortage                    → shortage model, per-country impact + price bands
  • /api/rigs                        → oil rig dataset with threat scoring
  • Country flag codes embedded in all article/market responses
  • ICAO24 → country code resolver
  • Hormuz closure scenario modelling
"""

import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import uvicorn

app = FastAPI(title="Global Conflict Intelligence API", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

load_dotenv(Path(__file__).with_name(".env"))
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL","INFO").upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("conflict_intel")

NEWS_API_KEY      = os.getenv("NEWS_API_KEY", "")
GNEWS_API_KEY     = os.getenv("GNEWS_API_KEY", "")
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")
OXR_APP_ID        = os.getenv("OXR_APP_ID", "")
# Optional: OpenSky credentials give 4000 req/day instead of 400
OPENSKY_USER      = os.getenv("OPENSKY_USER", "")
OPENSKY_PASS      = os.getenv("OPENSKY_PASS", "")
# Twitter/X Basic tier ($100/mo) required for search — free tier is write-only
TWITTER_BEARER    = os.getenv("TWITTER_BEARER_TOKEN", "")

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer env var %s=%r, using %d", name, raw, default)
        return default


def _default_cache_db_path() -> Path:
    if os.getenv("VERCEL"):
        return Path("/tmp/conflict-intel-cache.sqlite3")
    return Path(__file__).with_name("cache.sqlite3")


CACHE_TTL = _env_int("CACHE_TTL_SECONDS", 120)
NEWS_CACHE_TTL = _env_int("NEWS_CACHE_TTL_SECONDS", CACHE_TTL)
FLIGHTS_CACHE_TTL = _env_int("FLIGHTS_CACHE_TTL_SECONDS", 55)
MARKETS_CACHE_TTL = _env_int("MARKETS_CACHE_TTL_SECONDS", 60)
COMMODITIES_CACHE_TTL = _env_int("COMMODITIES_CACHE_TTL_SECONDS", 21600)
FOREX_CACHE_TTL = _env_int("FOREX_CACHE_TTL_SECONDS", 3600)
YAHOO_MARKETS_CACHE_TTL = _env_int("YAHOO_MARKETS_CACHE_TTL_SECONDS", 300)
STALE_CACHE_TTL = _env_int("STALE_CACHE_TTL_SECONDS", 1800)
CACHE_RETENTION_SECONDS = _env_int("CACHE_RETENTION_SECONDS", 86400)
CACHE_DB_PATH = Path(os.getenv("CACHE_DB_PATH", str(_default_cache_db_path())))
NEWS_BASE_LIMIT = 60
_rss_err_ts: dict[str, float] = {}
RSS_ERR_COOL = 300

class CacheStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._memory: dict[str, dict[str, Any]] = {}
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        self._conn.commit()
        self.prune(CACHE_RETENTION_SECONDS)
        logger.info("Cache store ready path=%s retention=%ss", self.db_path, CACHE_RETENTION_SECONDS)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def get(self, key: str) -> dict[str, Any] | None:
        entry = self._memory.get(key)
        if entry is not None:
            return {"data": copy.deepcopy(entry["data"]), "ts": entry["ts"], "layer": "memory"}

        with self._lock:
            row = self._conn.execute(
                "SELECT payload, created_at FROM api_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()

        if row is None:
            return None

        try:
            data = json.loads(row[0])
        except json.JSONDecodeError:
            logger.warning("Dropping unreadable cache entry key=%s", key)
            self.delete(key)
            return None

        entry = {"data": data, "ts": float(row[1])}
        self._memory[key] = entry
        return {"data": copy.deepcopy(data), "ts": entry["ts"], "layer": "sqlite"}

    def set(self, key: str, data: Any) -> None:
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        ts = time.time()
        self._memory[key] = {"data": copy.deepcopy(data), "ts": ts}
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO api_cache(cache_key, payload, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload = excluded.payload,
                    created_at = excluded.created_at
                """,
                (key, payload, ts),
            )
            self._conn.commit()

    def delete(self, key: str) -> None:
        self._memory.pop(key, None)
        with self._lock:
            self._conn.execute("DELETE FROM api_cache WHERE cache_key = ?", (key,))
            self._conn.commit()

    def prune(self, max_age: int) -> None:
        cutoff = time.time() - max_age
        with self._lock:
            self._conn.execute("DELETE FROM api_cache WHERE created_at < ?", (cutoff,))
            self._conn.commit()
        for key, entry in list(self._memory.items()):
            if entry["ts"] < cutoff:
                self._memory.pop(key, None)


cache_store = CacheStore(CACHE_DB_PATH)
_fetch_locks: dict[str, asyncio.Lock] = {}


def _fetch_lock(key: str) -> asyncio.Lock:
    lock = _fetch_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _fetch_locks[key] = lock
    return lock


def _cache_get(key, ttl=CACHE_TTL, *, allow_stale=False, stale_ttl=STALE_CACHE_TTL):
    entry = cache_store.get(key)
    if not entry:
        logger.debug("cache miss key=%s", key)
        return None

    age = time.time() - entry["ts"]
    if age < ttl:
        logger.debug("cache hit key=%s layer=%s age=%.1fs ttl=%ss", key, entry["layer"], age, ttl)
        return entry["data"]

    if allow_stale and age < stale_ttl:
        logger.info("cache stale-hit key=%s layer=%s age=%.1fs", key, entry["layer"], age)
        return entry["data"]

    logger.debug("cache expired key=%s layer=%s age=%.1fs ttl=%ss", key, entry["layer"], age, ttl)
    return None


def _cache_set(key, data):
    cache_store.set(key, data)
    logger.debug("cache set key=%s", key)
    return data


async def _cached_json(key: str, ttl: int, builder, *, stale_ttl: int = STALE_CACHE_TTL):
    cached = _cache_get(key, ttl=ttl)
    if cached is not None:
        return cached

    lock = _fetch_lock(key)
    async with lock:
        cached = _cache_get(key, ttl=ttl)
        if cached is not None:
            return cached

        started = time.perf_counter()
        try:
            data = await builder()
        except Exception as exc:
            stale = _cache_get(key, ttl=ttl, allow_stale=True, stale_ttl=stale_ttl)
            if stale is not None:
                logger.warning("cache fallback key=%s reason=%r", key, exc)
                return stale
            raise

        logger.info("cache refresh key=%s duration_ms=%.1f", key, (time.perf_counter() - started) * 1000)
        return _cache_set(key, data)

class WSManager:
    def __init__(self): self.active = []
    async def connect(self, ws):
        await ws.accept(); self.active.append(ws)
    def disconnect(self, ws):
        if ws in self.active: self.active.remove(ws)
    async def broadcast(self, data):
        dead = []
        for ws in self.active:
            try: await ws.send_json(data)
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

manager = WSManager()


def _is_ws_closed_error(exc: Exception) -> bool:
    if isinstance(exc, WebSocketDisconnect):
        return True
    if isinstance(exc, RuntimeError):
        msg = str(exc).lower()
        return "close message has been sent" in msg or "websocket is not connected" in msg
    return False


async def _ws_send_json(ws: WebSocket, message_type: str, data: Any) -> bool:
    try:
        await ws.send_json({"type": message_type, "data": data})
        return True
    except Exception as exc:
        if _is_ws_closed_error(exc):
            logger.info("WS closed during send type=%s", message_type)
            return False
        raise


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = uuid.uuid4().hex[:8]
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - started) * 1000
        logger.exception(
            "request failed id=%s method=%s path=%s duration_ms=%.1f",
            request_id,
            request.method,
            request.url.path,
            duration_ms,
        )
        raise

    duration_ms = (time.perf_counter() - started) * 1000
    response.headers["X-Request-ID"] = request_id
    logger.info(
        "request id=%s method=%s path=%s status=%s duration_ms=%.1f",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.on_event("shutdown")
async def close_cache_store():
    cache_store.close()

# ═══════════════════════════════════════════════
# ICAO24 → country code
# ═══════════════════════════════════════════════
ICAO_RANGES = [
    (0x700000, 0x73FFFF, "RU"), (0x780000, 0x7BFFFF, "CN"),
    (0x800000, 0x83FFFF, "IN"), (0xA00000, 0xAFFFFF, "US"),
    (0xC00000, 0xC3FFFF, "CA"), (0x380000, 0x3BFFFF, "DE"),
    (0x3C0000, 0x3FFFFF, "FR"), (0x400000, 0x43FFFF, "GB"),
    (0x480000, 0x4BFFFF, "IT"), (0x340000, 0x37FFFF, "ES"),
    (0x458000, 0x45FFFF, "JP"), (0x71C000, 0x71FFFF, "TR"),
    (0x730000, 0x737FFF, "IR"), (0x728000, 0x72BFFF, "IQ"),
    (0x710000, 0x717FFF, "SA"), (0x896000, 0x8963FF, "SG"),
    (0x718000, 0x71BFFF, "AE"), (0x06A000, 0x06AFFF, "IL"),
]
ICAO_PREFIXES = {
    "EP": "IR", "4X": "IL", "UR": "UA", "RA": "RU", "TC": "TR",
    "OD": "LB", "AP": "PK", "VT": "IN", "9V": "SG", "HL": "KR",
    "JA": "JP", "B-H": "HK", "B-L": "TW", "B-K": "CN",
}
FLAG_EMOJI = {
    "IR":"🇮🇷","US":"🇺🇸","SA":"🇸🇦","RU":"🇷🇺","CN":"🇨🇳","IN":"🇮🇳","IQ":"🇮🇶",
    "AE":"🇦🇪","KW":"🇰🇼","QA":"🇶🇦","OM":"🇴🇲","BH":"🇧🇭","PK":"🇵🇰","TR":"🇹🇷",
    "IL":"🇮🇱","GB":"🇬🇧","FR":"🇫🇷","DE":"🇩🇪","NO":"🇳🇴","NL":"🇳🇱","GR":"🇬🇷",
    "SG":"🇸🇬","JP":"🇯🇵","KR":"🇰🇷","MY":"🇲🇾","LR":"🇱🇷","MH":"🇲🇭","PA":"🇵🇦",
    "BS":"🇧🇸","CY":"🇨🇾","MT":"🇲🇹","IT":"🇮🇹","YE":"🇾🇪","NG":"🇳🇬","UA":"🇺🇦",
    "CA":"🇨🇦","AU":"🇦🇺","BR":"🇧🇷","KZ":"🇰🇿","AZ":"🇦🇿","VE":"🇻🇪","HK":"🇭🇰",
    "BD":"🇧🇩","EG":"🇪🇬","LY":"🇱🇾","DZ":"🇩🇿","NG":"🇳🇬","GH":"🇬🇭","ES":"🇪🇸",
}

def icao_to_country(icao24: str) -> str:
    if not icao24: return "??"
    try:
        n = int(icao24, 16)
        for lo, hi, cc in ICAO_RANGES:
            if lo <= n <= hi: return cc
    except: pass
    upper = icao24.upper()
    for pfx, cc in ICAO_PREFIXES.items():
        if upper.startswith(pfx): return cc
    return "??"

def flag(cc: str) -> str:
    return FLAG_EMOJI.get(cc, "🏳")

# ═══════════════════════════════════════════════
# MILITARY CALLSIGN DETECTION
# ═══════════════════════════════════════════════
MIL_PREFIXES = {"RCH","CNV","ASY","DUKE","ROCKY","REACH","HOMER","EVAC","JAKE","PACK",
                "CHAOS","LOBO","FORTE","TIGER","BLADE","RAVEN","EAGLE","HAWK","GHOST",
                "ANVIL","BISON","BRONCO","COBRA","FALCON","VIPER","WEASEL","SPECTRE"}
MIL_SQUAWKS = {"7500","7600","7700","7777","0000"}

def is_military(callsign: str, squawk: str = "") -> bool:
    cs = (callsign or "").strip().upper()
    if squawk in MIL_SQUAWKS: return True
    for pfx in MIL_PREFIXES:
        if cs.startswith(pfx): return True
    if re.match(r'^[A-Z]{3}\d{3,4}$', cs): return False  # typical commercial
    if re.match(r'^[A-Z]{4,}\d*$', cs) and len(cs) <= 6: return True  # likely military
    return False

# ═══════════════════════════════════════════════
# CONFLICT ZONE BOUNDING BOXES
# ═══════════════════════════════════════════════
FLIGHT_ZONES = [
    {"name":"Persian Gulf",     "lamin":22.0,"lomin":48.0,"lamax":30.0,"lomax":60.0},
    {"name":"Red Sea",          "lamin":11.0,"lomin":32.0,"lamax":30.0,"lomax":45.0},
    {"name":"Eastern Med",      "lamin":31.0,"lomin":28.0,"lamax":38.0,"lomax":37.0},
    {"name":"Black Sea",        "lamin":40.0,"lomin":27.0,"lamax":47.0,"lomax":41.0},
    {"name":"Taiwan Strait",    "lamin":22.0,"lomin":118.0,"lamax":28.0,"lomax":124.0},
    {"name":"Korean Peninsula", "lamin":35.0,"lomin":124.0,"lamax":42.0,"lomax":132.0},
    {"name":"Ukraine/Poland",   "lamin":46.0,"lomin":22.0,"lamax":52.0,"lomax":38.0},
]

async def fetch_opensky_zone(client: httpx.AsyncClient, zone: dict) -> list[dict]:
    """Fetch live aircraft from OpenSky Network over a bounding box."""
    aircraft = []
    try:
        params = {
            "lamin": zone["lamin"], "lomin": zone["lomin"],
            "lamax": zone["lamax"], "lomax": zone["lomax"],
        }
        auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER else None
        r = await client.get(
            "https://opensky-network.org/api/states/all",
            params=params, timeout=10,
            auth=auth if auth else httpx.USE_CLIENT_DEFAULT
        )
        r.raise_for_status()
        states = r.json().get("states") or []
        for s in states:
            if len(s) < 10: continue
            icao24, callsign = s[0], (s[1] or "").strip()
            lon, lat = s[5], s[6]
            altitude = s[7] or 0
            on_ground = s[8]
            speed = s[9] or 0
            heading = s[10] or 0
            squawk = s[14] or ""
            if not lat or not lon or on_ground: continue
            cc = icao_to_country(icao24)
            mil = is_military(callsign, squawk)
            aircraft.append({
                "icao24":   icao24,
                "callsign": callsign or "UNKNOWN",
                "lat":      round(lat, 4),
                "lon":      round(lon, 4),
                "altitude": int(altitude),
                "speed":    round(speed, 1),
                "heading":  round(heading, 1),
                "squawk":   squawk,
                "country":  cc,
                "flag":     flag(cc),
                "is_military": mil,
                "zone":     zone["name"],
            })
        logger.info("OpenSky zone=%s: %d aircraft", zone["name"], len(aircraft))
    except Exception as e:
        logger.warning("OpenSky zone=%s error: %r", zone["name"], e)
    return aircraft


async def _fetch_all_flights_uncached() -> dict:
    async with httpx.AsyncClient(headers={"User-Agent": "ConflictDashboard/3.0"}) as client:
        results = await asyncio.gather(*[fetch_opensky_zone(client, z) for z in FLIGHT_ZONES], return_exceptions=True)

    all_flights = []
    zone_counts = {}
    for zone, result in zip(FLIGHT_ZONES, results):
        if isinstance(result, Exception):
            logger.warning("Zone task failed %s: %r", zone["name"], result)
            zone_counts[zone["name"]] = {"ok": False, "count": 0}
            continue
        all_flights.extend(result)
        zone_counts[zone["name"]] = {"ok": True, "count": len(result)}

    # Deduplicate by icao24 (aircraft can appear in overlapping zones)
    seen = set()
    deduped = []
    for f in all_flights:
        if f["icao24"] not in seen:
            seen.add(f["icao24"])
            deduped.append(f)

    # Sort: military first, then by altitude
    deduped.sort(key=lambda f: (-int(f["is_military"]), -f["altitude"]))

    result = {
        "status": "ok",
        "count": len(deduped),
        "military_count": sum(1 for f in deduped if f["is_military"]),
        "zones": zone_counts,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "flights": deduped,
    }
    logger.info(
        "flights rebuilt aircraft=%d military=%d zones=%d",
        result["count"],
        result["military_count"],
        len(result["zones"]),
    )
    return result


async def fetch_all_flights() -> dict:
    """Aggregate flights from all conflict zones."""
    return await _cached_json("flights", ttl=FLIGHTS_CACHE_TTL, builder=_fetch_all_flights_uncached)


# ═══════════════════════════════════════════════
# OIL RIGS — Static enriched dataset with threat scoring
# ═══════════════════════════════════════════════
OIL_RIGS_DATA = [
    # Persian Gulf — highest conflict exposure
    {"id":"ir-kharg",    "name":"Kharg Island Export Terminal","lat":29.24,"lon":50.33,"country":"IR","operator":"NIOC",          "field":"Kharg",        "bpd":"2,400,000","region":"Persian Gulf","type":"terminal", "threat":"HIGH",  "notes":"Primary Iranian crude export — direct conflict target"},
    {"id":"ir-lavan",    "name":"Lavan Offshore Platform",     "lat":26.81,"lon":53.37,"country":"IR","operator":"NIOC",          "field":"Lavan",        "bpd":"120,000",  "region":"Persian Gulf","type":"offshore", "threat":"HIGH",  "notes":"Inside potential conflict zone, Iranian-operated"},
    {"id":"ir-rostam",   "name":"Rostam Platform",             "lat":27.04,"lon":55.18,"country":"IR","operator":"NIOC",          "field":"Rostam",       "bpd":"50,000",   "region":"Persian Gulf","type":"offshore", "threat":"HIGH",  "notes":"Near Strait of Hormuz"},
    {"id":"sa-safaniya", "name":"Safaniya Offshore Field",     "lat":27.84,"lon":48.92,"country":"SA","operator":"Saudi Aramco",  "field":"Safaniya",     "bpd":"1,200,000","region":"Persian Gulf","type":"offshore", "threat":"MED",   "notes":"World's largest offshore oilfield"},
    {"id":"sa-ras-tanura","name":"Ras Tanura Export Terminal", "lat":26.65,"lon":50.16,"country":"SA","operator":"Saudi Aramco",  "field":"Export Hub",   "bpd":"6,500,000","region":"Persian Gulf","type":"terminal", "threat":"MED",   "notes":"World's largest petroleum loading facility"},
    {"id":"ae-zakum",    "name":"Upper Zakum Field",           "lat":24.75,"lon":53.28,"country":"AE","operator":"ADNOC/ExxonMobil","field":"Zakum",      "bpd":"750,000",  "region":"UAE",          "type":"offshore", "threat":"LOW",   "notes":"UAE's largest offshore"},
    {"id":"ae-buhasa",   "name":"Bu Hasa Onshore Field",      "lat":23.50,"lon":53.77,"country":"AE","operator":"ADNOC",         "field":"Bu Hasa",      "bpd":"600,000",  "region":"UAE",          "type":"onshore",  "threat":"LOW",   "notes":"Key ADNOC onshore asset"},
    {"id":"qa-northfield","name":"North Field LNG — Qatar",   "lat":25.50,"lon":51.50,"country":"QA","operator":"QatarEnergy",   "field":"North Field",  "bpd":"77B cf/d", "region":"Qatar",        "type":"LNG",      "threat":"LOW",   "notes":"World's largest natural gas field"},
    {"id":"iq-rumaila",  "name":"Rumaila Supergiant Field",   "lat":30.50,"lon":47.50,"country":"IQ","operator":"BP/Iraq NOC",   "field":"Rumaila",      "bpd":"1,500,000","region":"Iraq",         "type":"onshore",  "threat":"MED",   "notes":"Iraq's most productive field"},
    {"id":"iq-kirkuk",   "name":"Kirkuk Field",               "lat":35.47,"lon":44.39,"country":"IQ","operator":"Iraq NOC",      "field":"Kirkuk",       "bpd":"300,000",  "region":"Iraq",         "type":"onshore",  "threat":"HIGH",  "notes":"Kurdish/Iraqi contested area"},
    {"id":"kw-mina",     "name":"Mina Al Ahmadi Terminal",    "lat":29.07,"lon":48.17,"country":"KW","operator":"KPC",           "field":"Burgan/Export","bpd":"2,900,000","region":"Kuwait",        "type":"terminal", "threat":"MED",   "notes":"Kuwait's primary export terminal"},
    # Hormuz chokepoint
    {"id":"hormuz",      "name":"Strait of Hormuz Chokepoint","lat":26.57,"lon":56.27,"country":"",  "operator":"International", "field":"Transit Zone", "bpd":"17,000,000","region":"Hormuz",       "type":"chokepoint","threat":"HIGH", "notes":"~17M bpd transit daily — most critical energy chokepoint"},
    # Caspian
    {"id":"kz-kashagan", "name":"Kashagan Field",             "lat":45.80,"lon":53.00,"country":"KZ","operator":"NCOC",          "field":"Kashagan",     "bpd":"400,000",  "region":"Caspian Sea",  "type":"offshore", "threat":"LOW",   "notes":"Kazakhstan mega-project"},
    {"id":"az-acg",      "name":"ACG Azeri-Chirag-Gunashli",  "lat":40.48,"lon":50.40,"country":"AZ","operator":"BP/SOCAR",      "field":"ACG",          "bpd":"600,000",  "region":"Caspian Sea",  "type":"offshore", "threat":"LOW",   "notes":"BP flagship Caspian asset"},
    # Red Sea
    {"id":"bab-mandeb",  "name":"Bab el-Mandeb Chokepoint",  "lat":12.60,"lon":43.45,"country":"",  "operator":"ICS Monitor",   "field":"Transit Zone", "bpd":"6,000,000","region":"Red Sea",       "type":"chokepoint","threat":"HIGH", "notes":"Houthi attack zone — ~6M bpd at risk"},
    # North Sea
    {"id":"no-sverdrup", "name":"Johan Sverdrup Platform",    "lat":58.87,"lon":2.85, "country":"NO","operator":"Equinor",       "field":"Johan Sverdrup","bpd":"720,000", "region":"North Sea",    "type":"offshore", "threat":"LOW",   "notes":"Norway's largest active field"},
    {"id":"gb-buzzard",  "name":"Buzzard Field",              "lat":58.10,"lon":1.14, "country":"GB","operator":"Equinor",       "field":"Buzzard",      "bpd":"175,000",  "region":"North Sea",    "type":"offshore", "threat":"LOW",   "notes":"UK North Sea key asset"},
    {"id":"gb-forties",  "name":"Forties Pipeline System",    "lat":57.50,"lon":0.50, "country":"GB","operator":"INEOS",         "field":"Forties",      "bpd":"450,000",  "region":"North Sea",    "type":"pipeline", "threat":"LOW",   "notes":"Critical UK oil infrastructure"},
    # West Africa
    {"id":"ng-bonga",    "name":"Bonga FPSO — Nigeria",       "lat":3.60, "lon":5.45, "country":"NG","operator":"Shell",         "field":"Bonga",        "bpd":"225,000",  "region":"West Africa",  "type":"FPSO",     "threat":"MED",   "notes":"Niger Delta militant activity risk"},
    {"id":"ng-agbami",   "name":"Agbami FPSO — Nigeria",      "lat":4.20, "lon":4.12, "country":"NG","operator":"Chevron",       "field":"Agbami",       "bpd":"250,000",  "region":"West Africa",  "type":"FPSO",     "threat":"MED",   "notes":"Deepwater, moderate piracy risk"},
    # Gulf of Mexico
    {"id":"us-thunder",  "name":"Thunder Horse PDQ",          "lat":28.20,"lon":-88.20,"country":"US","operator":"BP",           "field":"Thunder Horse","bpd":"170,000",  "region":"Gulf of Mexico","type":"offshore","threat":"LOW",   "notes":"US Gulf major platform"},
    {"id":"us-mars",     "name":"Mars-Ursa Platform",         "lat":28.17,"lon":-89.97,"country":"US","operator":"Shell",        "field":"Mars-Ursa",    "bpd":"200,000",  "region":"Gulf of Mexico","type":"offshore","threat":"LOW",   "notes":"Shell deepwater hub"},
    # Russia
    {"id":"ru-sakhalin", "name":"Sakhalin-2 LNG Terminal",   "lat":51.57,"lon":143.47,"country":"RU","operator":"Gazprom/JERA", "field":"Sakhalin-2",   "bpd":"9.6M tpa", "region":"Russia Pacific","type":"LNG",     "threat":"MED",   "notes":"Under Western sanctions pressure"},
    {"id":"ru-priraz",   "name":"Prirazlomnoye Arctic Rig",  "lat":69.23,"lon":57.35, "country":"RU","operator":"Gazprom Neft", "field":"Prirazlomnoye","bpd":"120,000",  "region":"Arctic",       "type":"offshore", "threat":"LOW",   "notes":"Russia's first Arctic offshore platform"},
    # Libya
    {"id":"ly-raslanuf", "name":"Ras Lanuf Terminal",         "lat":30.49,"lon":18.50, "country":"LY","operator":"NOC Libya",   "field":"Ras Lanuf",    "bpd":"220,000",  "region":"Libya",        "type":"terminal", "threat":"HIGH",  "notes":"Civil war disruption risk"},
    # Asia
    {"id":"cn-bohai",    "name":"Bohai Bay Fields — China",  "lat":38.50,"lon":121.00,"country":"CN","operator":"CNOOC",         "field":"Bohai",        "bpd":"600,000",  "region":"China",        "type":"offshore", "threat":"LOW",   "notes":"China's primary offshore"},
    {"id":"vn-namcon",   "name":"Nam Con Son — Vietnam",     "lat":9.70, "lon":108.50,"country":"VN","operator":"PetroVietnam", "field":"Nam Con Son",  "bpd":"200,000",  "region":"South China Sea","type":"offshore","threat":"MED",  "notes":"South China Sea territorial dispute zone"},
    {"id":"in-mumbai",   "name":"Mumbai High — India",       "lat":19.10,"lon":71.50, "country":"IN","operator":"ONGC",         "field":"Mumbai High",  "bpd":"300,000",  "region":"Arabian Sea",  "type":"offshore", "threat":"LOW",   "notes":"India's largest producing field"},
    # Brazil
    {"id":"br-lula",     "name":"Lula Pre-Salt FPSO — Brazil","lat":-22.50,"lon":-40.00,"country":"BR","operator":"Petrobras", "field":"Lula",         "bpd":"900,000",  "region":"Brazil",       "type":"FPSO",     "threat":"LOW",   "notes":"World-class pre-salt deepwater"},
    # Algeria/Egypt
    {"id":"dz-hassi",    "name":"Hassi Messaoud Field",       "lat":31.71,"lon":5.95,  "country":"DZ","operator":"Sonatrach",   "field":"Hassi Messaoud","bpd":"350,000", "region":"Algeria",      "type":"onshore",  "threat":"LOW",   "notes":"Algeria main producing field"},
]

def score_rig_threat(rig: dict, attack_count: int) -> str:
    """Dynamically adjust threat score based on current news attack count."""
    base = rig.get("threat", "LOW")
    region = rig.get("region", "")
    high_risk_regions = {"Persian Gulf", "Hormuz", "Red Sea", "Libya", "Iraq"}
    if region in high_risk_regions and attack_count > 5:
        if base == "MED": return "HIGH"
        if base == "LOW": return "MED"
    return base


# ═══════════════════════════════════════════════
# OIL TANKER / SHIP AIS SIMULATION + REAL DATA
# ═══════════════════════════════════════════════
# Note: Real-time AIS requires paid APIs (MarineTraffic, VesselFinder).
# This endpoint serves enriched static data + can be extended with AIS webhooks.
# Free option: VesselFinder free tier shows limited vessels.

KNOWN_TANKERS = [
    {"name":"Pacific Kestrel",    "imo":"9887654","flag":"MH","cargo":"Crude",   "lat":26.80,"lon":56.50,"status":"TRANSIT",  "from":"Kharg Island, IR","to":"Jamnagar, IN",  "dwt":320000,"route":"Hormuz → Arabian Sea",          "shadow":False},
    {"name":"Al Marjan",          "imo":"9334421","flag":"AE","cargo":"Crude",   "lat":26.40,"lon":57.20,"status":"TRANSIT",  "from":"Jubail, SA",      "to":"Ningbo, CN",     "dwt":280000,"route":"Hormuz → Malacca → China",       "shadow":False},
    {"name":"BW Odin",            "imo":"9712031","flag":"SG","cargo":"LNG",     "lat":25.90,"lon":56.80,"status":"TRANSIT",  "from":"North Field, QA", "to":"Futtsu, JP",     "dwt":153000,"route":"Hormuz → Malacca Strait",        "shadow":False},
    {"name":"Seaways Tanker",     "imo":"9456234","flag":"GR","cargo":"Crude",   "lat":27.10,"lon":55.90,"status":"TRANSIT",  "from":"Basrah, IQ",      "to":"Rotterdam, NL",  "dwt":300000,"route":"Hormuz → Suez Canal",             "shadow":False},
    {"name":"Devon Courage",      "imo":"9334567","flag":"NO","cargo":"Products","lat":26.10,"lon":57.50,"status":"TRANSIT",  "from":"Ras Tanura, SA",  "to":"Mumbai, IN",     "dwt":115000,"route":"Persian Gulf → West India",       "shadow":False},
    {"name":"Marlin Luanda",      "imo":"9678432","flag":"LR","cargo":"Crude",   "lat":14.50,"lon":42.50,"status":"DIVERTED", "from":"Ras Tanura, SA",  "to":"Rotterdam, NL",  "dwt":310000,"route":"DIVERTED: Cape of Good Hope",     "shadow":False,"note":"Houthi threat — Cape re-route adds 10-14 days"},
    {"name":"SC Taipei",          "imo":"9455123","flag":"PA","cargo":"Crude",   "lat":13.00,"lon":44.20,"status":"DIVERTED", "from":"Basrah, IQ",      "to":"South Korea",    "dwt":290000,"route":"DIVERTED: Cape of Good Hope",     "shadow":False,"note":"Houthi attack risk confirmed — re-routed"},
    {"name":"Hafnia Andromeda",   "imo":"9341872","flag":"SG","cargo":"Products","lat":15.80,"lon":41.30,"status":"TRANSIT",  "from":"Jubail, SA",      "to":"Rotterdam, NL",  "dwt":75000, "route":"Red Sea → Suez Canal",              "shadow":False},
    {"name":"New Hellas",         "imo":"9512345","flag":"GR","cargo":"Crude",   "lat":29.50,"lon":32.30,"status":"TRANSIT",  "from":"Ras Tanura, SA",  "to":"Augusta, IT",    "dwt":260000,"route":"Suez Canal → Mediterranean",       "shadow":False},
    {"name":"Ploiesti Star",      "imo":"9234561","flag":"CY","cargo":"Crude",   "lat":31.20,"lon":32.60,"status":"TRANSIT",  "from":"Novorossiysk, RU","to":"Trieste, IT",    "dwt":140000,"route":"Black Sea → Bosphorus → Med",      "shadow":False},
    {"name":"Formosa One",        "imo":"9456789","flag":"PA","cargo":"Crude",   "lat":25.00,"lon":121.20,"status":"ANCHORED","from":"Kuwait",          "to":"Taichung, TW",   "dwt":290000,"route":"Persian Gulf → Taiwan",            "shadow":False,"note":"Awaiting berth — port congestion"},
    {"name":"Energy Sincerity",   "imo":"9678901","flag":"HK","cargo":"Crude",   "lat":31.20,"lon":122.40,"status":"LOADING", "from":"Dalian, CN",      "to":"",               "dwt":320000,"route":"Loading at Dalian SPM",            "shadow":False},
    {"name":"LNG Fukurokuju",     "imo":"9123456","flag":"JP","cargo":"LNG",     "lat":34.68,"lon":135.20,"status":"TRANSIT",  "from":"Sabine Pass, US", "to":"Futtsu, JP",     "dwt":145000,"route":"Pacific LNG route",               "shadow":False},
    {"name":"Maharshi Valmiki",   "imo":"9567890","flag":"IN","cargo":"Crude",   "lat":18.90,"lon":72.80,"status":"LOADING",  "from":"Mundra, IN",      "to":"Arabian Gulf",   "dwt":160000,"route":"Arabian Sea loop",                "shadow":False},
    {"name":"Arctic Lady",        "imo":"9287654","flag":"NO","cargo":"LNG",     "lat":70.00,"lon":28.00,"status":"TRANSIT",  "from":"Hammerfest, NO",  "to":"Rotterdam, NL",  "dwt":140000,"route":"Norwegian Arctic LNG route",       "shadow":False},
    # Shadow fleet (Russia sanctions evasion)
    {"name":"Ns Champion",        "imo":"9234112","flag":"PA","cargo":"Crude",   "lat":57.00,"lon":20.00,"status":"TRANSIT",  "from":"Novorossiysk, RU","to":"Baniyas, SY",    "dwt":130000,"route":"Baltic → Turkey shadow route",      "shadow":True, "note":"Shadow fleet — Russian crude, sanctions evasion"},
    {"name":"Andromeda Star",     "imo":"9345678","flag":"BS","cargo":"Crude",   "lat":52.00,"lon":3.50, "status":"TRANSIT",  "from":"Primorsk, RU",    "to":"India (undiscl.)","dwt":140000,"route":"Baltic exit → Cape route",           "shadow":True, "note":"Shadow fleet — sanctioned vessel, AIS gaps detected"},
    {"name":"Eagle Baltic",       "imo":"9412000","flag":"PA","cargo":"Crude",   "lat":60.50,"lon":26.50,"status":"TRANSIT",  "from":"Ust-Luga, RU",    "to":"Unknown",        "dwt":115000,"route":"Baltic — AIS spoofing suspected",    "shadow":True, "note":"Russia shadow fleet — AIS identity changed twice"},
    {"name":"Pablo (ex-Salvia)",  "imo":"9156783","flag":"PA","cargo":"Crude",   "lat":14.00,"lon":50.00,"status":"TRANSIT",  "from":"Bandar Abbas, IR","to":"Ningbo, CN",     "dwt":170000,"route":"Iran crude → China via Indian Ocean","shadow":True, "note":"Iranian sanctions evasion — transponder gaps"},
]

def enrich_tanker(t: dict) -> dict:
    """Add flag emoji and computed fields."""
    return {
        **t,
        "flag_emoji": FLAG_EMOJI.get(t.get("flag",""), "🏳"),
        "dwt_label":  f"{t['dwt']:,} DWT",
        "is_shadow":  t.get("shadow", False),
        "risk_level": "HIGH" if t.get("status")=="DIVERTED" or t.get("shadow") else
                      "MED"  if t.get("status")=="ANCHORED" else "LOW",
    }


# ═══════════════════════════════════════════════
# SHORTAGE MODEL
# ═══════════════════════════════════════════════
SHORTAGE_COUNTRIES = [
    {"name":"India",       "cc":"IN","import_dep":85,"reserve_days":60, "hormuz_dep":60,"redsea_dep":40,"alt_routes":1},
    {"name":"Japan",       "cc":"JP","import_dep":98,"reserve_days":180,"hormuz_dep":80,"redsea_dep":10,"alt_routes":0},
    {"name":"China",       "cc":"CN","import_dep":70,"reserve_days":90, "hormuz_dep":55,"redsea_dep":30,"alt_routes":2},
    {"name":"South Korea", "cc":"KR","import_dep":98,"reserve_days":120,"hormuz_dep":72,"redsea_dep":15,"alt_routes":0},
    {"name":"Germany",     "cc":"DE","import_dep":95,"reserve_days":90, "hormuz_dep":10,"redsea_dep":35,"alt_routes":2},
    {"name":"Italy",       "cc":"IT","import_dep":90,"reserve_days":95, "hormuz_dep":15,"redsea_dep":45,"alt_routes":1},
    {"name":"Pakistan",    "cc":"PK","import_dep":80,"reserve_days":20, "hormuz_dep":70,"redsea_dep":50,"alt_routes":0},
    {"name":"Turkey",      "cc":"TR","import_dep":93,"reserve_days":70, "hormuz_dep":30,"redsea_dep":40,"alt_routes":1},
    {"name":"France",      "cc":"FR","import_dep":98,"reserve_days":115,"hormuz_dep":12,"redsea_dep":30,"alt_routes":2},
    {"name":"USA",         "cc":"US","import_dep":20,"reserve_days":700,"hormuz_dep":5, "redsea_dep":8, "alt_routes":3},
    {"name":"Bangladesh",  "cc":"BD","import_dep":95,"reserve_days":15, "hormuz_dep":65,"redsea_dep":60,"alt_routes":0},
    {"name":"Singapore",   "cc":"SG","import_dep":100,"reserve_days":90,"hormuz_dep":75,"redsea_dep":20,"alt_routes":1},
    {"name":"Saudi Arabia","cc":"SA","import_dep":0, "reserve_days":9999,"hormuz_dep":0,"redsea_dep":0, "alt_routes":3},
]

PRICE_SCENARIOS = {
    "base":    {"label":"Base Case",     "wti":(78,92),  "brent":(82,98),  "hormuz_mult":0.3,"redsea_mult":0.2,"desc":"Hormuz harassment only, no closure. +10-15% freight premium."},
    "partial": {"label":"Partial Closure","wti":(95,120), "brent":(100,130),"hormuz_mult":0.7,"redsea_mult":0.4,"desc":"1-week Hormuz disruption. ~3M bpd offline. Supply shock begins."},
    "full":    {"label":"Full Closure",   "wti":(140,200),"brent":(150,220),"hormuz_mult":1.0,"redsea_mult":0.6,"desc":"Full 30-day Hormuz closure. ~17M bpd offline. 1973-style energy crisis."},
}

def compute_impact(country: dict, scenario_key: str, attack_count: int) -> dict:
    sc = PRICE_SCENARIOS[scenario_key]
    if country["import_dep"] == 0:
        return {"score": 5, "color": "green", "label": "MINIMAL", "disruption_pct": 0}
    hm = sc["hormuz_mult"] * (country["hormuz_dep"] / 100)
    rm = sc["redsea_mult"] * (country["redsea_dep"] / 100)
    dep = country["import_dep"] / 100
    reserve_risk = max(0, 1 - country["reserve_days"] / 365)
    alt_risk = 1 - min(country["alt_routes"] / 3, 1)
    # Live news factor: more attacks = higher multiplier
    news_boost = min(1.3, 1.0 + attack_count * 0.015)
    raw = (hm * 0.40 + rm * 0.15 + dep * 0.20 + reserve_risk * 0.15 + alt_risk * 0.10) * 100 * news_boost
    score = min(99, max(1, round(raw)))
    color = "red" if score > 70 else "amber" if score > 45 else "blue" if score > 20 else "green"
    label = "CRITICAL" if score > 70 else "HIGH" if score > 45 else "MODERATE" if score > 20 else "LOW"
    disruption = min(95, round((country["hormuz_dep"] * sc["hormuz_mult"] + country["redsea_dep"] * sc["redsea_mult"]) * country["import_dep"] / 10000 * 100))
    return {"score": score, "color": color, "label": label, "disruption_pct": disruption}


# ═══════════════════════════════════════════════
# NEWS SOURCES (unchanged from v2, abbreviated)
# ═══════════════════════════════════════════════
INVALID_XML_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
BARE_AMP_RE    = re.compile(r"&(?!#\d+;|#x[0-9a-fA-F]+;|[a-zA-Z][\w.\-]*;)")
ATTACK_KW    = {"strike","attack","bomb","drone","missile","kill","explosion","blast","airstrike","shelling"}
DIPLOMACY_KW = {"sanction","deal","negotiat","talk","diplomat","treaty","ceasefire","nuclear deal","peace"}
MILITARY_KW  = {"deploy","military","navy","troops","base","exercise","fleet","irgc","pentagon","carrier","nato"}
ECONOMY_KW   = {"oil","price","sanction","trade","crude","energy","export","bitcoin","stock","gas","forex"}
REGION_MAP = {
    "iran":("Iran/Gulf",32.43,53.69),"tehran":("Iran/Gulf",35.69,51.39),"irgc":("Iran/Gulf",35.69,51.39),
    "hormuz":("Persian Gulf",26.57,56.27),"houthi":("Yemen",15.35,44.21),"yemen":("Yemen",15.55,48.52),
    "baghdad":("Iraq",33.34,44.40),"iraq":("Iraq",33.22,43.68),"damascus":("Syria",33.51,36.29),
    "israel":("Israel",31.05,34.86),"gaza":("Gaza",31.35,34.31),"beirut":("Lebanon",33.89,35.50),
    "saudi":("Saudi Arabia",23.89,45.08),"riyadh":("Saudi Arabia",24.69,46.72),
    "kyiv":("Ukraine",50.45,30.52),"ukraine":("Ukraine",49.00,32.00),"donbas":("Ukraine",48.00,37.50),
    "moscow":("Russia",55.75,37.62),"russia":("Russia",61.52,105.31),
    "taiwan":("Taiwan",25.03,121.56),"beijing":("China",39.91,116.39),
    "korea":("Korea",37.55,126.99),"pyongyang":("North Korea",39.02,125.75),
    "washington":("USA",38.91,-77.04),"pentagon":("USA",38.87,-77.05),
}
RELEVANCE_KW = {"iran","us ","usa","america","irgc","nuclear","sanction","hormuz","gulf","houthi",
                "middle east","war","attack","missile","drone","military","pentagon","tehran",
                "ukraine","russia","nato","taiwan","china","korea","dprk","conflict","offensive","ceasefire"}
RSS_FEEDS = {
    "BBC World":        "http://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera":       "https://www.aljazeera.com/xml/rss/all.xml",
    "Reuters World":    "https://rsshub.app/reuters/world",
    "Guardian World":   "https://www.theguardian.com/world/rss",
    "AP Top News":      "https://rsshub.app/apnews/topics/apf-topnews",
    "Guardian Iran":    "https://www.theguardian.com/world/iran/rss",
    "Kyiv Independent": "https://kyivindependent.com/feed/",
    "Defense News":     "https://www.defensenews.com/rss/",
}
NEWSAPI_EP = "https://newsapi.org/v2/everything"
GNEWS_EP   = "https://gnews.io/api/v4/search"
GDELT_EP   = "https://api.gdeltproject.org/api/v2/doc/doc"

def _san(t): return BARE_AMP_RE.sub("&amp;", INVALID_XML_RE.sub("", t or ""))
def _xv(item, paths, ns=None, attrs=None):
    ns = ns or {}
    for p in paths:
        node = item.find(p, ns)
        if node is None: continue
        v = (node.text or "").strip()
        if v: return v
        for a in (attrs or []):
            v = (node.attrib.get(a) or "").strip()
            if v: return v
    return ""
def _aid(title, url=""): return hashlib.md5((title[:80]+url[:40]).lower().encode()).hexdigest()[:12]
def _rwarn(name, e):
    now = time.time()
    if now - _rss_err_ts.get(name, 0) >= RSS_ERR_COOL:
        _rss_err_ts[name] = now; logger.warning("RSS(%s): %r", name, e)

def classify(title, desc):
    text = (title + " " + (desc or "")).lower()
    words = set(re.findall(r'\b\w+\b', text))
    scores = {"attack": len(ATTACK_KW & words), "diplomacy": sum(1 for k in DIPLOMACY_KW if k in text),
              "military": len(MILITARY_KW & words), "economy": len(ECONOMY_KW & words)}
    atype = max(scores, key=scores.get) if max(scores.values()) > 0 else "military"
    sev = min(10, 1 + scores["attack"] * 2 + scores["military"])
    region, lat, lon = "Middle East", 29.0, 53.0
    for kw, (r, la, lo) in REGION_MAP.items():
        if kw in text: region, lat, lon = r, la, lo; break
    return {"type": atype, "lat": lat, "lon": lon, "location": region, "severity": sev}

async def parse_rss(client, name, url):
    articles = []
    try:
        r = await client.get(url, timeout=8, follow_redirects=True); r.raise_for_status()
        xml = _san(r.text)
        if not xml.strip(): return []
        try: root = ET.fromstring(xml)
        except ET.ParseError as e: raise ValueError(str(e)) from e
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)
        for item in items[:20]:
            title = _xv(item, ["title","atom:title"], ns)
            desc  = _xv(item, ["description","summary","content","atom:summary","atom:content"], ns)
            link  = _xv(item, ["link","atom:link"], ns, attrs=["href"]) or "#"
            pub   = _xv(item, ["pubDate","published","updated","atom:published","atom:updated"], ns)
            if not title: continue
            if not any(k in (title+" "+desc).lower() for k in RELEVANCE_KW): continue
            meta = classify(title, desc)
            articles.append({"id": _aid(title, link), "title": title.strip(),
                "description": re.sub(r"<[^>]+>", "", desc or "")[:250].strip(),
                "source": name, "url": link.strip() or "#", "published_at": pub, **meta})
        logger.info("RSS %s: used=%d", name, len(articles))
    except Exception as e: _rwarn(name, e)
    return articles

async def fetch_gdelt(client):
    articles = []
    for q in ["Iran United States military","Ukraine Russia offensive","Houthi attack Red Sea","Taiwan China military"]:
        try:
            r = await client.get(GDELT_EP, params={"query":q,"mode":"artlist","maxrecords":20,"format":"json","timespan":"12h","sort":"DateDesc"}, timeout=12)
            r.raise_for_status()
            for art in (r.json().get("articles") or [])[:8]:
                title = art.get("title","")
                if not title: continue
                meta = classify(title, "")
                articles.append({"id": _aid(title, art.get("url","")), "title": title,
                    "description": f"Coverage: {art.get('seenbycount','N/A')} outlets",
                    "source": art.get("domain","GDELT"), "url": art.get("url","#"), "published_at": art.get("seendate",""), **meta})
        except Exception as e: logger.warning("GDELT(%r): %r", q, e)
    return articles

async def fetch_newsapi(client):
    if not NEWS_API_KEY: return []
    articles = []
    for q in ["Iran US war OR Iran attack OR Houthi","Ukraine Russia war OR Ukraine offensive","Taiwan China military"]:
        try:
            r = await client.get(NEWSAPI_EP, params={"q":q,"sortBy":"publishedAt","language":"en","pageSize":15,"apiKey":NEWS_API_KEY}, timeout=12)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "error": raise RuntimeError(data.get("message",""))
            for art in (data.get("articles") or [])[:15]:
                title, desc = art.get("title","") or "", art.get("description","") or ""
                meta = classify(title, desc)
                articles.append({"id": _aid(title, art.get("url","")), "title": title, "description": desc[:250],
                    "source": (art.get("source") or {}).get("name","NewsAPI"), "url": art.get("url","#"),
                    "published_at": art.get("publishedAt",""), **meta})
        except Exception as e: logger.warning("NewsAPI: %r", e)
    return articles

async def fetch_gnews(client):
    if not GNEWS_API_KEY: return []
    articles = []
    for q in ["Iran US military","Ukraine war","Taiwan China"]:
        try:
            r = await client.get(GNEWS_EP, params={"q":q,"lang":"en","max":10,"apikey":GNEWS_API_KEY}, timeout=12)
            r.raise_for_status()
            for art in (r.json().get("articles") or [])[:10]:
                title, desc = art.get("title",""), art.get("description","") or ""
                meta = classify(title, desc)
                articles.append({"id": _aid(title, art.get("url","")), "title": title, "description": desc[:250],
                    "source": (art.get("source") or {}).get("name","GNews"), "url": art.get("url","#"),
                    "published_at": art.get("publishedAt",""), **meta})
        except Exception as e: logger.warning("GNews: %r", e)
    return articles

REDDIT_SUBS = ["worldnews", "geopolitics", "ukraine", "iran", "ChinaPolicy", "energy"]
TWITTER_QUERIES = [
    "iran attack OR iran war -is:retweet lang:en",
    "ukraine war OR ukraine offensive -is:retweet lang:en",
    "taiwan china military OR south china sea -is:retweet lang:en",
    "houthi attack OR red sea shipping -is:retweet lang:en",
]

async def fetch_reddit(client: httpx.AsyncClient) -> list[dict]:
    """Reddit public JSON API — free, no key, rate-limited to ~1 req/s."""
    cached = _cache_get("reddit", ttl=300)
    if cached is not None: return cached

    reddit_headers = {"User-Agent": "ConflictIntelDashboard/3.0 (educational research; conflict tracking)"}

    async def _fetch_sub(sub: str):
        try:
            r = await client.get(
                f"https://www.reddit.com/r/{sub}/new.json",
                params={"limit": 25}, headers=reddit_headers, timeout=10
            )
            r.raise_for_status()
            return sub, (r.json().get("data") or {}).get("children") or []
        except Exception as e:
            logger.warning("Reddit r/%s: %r", sub, e)
            return sub, []

    results = await asyncio.gather(*[_fetch_sub(s) for s in REDDIT_SUBS])
    articles = []
    for sub, posts in results:
        for p in posts:
            d = p.get("data") or {}
            title = (d.get("title") or "").strip()
            if not title: continue
            body = (d.get("selftext") or "")[:300]
            text_lower = (title + " " + body).lower()
            if not any(kw in text_lower for kw in RELEVANCE_KW): continue
            permalink = d.get("permalink", "")
            url = f"https://reddit.com{permalink}" if permalink else "#"
            score = d.get("score", 0)
            n_comments = d.get("num_comments", 0)
            created = d.get("created_utc", 0)
            meta = classify(title, body)
            articles.append({
                "id": _aid(title, url),
                "title": title,
                "description": f"▲ {score:,} upvotes · 💬 {n_comments} comments",
                "source": f"r/{sub}",
                "url": url,
                "published_at": datetime.fromtimestamp(created, timezone.utc).isoformat() if created else "",
                "platform": "reddit",
                "upvotes": score,
                **meta,
            })
    articles.sort(key=lambda a: -a.get("upvotes", 0))
    return _cache_set("reddit", articles)


async def fetch_twitter(client: httpx.AsyncClient) -> list[dict]:
    """
    Twitter/X API v2 recent tweet search.
    Requires TWITTER_BEARER_TOKEN in .env (Basic tier $100/mo — free tier has no read access).
    Gracefully returns [] if no token configured.
    """
    if not TWITTER_BEARER: return []
    cached = _cache_get("twitter", ttl=300)
    if cached is not None: return cached

    headers = {"Authorization": f"Bearer {TWITTER_BEARER}"}
    articles = []
    for q in TWITTER_QUERIES:
        try:
            r = await client.get(
                "https://api.twitter.com/2/tweets/search/recent",
                params={"query": q, "max_results": 10,
                        "tweet.fields": "created_at,public_metrics,author_id",
                        "expansions": "author_id", "user.fields": "username,name"},
                headers=headers, timeout=10
            )
            if r.status_code in (401, 403):
                logger.warning("Twitter auth error %d — check TWITTER_BEARER_TOKEN (requires Basic tier)", r.status_code)
                break
            r.raise_for_status()
            data = r.json()
            users = {u["id"]: u for u in (data.get("includes") or {}).get("users") or []}
            for tw in (data.get("data") or []):
                text = (tw.get("text") or "").strip()
                if not text: continue
                author = users.get(tw.get("author_id", ""), {})
                username = author.get("username", "unknown")
                m = tw.get("public_metrics") or {}
                likes, rts = m.get("like_count", 0), m.get("retweet_count", 0)
                tid = tw.get("id", "")
                meta = classify(text, "")
                articles.append({
                    "id": _aid(text, tid),
                    "title": text,
                    "description": f"♥ {likes:,} · 🔁 {rts:,}",
                    "source": f"@{username}",
                    "url": f"https://x.com/{username}/status/{tid}",
                    "published_at": tw.get("created_at", ""),
                    "platform": "twitter",
                    "likes": likes,
                    **meta,
                })
        except Exception as e:
            logger.warning("Twitter q=%r: %r", q, e)
    return _cache_set("twitter", articles)


def _news_cache_key(limit: int) -> str:
    return "news_payload" if limit == NEWS_BASE_LIMIT else f"news_payload:{limit}"


def _truncate_news_payload(payload: dict, limit: int) -> dict:
    articles = list((payload.get("articles") or [])[:limit])
    attack_count = sum(1 for article in articles if article.get("type") == "attack")
    ratio = attack_count / len(articles) if articles else 0
    threat = "CRITICAL" if ratio > .40 else "HIGH" if ratio > .25 else "ELEVATED" if ratio > .10 else "LOW"
    return {
        **payload,
        "count": len(articles),
        "attack_count": attack_count,
        "threat_level": threat,
        "articles": articles,
    }


async def _fetch_news_payload_uncached(limit: int) -> dict:
    async with httpx.AsyncClient(headers={"User-Agent": "ConflictDashboard/3.0"}, follow_redirects=True) as client:
        task_map = {
            **{f"rss:{n}": parse_rss(client, n, u) for n, u in RSS_FEEDS.items()},
            "gdelt":   fetch_gdelt(client),
            "newsapi": fetch_newsapi(client),
            "gnews":   fetch_gnews(client),
            "reddit":  fetch_reddit(client),
            "twitter": fetch_twitter(client),
        }
        results = await asyncio.gather(*task_map.values(), return_exceptions=True)
    src_status, all_articles = {}, []
    for name, result in zip(task_map.keys(), results):
        if isinstance(result, Exception): src_status[name] = {"ok": False, "count": 0}
        else: src_status[name] = {"ok": True, "count": len(result)}; all_articles.extend(result)
    seen, deduped = set(), []
    for a in all_articles:
        if a.get("id") not in seen: seen.add(a["id"]); deduped.append(a)
    deduped.sort(key=lambda a: (-a.get("severity",1), a.get("published_at","") or ""))
    deduped = deduped[:limit]
    attack_count = sum(1 for a in deduped if a.get("type") == "attack")
    ratio = attack_count / len(deduped) if deduped else 0
    threat = "CRITICAL" if ratio>.40 else "HIGH" if ratio>.25 else "ELEVATED" if ratio>.10 else "LOW"
    payload = {"status":"ok","count":len(deduped),"threat_level":threat,"attack_count":attack_count,
               "fetched_at":datetime.now(timezone.utc).isoformat(),"sources":src_status,"articles":deduped}
    logger.info("news rebuilt articles=%d attack_count=%d sources=%d", payload["count"], attack_count, len(src_status))
    return payload


async def _build_news_payload(limit=NEWS_BASE_LIMIT):
    limit = max(1, min(limit, 150))
    if limit <= NEWS_BASE_LIMIT:
        base_cached = _cache_get("news_payload", ttl=NEWS_CACHE_TTL)
        if base_cached is not None:
            return _truncate_news_payload(base_cached, limit)

    payload = await _cached_json(
        _news_cache_key(limit),
        ttl=NEWS_CACHE_TTL,
        builder=lambda: _fetch_news_payload_uncached(limit),
    )

    if limit == NEWS_BASE_LIMIT:
        return payload

    if limit > NEWS_BASE_LIMIT and _cache_get("news_payload", ttl=NEWS_CACHE_TTL) is None:
        _cache_set("news_payload", _truncate_news_payload(payload, NEWS_BASE_LIMIT))

    return payload


async def _get_news_context() -> dict:
    cached = _cache_get("news_payload", ttl=NEWS_CACHE_TTL)
    if cached is not None:
        return cached
    return await _build_news_payload(NEWS_BASE_LIMIT)

# ═══════════════════════════════════════════════
# MARKET DATA (from v2)
# ═══════════════════════════════════════════════
async def fetch_crypto(client):
    cached = _cache_get("crypto", ttl=MARKETS_CACHE_TTL)
    if cached: return cached
    try:
        r = await client.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true", timeout=8)
        r.raise_for_status()
        d = r.json()
        result = {"bitcoin":{"price":d.get("bitcoin",{}).get("usd",0),"change_24h":round(d.get("bitcoin",{}).get("usd_24h_change",0),2)},
                  "ethereum":{"price":d.get("ethereum",{}).get("usd",0),"change_24h":round(d.get("ethereum",{}).get("usd_24h_change",0),2)}}
        return _cache_set("crypto", result)
    except Exception as e: logger.warning("CoinGecko: %r", e); return {}

def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _raise_alpha_vantage_error(data: dict[str, Any], label: str) -> None:
    for key in ("Error Message", "Information", "Note"):
        message = data.get(key)
        if message:
            raise RuntimeError(f"{label}: {message}")


def _has_price_snapshot(snapshot: Any) -> bool:
    if not isinstance(snapshot, dict):
        return False
    price = _to_float(snapshot.get("price"))
    return price is not None and price > 0


def _has_extended_market_data(bucket: dict[str, Any] | None) -> bool:
    if not isinstance(bucket, dict):
        return False
    return any(_has_price_snapshot(bucket.get(key)) for key in ("wti", "brent", "nat_gas", "gold", "spy"))


def _series_snapshot(rows: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    values = [_to_float(item.get("value")) for item in rows]
    valid = [value for value in values if value is not None]
    latest = valid[0] if valid else None
    previous = valid[1] if len(valid) > 1 else None
    return latest, previous


def _timeseries_snapshot(series: dict[str, dict[str, Any]], close_key: str) -> tuple[float | None, float | None]:
    valid = []
    for day in sorted(series.keys(), reverse=True):
        value = _to_float((series.get(day) or {}).get(close_key))
        if value is not None:
            valid.append(value)
        if len(valid) >= 2:
            break
    latest = valid[0] if valid else None
    previous = valid[1] if len(valid) > 1 else None
    return latest, previous


def _price_change(latest: float | None, previous: float | None) -> dict[str, float] | None:
    if latest is None or latest <= 0:
        return None
    change = round((latest - previous) / previous * 100, 2) if previous not in (None, 0) else 0.0
    return {"price": round(latest, 2), "change_24h": change}


async def _alpha_query(client: httpx.AsyncClient, label: str, **params) -> dict[str, Any]:
    r = await client.get("https://www.alphavantage.co/query", params=params, timeout=12)
    r.raise_for_status()
    data = r.json()
    _raise_alpha_vantage_error(data, label)
    return data


async def fetch_yahoo_markets(client):
    cached = _cache_get("yahoo_markets", ttl=YAHOO_MARKETS_CACHE_TTL)
    if cached:
        return cached
    stale = _cache_get("yahoo_markets", ttl=YAHOO_MARKETS_CACHE_TTL, allow_stale=True, stale_ttl=CACHE_RETENTION_SECONDS) or {}

    result = {}
    symbol_map = {
        "wti": "CL=F",
        "brent": "BZ=F",
        "nat_gas": "NG=F",
        "gold": "GC=F",
        "spy": "SPY",
    }
    for key, symbol in symbol_map.items():
        try:
            r = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"interval": "1d", "range": "5d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=12,
            )
            r.raise_for_status()
            payload = r.json()
            chart = (payload.get("chart") or {}).get("result") or []
            if not chart:
                raise RuntimeError(f"missing chart result for {symbol}")
            node = chart[0]
            meta = node.get("meta") or {}
            quotes = ((node.get("indicators") or {}).get("quote") or [{}])[0]
            closes = [_to_float(value) for value in (quotes.get("close") or [])]
            closes = [value for value in closes if value is not None]
            latest = _to_float(meta.get("regularMarketPrice")) or (closes[-1] if closes else None)
            previous = _to_float(meta.get("chartPreviousClose"))
            if previous is None and len(closes) >= 2:
                previous = closes[-2]
            snapshot = _price_change(latest, previous)
            if snapshot:
                result[key] = snapshot
        except Exception as e:
            logger.warning("Yahoo(%s): %r", key, e)

    merged = {**stale, **result}
    if merged:
        return _cache_set("yahoo_markets", merged)
    return {}


async def fetch_alpha_markets(client):
    cached = _cache_get("alpha_markets", ttl=COMMODITIES_CACHE_TTL)
    if cached:
        return cached
    stale = _cache_get("alpha_markets", ttl=COMMODITIES_CACHE_TTL, allow_stale=True, stale_ttl=CACHE_RETENTION_SECONDS) or {}
    if not ALPHA_VANTAGE_KEY:
        return stale

    result = {}
    for key, fn in [("wti", "WTI"), ("brent", "BRENT"), ("nat_gas", "NATURAL_GAS")]:
        try:
            data = await _alpha_query(client, f"AV({fn})", function=fn, interval="daily", apikey=ALPHA_VANTAGE_KEY)
            latest, previous = _series_snapshot(data.get("data") or [])
            snapshot = _price_change(latest, previous)
            if snapshot:
                result[key] = snapshot
        except Exception as e:
            logger.warning("AV(%s): %r", key, e)

    try:
        data = await _alpha_query(
            client,
            "AV(GOLD_SILVER_HISTORY)",
            function="GOLD_SILVER_HISTORY",
            symbol="GOLD",
            interval="daily",
            apikey=ALPHA_VANTAGE_KEY,
        )
        latest, previous = _series_snapshot(data.get("data") or [])
        snapshot = _price_change(latest, previous)
        if snapshot:
            result["gold"] = snapshot
    except Exception as e:
        logger.warning("AV(gold): %r", e)

    try:
        data = await _alpha_query(
            client,
            "AV(TIME_SERIES_DAILY:SPY)",
            function="TIME_SERIES_DAILY",
            symbol="SPY",
            outputsize="compact",
            apikey=ALPHA_VANTAGE_KEY,
        )
        latest, previous = _timeseries_snapshot(data.get("Time Series (Daily)") or {}, "4. close")
        snapshot = _price_change(latest, previous)
        if snapshot:
            result["spy"] = snapshot
    except Exception as e:
        logger.warning("AV(spy): %r", e)

    merged = {**stale, **result}
    if merged:
        return _cache_set("alpha_markets", merged)
    return {}


async def fetch_fx_rates(client):
    cached = _cache_get("forex_rates", ttl=FOREX_CACHE_TTL)
    if cached:
        return cached
    stale = _cache_get("forex_rates", ttl=FOREX_CACHE_TTL, allow_stale=True, stale_ttl=CACHE_RETENTION_SECONDS) or {}
    result = {}

    if OXR_APP_ID:
        try:
            r = await client.get(
                "https://openexchangerates.org/api/latest.json",
                params={"app_id": OXR_APP_ID, "symbols": "INR,EUR,GBP,JPY"},
                timeout=10,
            )
            r.raise_for_status()
            rates = r.json().get("rates", {})
            result.update({"usd_inr": rates.get("INR", 0), "usd_eur": rates.get("EUR", 0), "usd_gbp": rates.get("GBP", 0), "usd_jpy": rates.get("JPY", 0)})
        except Exception as e:
            logger.warning("OpenExchangeRates: %r", e)

    if not result:
        try:
            r = await client.get(
                "https://api.frankfurter.dev/v1/latest",
                params={"base": "USD", "symbols": "INR,EUR,GBP,JPY"},
                timeout=10,
            )
            r.raise_for_status()
            rates = r.json().get("rates", {})
            result.update({"usd_inr": rates.get("INR", 0), "usd_eur": rates.get("EUR", 0), "usd_gbp": rates.get("GBP", 0), "usd_jpy": rates.get("JPY", 0)})
        except Exception as e:
            logger.warning("Frankfurter: %r", e)

    merged = {**stale, **{k: v for k, v in result.items() if _to_float(v)}}
    if merged:
        return _cache_set("forex_rates", merged)
    return {}


async def fetch_commodities(client):
    cached = _cache_get("commodities", ttl=MARKETS_CACHE_TTL)
    if cached and _has_extended_market_data(cached):
        return cached
    if cached:
        logger.info("commodities cache missing extended market data, forcing refresh")

    yahoo_markets, alpha_markets, forex_rates = await asyncio.gather(
        fetch_yahoo_markets(client),
        fetch_alpha_markets(client),
        fetch_fx_rates(client),
        return_exceptions=True,
    )

    result = {}
    if isinstance(alpha_markets, dict):
        result.update(alpha_markets)
    elif isinstance(alpha_markets, Exception):
        logger.warning("alpha_markets error: %r", alpha_markets)
    if isinstance(yahoo_markets, dict):
        result.update(yahoo_markets)
    elif isinstance(yahoo_markets, Exception):
        logger.warning("yahoo_markets error: %r", yahoo_markets)
    if isinstance(forex_rates, dict):
        result.update(forex_rates)
    elif isinstance(forex_rates, Exception):
        logger.warning("forex_rates error: %r", forex_rates)
    if result:
        return _cache_set("commodities", result)

    stale = _cache_get("commodities", ttl=MARKETS_CACHE_TTL, allow_stale=True, stale_ttl=CACHE_RETENTION_SECONDS)
    if stale:
        return stale

    return {}


async def _fetch_markets_payload_uncached() -> dict:
    async with httpx.AsyncClient(headers={"User-Agent": "ConflictDashboard/3.0"}) as client:
        crypto, commodities = await asyncio.gather(fetch_crypto(client), fetch_commodities(client), return_exceptions=True)
    c = crypto if isinstance(crypto, dict) else {}
    cm = commodities if isinstance(commodities, dict) else {}
    stocks = {k: cm.pop(k) for k in ["spy"] if k in cm}
    payload = {"status":"ok","fetched_at":datetime.now(timezone.utc).isoformat(),"crypto":c,"commodities":cm,"stocks":stocks}
    logger.info("markets rebuilt crypto=%d commodities=%d stocks=%d", len(c), len(cm), len(stocks))
    return payload


async def _build_markets_payload() -> dict:
    cached = _cache_get("markets_payload", ttl=MARKETS_CACHE_TTL)
    if cached and (_has_extended_market_data(cached.get("commodities")) or _has_extended_market_data(cached.get("stocks"))):
        return cached
    if cached:
        logger.info("markets payload cache missing extended market data, forcing refresh")
    return await _cached_json(
        "markets_payload",
        ttl=0 if cached else MARKETS_CACHE_TTL,
        builder=_fetch_markets_payload_uncached,
    )

# ═══════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════

def _root_status_payload() -> dict[str, Any]:
    return {
        "status": "Global Conflict Intelligence API v3",
        "frontend": "/index.html",
        "endpoints": [
            "/api/news",
            "/api/markets",
            "/api/flights",
            "/api/ships",
            "/api/rigs",
            "/api/shortage",
            "/api/health",
            "/api/status",
            "/ws",
        ],
    }


@app.get("/")
async def root(request: Request):
    accept = (request.headers.get("accept") or "").lower()
    wants_html = "text/html" in accept or "application/xhtml+xml" in accept
    if os.getenv("VERCEL") and wants_html:
        return RedirectResponse("/index.html", status_code=307)
    return _root_status_payload()


@app.get("/api/status")
async def api_status():
    return _root_status_payload()

@app.get("/api/news")
async def get_news(limit: int = Query(default=NEWS_BASE_LIMIT, le=150)):
    return await _build_news_payload(limit)

@app.get("/api/markets")
async def get_markets():
    return await _build_markets_payload()

@app.get("/api/flights")
async def get_flights():
    """Live aircraft over conflict zones via OpenSky Network (free)."""
    return await fetch_all_flights()

@app.get("/api/ships")
async def get_ships(scenario: str = Query(default="base")):
    """Oil tankers, LNG carriers, and shadow fleet vessels."""
    news = await _get_news_context()
    attack_count = news.get("attack_count", 3)
    tankers = [enrich_tanker(t) for t in KNOWN_TANKERS]
    diverted = [t for t in tankers if t["status"] == "DIVERTED"]
    shadow   = [t for t in tankers if t["is_shadow"]]
    return {
        "status": "ok",
        "count": len(tankers),
        "diverted_count": len(diverted),
        "shadow_fleet_count": len(shadow),
        "hormuz_transits": len([t for t in tankers if "Hormuz" in t["route"]]),
        "redsea_risk": len([t for t in tankers if "Red Sea" in t["route"] or "Suez" in t["route"]]),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "tankers": tankers,
    }

@app.get("/api/rigs")
async def get_rigs():
    """Oil rigs and offshore platforms with dynamic threat scoring."""
    news = await _get_news_context()
    attack_count = news.get("attack_count", 0)
    rigs = []
    for r in OIL_RIGS_DATA:
        enriched = {**r, "flag_emoji": FLAG_EMOJI.get(r.get("country",""), "🏳"),
                    "threat": score_rig_threat(r, attack_count)}
        rigs.append(enriched)
    high = sum(1 for r in rigs if r["threat"]=="HIGH")
    return {"status":"ok","count":len(rigs),"high_threat_count":high,
            "fetched_at":datetime.now(timezone.utc).isoformat(),"rigs":rigs}

@app.get("/api/shortage")
async def get_shortage(scenario: str = Query(default="base")):
    """Per-country oil shortage impact model under different Hormuz/Red Sea scenarios."""
    if scenario not in PRICE_SCENARIOS:
        scenario = "base"
    news = await _get_news_context()
    attack_count = news.get("attack_count", 3)
    sc = PRICE_SCENARIOS[scenario]
    impacts = []
    for c in SHORTAGE_COUNTRIES:
        imp = compute_impact(c, scenario, attack_count)
        impacts.append({**c, "flag_emoji": FLAG_EMOJI.get(c["cc"],"🏳"), **imp})
    impacts.sort(key=lambda x: -x["score"])
    return {
        "status": "ok",
        "scenario": scenario,
        "scenario_label": sc["label"],
        "scenario_desc": sc["desc"],
        "wti_forecast_low": sc["wti"][0],
        "wti_forecast_high": sc["wti"][1],
        "brent_forecast_low": sc["brent"][0],
        "brent_forecast_high": sc["brent"][1],
        "attack_count_live": attack_count,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "countries": impacts,
    }

@app.get("/api/health")
async def get_health():
    return {"status":"ok","api_keys":{"news_api":bool(NEWS_API_KEY),"gnews":bool(GNEWS_API_KEY),
            "alpha_vantage":bool(ALPHA_VANTAGE_KEY),"oxr":bool(OXR_APP_ID),"opensky_auth":bool(OPENSKY_USER),
            "twitter_bearer":bool(TWITTER_BEARER),"reddit":"no_key_required"},
            "cache":{"backend":"sqlite+memory","db_path":str(CACHE_DB_PATH.name),
            "news_ttl":NEWS_CACHE_TTL,"markets_ttl":MARKETS_CACHE_TTL,"commodities_ttl":COMMODITIES_CACHE_TTL,
            "forex_ttl":FOREX_CACHE_TTL,"flights_ttl":FLIGHTS_CACHE_TTL},
            "checked_at":datetime.now(timezone.utc).isoformat()}

# ═══════════════════════════════════════════════
# WEBSOCKET — also pushes flights/ships
# ═══════════════════════════════════════════════
async def _ws_loop(ws: WebSocket):
    await manager.connect(ws)
    logger.info("WS connected total=%d", len(manager.active))
    try:
        news = await _build_news_payload(NEWS_BASE_LIMIT)
        if not await _ws_send_json(ws, "news", news):
            return
        if not await _ws_send_json(ws, "markets", await _build_markets_payload()):
            return
        flights = await fetch_all_flights()
        if not await _ws_send_json(ws, "flights", flights):
            return
        ships = await get_ships()
        if not await _ws_send_json(ws, "ships", ships):
            return
    except Exception as e:
        logger.error("WS init: %r", e)
        return

    last_news = last_market = last_flights = last_ships = time.time()
    try:
        while True:
            now = time.time()
            try:
                msg = await asyncio.wait_for(ws.receive_json(), timeout=0.1)
                if msg.get("action") == "refresh":
                    logger.info("WS refresh requested")
                    news = await _build_news_payload(NEWS_BASE_LIMIT)
                    if not await _ws_send_json(ws, "news", news):
                        break
                    flights = await fetch_all_flights()
                    if not await _ws_send_json(ws, "flights", flights):
                        break
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                logger.info("WS client disconnected during receive")
                break
            if now - last_market >= 30:
                try:
                    if not await _ws_send_json(ws, "markets", await _build_markets_payload()):
                        break
                    last_market = now
                except Exception as e:
                    logger.warning("WS market: %r", e)
            if now - last_news >= 120:
                try:
                    news = await _build_news_payload(NEWS_BASE_LIMIT)
                    if not await _ws_send_json(ws, "news", news):
                        break
                    last_news = now
                except Exception as e:
                    logger.warning("WS news: %r", e)
            if now - last_flights >= 60:
                try:
                    flights = await fetch_all_flights()
                    if not await _ws_send_json(ws, "flights", flights):
                        break
                    last_flights = now
                except Exception as e:
                    logger.warning("WS flights: %r", e)
            if now - last_ships >= 120:
                try:
                    ships = await get_ships()
                    if not await _ws_send_json(ws, "ships", ships):
                        break
                    last_ships = now
                except Exception as e:
                    logger.warning("WS ships: %r", e)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.info("WS client disconnected")
    finally:
        manager.disconnect(ws)
        logger.info("WS disconnected total=%d", len(manager.active))

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket): await _ws_loop(ws)
@app.websocket("/ws/monitor")
async def ws_monitor(ws: WebSocket): await _ws_loop(ws)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
