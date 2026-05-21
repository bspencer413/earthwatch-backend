from fastapi import FastAPI, Depends, HTTPException, Header, status
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta, timezone
from typing import Optional, List
import jwt
import bcrypt
import psycopg2
import psycopg2.extras
import os
import threading
import time
import re
import html as html_lib
import httpx
import urllib.request
import urllib.parse
import urllib.error
import json as json_lib
from contextlib import contextmanager

# === MW LEGACY (commented for EW) =============================================
# from google.cloud import bigquery
# from google.oauth2 import service_account
# ==============================================================================

# ── Config from environment ───────────────────────────────────────────────────
SECRET_KEY = os.environ.get("JWT_SECRET", os.environ.get("SECRET_KEY", "earthwatch-fallback-key"))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 10080
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "alerts@earthwatch.app")
GOOGLE_GEOCODING_API_KEY = os.environ.get("GOOGLE_GEOCODING_API_KEY", "")

VERSION = "0.1.12"

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable not set. "
        "In Render, link a Postgres database to this service, or set DATABASE_URL manually."
    )
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


# === MW LEGACY (commented for EW) =============================================
# COUNTRY_TO_REGION map and normalize_region_label / get_region_for_country
# helpers removed — EarthWatch uses lat/lng + radius geofences, not named regions.
# Restore from CW v0.1.6 main.py if a region-named vertical needs them.
# ==============================================================================


# === MW LEGACY (commented for EW) =============================================
# BigQuery / SSDI helpers (get_bq_client, run_bigquery, fmt_date, parse_bq_results,
# run_ssdi_query) removed — EarthWatch does not query SSDI. Restore from CW v0.1.6
# main.py if a future vertical needs SSDI access.
# ==============================================================================


# ── DB init ───────────────────────────────────────────────────────────────────

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    c = conn.cursor()

    # PostGIS auto-enable (idempotent — safe to run on every boot).
    try:
        c.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    except Exception as e:
        print("[init_db] PostGIS extension setup warning: " + str(e))

    conn.autocommit = False

    # Users (canonical, kept identical to MW/CW shape).
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Notifications (canonical shape, polymorphic via source_type / source_ref_id).
    c.execute("""CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        watchlist_id INTEGER,
        obituary_id INTEGER,
        message TEXT NOT NULL,
        sent BOOLEAN DEFAULT FALSE,
        email_sent BOOLEAN DEFAULT FALSE,
        source_type TEXT,
        source_ref_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )""")
    c.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS source_type TEXT")
    c.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS source_ref_id INTEGER")
    c.execute("CREATE INDEX IF NOT EXISTS idx_notifications_source ON notifications (source_type, source_ref_id)")

    # === EW schema ───────────────────────────────────────────────────────────
    # Places: a user-watched geofence (name + point + radius).
    #
    # Two booleans:
    #   - is_archived: legacy v0.1.0–v0.1.5 flag for "moved to My Places."
    #     v0.1.6 stops using this — kept on the row for backward-compat only.
    #   - in_my_places: v0.1.6 model. A place ALWAYS stays in Watchlist (where the
    #     cron monitors it). When a user taps "Save to My Places" the place is
    #     ALSO added to the My Places collection without leaving Watchlist.
    #     Watchlist and My Places are independent views of the same row.
    c.execute("""CREATE TABLE IF NOT EXISTS ew_places (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        lat DOUBLE PRECISION NOT NULL,
        lng DOUBLE PRECISION NOT NULL,
        radius_mi DOUBLE PRECISION NOT NULL DEFAULT 50,
        check_interval_minutes INTEGER NOT NULL DEFAULT 60,
        alert_level TEXT NOT NULL DEFAULT 'realtime',
        is_archived BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )""")
    # v0.1.6: add in_my_places column (idempotent).
    c.execute("ALTER TABLE ew_places ADD COLUMN IF NOT EXISTS in_my_places BOOLEAN NOT NULL DEFAULT FALSE")
    # One-shot migration: any existing place that was archived under the old
    # model gets is_archived flipped back to FALSE and in_my_places set TRUE.
    # That gives users a clean state — their archived items show up in My Places
    # AND in Watchlist (the new canonical behavior).
    c.execute("""UPDATE ew_places
                 SET in_my_places = TRUE, is_archived = FALSE
                 WHERE is_archived = TRUE AND in_my_places = FALSE""")
    # Generated PostGIS geometry column (auto-derived from lat/lng).
    c.execute("""DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='ew_places' AND column_name='geom'
        ) THEN
            ALTER TABLE ew_places
            ADD COLUMN geom geography(Point, 4326)
            GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography) STORED;
        END IF;
    END $$;""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_places_user ON ew_places (user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_places_geom ON ew_places USING GIST (geom)")

    # Events: a single hazard event from a source feed. geom is geography
    # (point OR polygon — NWS gives polygons, USGS gives points). UNIQUE on
    # (source, external_id) so re-fetching the same feed never duplicates rows.
    c.execute("""CREATE TABLE IF NOT EXISTS ew_events (
        id SERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        external_id TEXT NOT NULL,
        hazard_type TEXT,
        severity TEXT,
        magnitude DOUBLE PRECISION,
        title TEXT,
        description TEXT,
        url TEXT,
        occurred_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_payload JSONB,
        canonical_event_id INTEGER,
        UNIQUE (source, external_id)
    )""")
    c.execute("""DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='ew_events' AND column_name='geom'
        ) THEN
            ALTER TABLE ew_events
            ADD COLUMN geom geography(Geometry, 4326);
        END IF;
    END $$;""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_events_geom ON ew_events USING GIST (geom)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_events_occurred ON ew_events (occurred_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_events_source ON ew_events (source)")

    # Event matches: every (event, place) pair where the event geometry
    # intersected the place's radius. UNIQUE prevents double-alerting on rerun.
    c.execute("""CREATE TABLE IF NOT EXISTS ew_event_matches (
        id SERIAL PRIMARY KEY,
        event_id INTEGER NOT NULL,
        place_id INTEGER NOT NULL,
        distance_mi DOUBLE PRECISION,
        matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        alerted_at TIMESTAMP,
        UNIQUE (event_id, place_id),
        FOREIGN KEY (event_id) REFERENCES ew_events (id) ON DELETE CASCADE,
        FOREIGN KEY (place_id) REFERENCES ew_places (id) ON DELETE CASCADE
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_matches_place ON ew_event_matches (place_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_matches_event ON ew_event_matches (event_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ew_matches_alerted ON ew_event_matches (alerted_at)")

    conn.commit()
    conn.close()


@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()


# === MW LEGACY (commented for EW) =============================================
# Wikipedia / SSDI / Legacy.com helpers (fetch_wiki_data, fetch_wiki_data_smart,
# normalize_name, normalize_name_for_wiki, extract_full_death_date,
# is_deceased_from_wiki, search_legacy_oneoff, send_email_notification for obits)
# all removed. Restore from CW v0.1.6 main.py if needed.
# ==============================================================================


# === CW LEGACY (commented for EW) =============================================
# State Department advisory pipeline (parse_advisory_level, parse_country_name_from_title,
# fetch_state_advisories, upsert_advisory, fire_advisory_alerts,
# reconcile_advisory_alerts_for_ship, check_state_advisories, send_advisory_email)
# all removed. EarthWatch covers severe weather via NWS, marine via NWS+GDACS.
# Restore from CW v0.1.6 main.py if a future vertical needs travel advisories.
# ==============================================================================


# === CW LEGACY (commented for EW) =============================================
# NOAA High Seas marine forecast pipeline (MARINE_AREAS, _parse_max_wave_meters,
# _sea_state_for_wave, _fetch_marine_bulletin, _extract_issued_at,
# check_marine_forecasts) all removed. EarthWatch covers marine alerts via NWS
# CAP feed (which includes Gale/Storm/Hurricane Force/Special Marine warnings)
# and GDACS for non-US waters. Restore from CW v0.1.6 main.py if needed.
# ==============================================================================


# ── Auth helpers ──────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# ── Pydantic models ───────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class PlaceItem(BaseModel):
    name: str
    lat: float
    lng: float
    radius_mi: Optional[float] = 50.0
    alert_level: Optional[str] = "realtime"  # off | digest | realtime

class PlaceUpdate(BaseModel):
    name: Optional[str] = None
    radius_mi: Optional[float] = None
    alert_level: Optional[str] = None
    is_archived: Optional[bool] = None  # legacy — kept for backward compat
    in_my_places: Optional[bool] = None  # v0.1.6 canonical

class PlaceResponse(BaseModel):
    id: int
    name: str
    lat: float
    lng: float
    radius_mi: float
    alert_level: str
    is_archived: bool
    in_my_places: bool
    created_at: str

# === MW LEGACY (commented for EW) =============================================
# ObituarySearch / ObituaryResult / WatchlistItem / WatchlistResponse Pydantic
# models removed — EW uses PlaceItem / PlaceResponse exclusively.
# ==============================================================================


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="EarthWatch API", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        sub = payload.get("sub")
        if sub is None:
            raise HTTPException(status_code=401, detail="Invalid authentication")
        user_id: int = int(sub)
        return user_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


# ── Health / admin ────────────────────────────────────────────────────────────

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": VERSION
    }


@app.get("/admin/delete-user")
async def admin_delete_user(email: str):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = %s", (email,))
        user = c.fetchone()
        if not user:
            return {"deleted": False, "error": "User not found"}
        user_id = user[0]
        c.execute("DELETE FROM notifications WHERE user_id = %s", (user_id,))
        # Cascade deletes ew_event_matches via FK ON DELETE CASCADE.
        c.execute("DELETE FROM ew_places WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {"deleted": True, "email": email}


@app.get("/admin/stats")
async def get_stats():
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM ew_places WHERE is_archived = FALSE")
        active_places = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM ew_events")
        events = c.fetchone()[0]
        c.execute("SELECT source, COUNT(*) FROM ew_events GROUP BY source")
        by_source = {row[0]: row[1] for row in c.fetchall()}
        c.execute("SELECT COUNT(*) FROM ew_event_matches WHERE alerted_at IS NOT NULL")
        alerts_fired = c.fetchone()[0]
        return {
            "users": users,
            "active_places": active_places,
            "events_total": events,
            "events_by_source": by_source,
            "alerts_fired": alerts_fired,
            "version": VERSION,
        }


@app.get("/admin/check-now")
async def admin_check_now():
    """Manually trigger a full source pull + spatial-join + alert pass."""
    threading.Thread(target=run_check_cycle, daemon=True).start()
    return {"message": "EW check cycle started"}


@app.get("/admin/who-owns-what")
async def admin_who_owns_what():
    """Debug endpoint: show every user and how many places they own (active + archived).
    Used to verify the user_id filter is doing its job and we're not leaking data
    across accounts."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT u.id, u.email,
                   COUNT(p.id) FILTER (WHERE p.is_archived = FALSE) AS active,
                   COUNT(p.id) FILTER (WHERE p.is_archived = TRUE) AS archived,
                   COUNT(p.id) AS total
            FROM users u
            LEFT JOIN ew_places p ON p.user_id = u.id
            GROUP BY u.id, u.email
            ORDER BY u.id
        """)
        rows = []
        for r in c.fetchall():
            rows.append({
                "user_id": r[0],
                "email": r[1],
                "active": int(r[2]),
                "archived": int(r[3]),
                "total": int(r[4]),
            })
        return {"users": rows, "version": VERSION}


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/auth/register", response_model=Token)
async def register(user: UserCreate):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = %s", (user.email,))
        if c.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        password_hash = hash_password(user.password)
        c.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (user.email, password_hash))
        user_id = c.fetchone()[0]
        conn.commit()
        access_token = create_access_token(data={"sub": str(user_id)})
        return {"access_token": access_token, "token_type": "bearer"}


@app.post("/auth/login", response_model=Token)
async def login(user: UserLogin):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id, password_hash FROM users WHERE email = %s", (user.email,))
        result = c.fetchone()
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        access_token = create_access_token(data={"sub": str(result[0])})
        return {"access_token": access_token, "token_type": "bearer"}


@app.delete("/account")
async def delete_account(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM notifications WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM ew_places WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {"message": "Account permanently deleted"}


# ── EW Places endpoints ───────────────────────────────────────────────────────

@app.get("/ew/places", response_model=List[PlaceResponse])
async def list_places(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, lat, lng, radius_mi, alert_level, is_archived,
                   in_my_places, created_at
            FROM ew_places
            WHERE user_id = %s
            ORDER BY created_at DESC
        """, (user_id,))
        items = []
        for row in c.fetchall():
            items.append({
                "id": row[0],
                "name": row[1],
                "lat": float(row[2]),
                "lng": float(row[3]),
                "radius_mi": float(row[4]),
                "alert_level": row[5],
                "is_archived": bool(row[6]),
                "in_my_places": bool(row[7]),
                "created_at": str(row[8]),
            })
        return items


@app.post("/ew/places")
async def add_place(item: PlaceItem, user_id: int = Depends(get_current_user)):
    if item.lat < -90 or item.lat > 90:
        raise HTTPException(status_code=400, detail="lat must be between -90 and 90")
    if item.lng < -180 or item.lng > 180:
        raise HTTPException(status_code=400, detail="lng must be between -180 and 180")
    radius = float(item.radius_mi) if item.radius_mi is not None else 50.0
    if radius <= 0 or radius > 1000:
        raise HTTPException(status_code=400, detail="radius_mi must be between 0 and 1000")
    alert_level = item.alert_level or "realtime"
    if alert_level not in ("off", "digest", "realtime"):
        alert_level = "realtime"
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO ew_places (user_id, name, lat, lng, radius_mi, alert_level)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, created_at
        """, (user_id, item.name, item.lat, item.lng, radius, alert_level))
        row = c.fetchone()
        conn.commit()
        return {
            "message": "Place added",
            "id": row[0],
            "name": item.name,
            "lat": item.lat,
            "lng": item.lng,
            "radius_mi": radius,
            "alert_level": alert_level,
            "is_archived": False,
            "in_my_places": False,
            "created_at": str(row[1]),
        }


@app.patch("/ew/places/{place_id}")
async def update_place(place_id: int, update: PlaceUpdate, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM ew_places WHERE id = %s AND user_id = %s", (place_id, user_id))
        if not c.fetchone():
            raise HTTPException(status_code=404, detail="Place not found")
        sets = []
        params = []
        if update.name is not None:
            sets.append("name = %s")
            params.append(update.name)
        if update.radius_mi is not None:
            if update.radius_mi <= 0 or update.radius_mi > 1000:
                raise HTTPException(status_code=400, detail="radius_mi must be between 0 and 1000")
            sets.append("radius_mi = %s")
            params.append(update.radius_mi)
        if update.alert_level is not None:
            if update.alert_level not in ("off", "digest", "realtime"):
                raise HTTPException(status_code=400, detail="alert_level must be off/digest/realtime")
            sets.append("alert_level = %s")
            params.append(update.alert_level)
        if update.is_archived is not None:
            sets.append("is_archived = %s")
            params.append(bool(update.is_archived))
        if update.in_my_places is not None:
            sets.append("in_my_places = %s")
            params.append(bool(update.in_my_places))
        if not sets:
            return {"message": "No changes"}
        params.append(place_id)
        params.append(user_id)
        c.execute("UPDATE ew_places SET " + ", ".join(sets) + " WHERE id = %s AND user_id = %s", params)
        conn.commit()
        return {"message": "Place updated", "id": place_id}


@app.delete("/ew/places/{place_id}")
async def delete_place(place_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM ew_places WHERE id = %s AND user_id = %s", (place_id, user_id))
        conn.commit()
        if c.rowcount == 0:
            raise HTTPException(status_code=404, detail="Place not found")
        return {"message": "Place removed"}


@app.get("/ew/places/{place_id}/events")
async def get_place_events(place_id: int, user_id: int = Depends(get_current_user)):
    """Drawer payload — does a LIVE spatial join against the events table for this
    place's geofence, regardless of whether the place is on Watchlist or My Places.

    Canonical MW pattern: cron-driven background work writes to a tracking table
    (ew_event_matches) for de-dup of alerts; the drawer-on-open endpoint queries
    sources/events directly for the freshest answer at view-time. Mirrors MW's
    /watchlist/{id}/refresh behavior, scaled to the EW spatial-join model."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id, name, lat, lng, radius_mi, is_archived, in_my_places FROM ew_places WHERE id = %s AND user_id = %s",
                  (place_id, user_id))
        place = c.fetchone()
        if not place:
            raise HTTPException(status_code=404, detail="Place not found")

        place_lat = float(place[2])
        place_lng = float(place[3])
        place_radius = float(place[4])

        # Live spatial join: fetch matched events directly from ew_events for this
        # place, regardless of is_archived state. No dependence on ew_event_matches.
        #
        # v0.1.12: pure date sort, strict 10-day window. Watchlist semantics — the
        # user wants the LATEST activity, not source-prioritized history. The prior
        # severity/hazard_type CASE priority and the EONET-exempt long window were
        # hiding "what's happening now" behind "what's biggest." A long-running
        # Kīlauea WATCH or an EONET wildfire from a month ago no longer crowds out
        # this morning's M4 or fresh NWS warning. If HVO re-issues a notice on a
        # still-active volcano, sent_utc updates and it returns to view.
        c.execute("""
            SELECT e.id, e.source, e.external_id, e.hazard_type, e.severity, e.magnitude,
                   e.title, e.description, e.url, e.occurred_at,
                   ST_Distance(e.geom, p.geom) / 1609.344 AS distance_mi,
                   ST_Y(ST_Centroid(e.geom)::geometry) AS ev_lat, ST_X(ST_Centroid(e.geom)::geometry) AS ev_lng
            FROM ew_events e, ew_places p
            WHERE p.id = %s
              AND ST_DWithin(e.geom, p.geom, p.radius_mi * 1609.344)
              AND e.occurred_at > NOW() - INTERVAL '10 days'
            ORDER BY e.occurred_at DESC
            LIMIT 50
        """, (place_id,))

        events = []
        ev_pins = []
        for row in c.fetchall():
            ev_lat = float(row[11]) if row[11] is not None else None
            ev_lng = float(row[12]) if row[12] is not None else None
            events.append({
                "id": row[0],
                "source": row[1],
                "external_id": row[2],
                "hazard_type": row[3],
                "severity": row[4],
                "magnitude": float(row[5]) if row[5] is not None else None,
                "title": row[6],
                "description": row[7],
                "url": row[8],
                "occurred_at": str(row[9]) if row[9] else None,
                "distance_mi": round(float(row[10]), 1) if row[10] is not None else None,
                "lat": ev_lat,
                "lng": ev_lng,
            })
            if ev_lat is not None and ev_lng is not None:
                # (lat, lng, severity, hazard_type) -- hazard_type drives the icon
                # on the static map; severity is the color fallback if no icon match.
                ev_pins.append((ev_lat, ev_lng, row[4], row[3]))

        static_map_url = build_static_map_url(place_lat, place_lng, place_radius, ev_pins)

        return {
            "place": {
                "id": place[0],
                "name": place[1],
                "lat": place_lat,
                "lng": place_lng,
                "radius_mi": place_radius,
                "is_archived": bool(place[5]),
                "in_my_places": bool(place[6]),
            },
            "events": events,
            "count": len(events),
            "static_map_url": static_map_url,
        }


def build_static_map_url(lat: float, lng: float, radius_mi: float, ev_pins: list) -> Optional[str]:
    """Return a Google Static Maps URL showing the place + radius circle + event pins.
    If GOOGLE_GEOCODING_API_KEY is unset, returns None (frontend just hides the map).
    Zoom is computed from the radius so the circle fills most of the frame; we don't
    let Google auto-fit because event markers far outside the radius would zoom out
    too aggressively and shrink the user's place to a dot."""
    if not GOOGLE_GEOCODING_API_KEY:
        return None
    import math
    pts = []
    R_EARTH_MI = 3958.8
    for i in range(0, 37):
        angle = (i / 36.0) * 2 * math.pi
        dlat = (radius_mi / R_EARTH_MI) * math.cos(angle) * (180.0 / math.pi)
        dlng = (radius_mi / R_EARTH_MI) * math.sin(angle) * (180.0 / math.pi) / max(0.01, math.cos(math.radians(lat)))
        pts.append((lat + dlat, lng + dlng))
    path = "color:0x0d9488ff|weight:2|fillcolor:0x0d948833|" + "|".join([str(round(p[0], 5)) + "," + str(round(p[1], 5)) for p in pts])

    markers = []
    markers.append("color:0x0d9488|label:H|" + str(lat) + "," + str(lng))
    # Per-hazard Twemoji PNG icons (same emoji rendered in the drawer cards).
    # Pinned to twemoji v14.0.2 on jsdelivr for stability. If Google can't fetch
    # the icon URL, that marker drops silently -- map + Home pin still render.
    hazard_cp = {
        "volcano":          "1f30b",   # 🌋
        "wildfire":         "1f525",   # 🔥
        "earthquake":       "1f310",   # 🌐
        "hurricane":        "1f300",   # 🌀
        "tropical_cyclone": "1f300",   # 🌀
        "tornado":          "1f32a",   # 🌪
        "tsunami":          "1f30a",   # 🌊
        "flood":            "1f30a",   # 🌊
        "severe_weather":   "26c8",    # ⛈
        "winter_storm":     "2744",    # ❄
    }
    icon_base = "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/"
    sev_color = {"extreme": "0xdc2626", "severe": "0xea580c", "moderate": "0xd97706", "minor": "0xa16207"}
    for pin in ev_pins[:20]:
        # Accept both (lat, lng, sev) legacy and (lat, lng, sev, hazard_type) v0.1.11+.
        elat, elng = pin[0], pin[1]
        sev = pin[2] if len(pin) >= 3 else "minor"
        hazard_type = pin[3] if len(pin) >= 4 else None
        cp = hazard_cp.get(hazard_type or "")
        if cp:
            markers.append("icon:" + icon_base + cp + ".png|" + str(round(elat, 5)) + "," + str(round(elng, 5)))
        else:
            # No hazard icon mapping -- fall back to a severity-colored dot.
            c = sev_color.get(sev or "minor", "0xa16207")
            markers.append("color:" + c + "|" + str(round(elat, 5)) + "," + str(round(elng, 5)))

    # Zoom math: a 640px-wide map at zoom z covers ~ 156543.03 / 2^z meters per pixel
    # at the equator, scaled by cos(lat). We want the radius diameter (2*radius) to fit
    # ~85% of the 640px width, so:
    #   2 * radius_meters = 0.85 * 640 * (156543.03 / 2^z) * cos(lat)
    # solve for z. Clamp to [3, 14] so we never zoom past country-level or street-level.
    radius_m = radius_mi * 1609.344
    cos_lat = max(0.01, math.cos(math.radians(lat)))
    target_m_per_pixel = (2 * radius_m) / (0.85 * 640)
    if target_m_per_pixel <= 0:
        zoom = 9
    else:
        zoom_float = math.log2((156543.03 * cos_lat) / target_m_per_pixel)
        zoom = max(3, min(14, int(round(zoom_float))))

    params = [
        "size=640x320",
        "scale=2",
        "maptype=terrain",
        "zoom=" + str(zoom),
        "center=" + str(lat) + "," + str(lng),
        "path=" + urllib.parse.quote(path),
    ]
    for m in markers:
        params.append("markers=" + urllib.parse.quote(m))
    params.append("key=" + GOOGLE_GEOCODING_API_KEY)
    return "https://maps.googleapis.com/maps/api/staticmap?" + "&".join(params)


# ── Google Geocoding (search box on Add Place) ────────────────────────────────

class GeocodeQuery(BaseModel):
    query: str

@app.post("/ew/geocode")
async def geocode_place(q: GeocodeQuery, user_id: int = Depends(get_current_user)):
    """Forward geocode a free-text query (city, address, landmark) via Google.
    Returns up to 5 candidates so the user can pick the right match.
    Backend-side so the API key never lives in the frontend."""
    if not GOOGLE_GEOCODING_API_KEY:
        raise HTTPException(status_code=503, detail="Geocoding service not configured")
    text = (q.query or "").strip()
    if len(text) < 2:
        raise HTTPException(status_code=400, detail="Query too short")
    try:
        url = ("https://maps.googleapis.com/maps/api/geocode/json?address="
               + urllib.parse.quote(text)
               + "&key=" + GOOGLE_GEOCODING_API_KEY)
        req = urllib.request.Request(url, headers={"User-Agent": "EarthWatch/0.1"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json_lib.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        print("[geocode] " + str(e))
        raise HTTPException(status_code=502, detail="Geocoding lookup failed")
    status = data.get("status")
    if status == "ZERO_RESULTS":
        return {"candidates": []}
    if status != "OK":
        # Don't leak Google error_message to client — just say it failed.
        print("[geocode] Google returned status=" + str(status) + " for query: " + text)
        raise HTTPException(status_code=502, detail="Geocoding lookup failed")
    candidates = []
    for r in (data.get("results") or [])[:5]:
        loc = ((r.get("geometry") or {}).get("location")) or {}
        lat = loc.get("lat")
        lng = loc.get("lng")
        if lat is None or lng is None:
            continue
        candidates.append({
            "formatted_address": r.get("formatted_address", ""),
            "lat": float(lat),
            "lng": float(lng),
            "place_id": r.get("place_id", ""),
        })
    return {"candidates": candidates}


# ── Notifications (alerts feed) ───────────────────────────────────────────────

@app.get("/notifications")
async def get_notifications(user_id: int = Depends(get_current_user)):
    """Alerts feed. EW writes notifications with source_type='ew_event' and
    source_ref_id pointing to ew_event_matches.id."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT n.id, n.message, n.created_at, n.watchlist_id, n.source_type, n.source_ref_id,
                   p.name AS place_name
            FROM notifications n
            LEFT JOIN ew_places p ON n.watchlist_id = p.id
            WHERE n.user_id = %s
            ORDER BY n.created_at DESC LIMIT 100
        """, (user_id,))
        notifications = []
        for row in c.fetchall():
            notifications.append({
                "id": row[0],
                "message": row[1],
                "created_at": str(row[2]),
                "watchlist_id": row[3],
                "source_type": row[4] or "",
                "source_ref_id": row[5],
                "name": row[6] or "",
            })
        return notifications


@app.delete("/notifications/{notif_id}")
async def delete_notification(notif_id: int, user_id: int = Depends(get_current_user)):
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM notifications WHERE id = %s AND user_id = %s", (notif_id, user_id))
            conn.commit()
            return {"deleted": True}
    except Exception as e:
        print("Delete notification error: " + str(e))
        return {"deleted": False}


# ── Source adapters ───────────────────────────────────────────────────────────
# All four feeds are wrapped in a Sources class so adding a fifth (JMA, EMSC,
# whatever) is one new method, not a growing if/elif chain. Each adapter
# returns a normalized list of dicts:
#   {source, external_id, hazard_type, severity, magnitude, title, description,
#    url, occurred_at, geom_wkt, raw}
# The cron upserts these into ew_events keyed on (source, external_id).

# Module-level cache for US volcano coordinates. The HANS getElevatedVolcanoes
# feed identifies volcanoes by vnum but doesn't carry lat/lng; we fetch the full
# US volcano list once per process from volcanoesUS and cache the lookup.
# ~161 entries, rarely changes; one HTTP call at first HVO cron tick.
_VOLCANO_COORDS: dict = {}

def _ensure_volcano_coords():
    """Lazy-populate _VOLCANO_COORDS on first HVO call. Idempotent.
    Failure is non-fatal -- fetch_hvo() simply skips volcanoes it can't place."""
    global _VOLCANO_COORDS
    if _VOLCANO_COORDS:
        return
    try:
        data = Sources._http_get_json("https://volcanoes.usgs.gov/vsc/api/volcanoApi/volcanoesUS")
        if isinstance(data, list):
            for v in data:
                vnum = str(v.get("vnum") or "")
                lat = v.get("latitude")
                lng = v.get("longitude")
                if vnum and lat is not None and lng is not None:
                    _VOLCANO_COORDS[vnum] = (float(lat), float(lng), v.get("vName"))
            print("[hvo] cached " + str(len(_VOLCANO_COORDS)) + " US volcano coordinates")
    except Exception as e:
        print("[hvo] volcanoesUS cache load failed: " + str(e))


class Sources:
    """All hazard feed adapters. Each fetch_* returns a list of normalized event dicts."""

    BROWSER_UA = "EarthWatch/0.1 (https://earthwatch.app; alerts@earthwatch.app)"

    @staticmethod
    def _http_get_json(url: str, timeout: int = 20) -> Optional[dict]:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": Sources.BROWSER_UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json_lib.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as e:
            print("[Sources._http_get_json] " + url + " — " + str(e))
            return None

    # ── USGS Earthquakes ──────────────────────────────────────────────────────
    # Public GeoJSON feed. Free, no auth, refreshed every minute. We pull the
    # past-day M2.5+ feed; anything smaller isn't actionable for end users.
    @staticmethod
    def fetch_usgs() -> List[dict]:
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
        data = Sources._http_get_json(url)
        if not data or "features" not in data:
            return []
        out = []
        for feat in data.get("features", []):
            props = feat.get("properties") or {}
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates") or []
            if len(coords) < 2:
                continue
            lng, lat = coords[0], coords[1]
            depth_km = coords[2] if len(coords) > 2 else None
            mag = props.get("mag")
            place = props.get("place") or "Unknown location"
            time_ms = props.get("time")
            occurred_at = None
            if time_ms:
                try:
                    occurred_at = datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
                except Exception:
                    occurred_at = None
            ext_id = feat.get("id") or props.get("code")
            if not ext_id:
                continue
            severity = "minor"
            if mag is not None:
                if mag >= 7:
                    severity = "extreme"
                elif mag >= 6:
                    severity = "severe"
                elif mag >= 5:
                    severity = "moderate"
                elif mag >= 4:
                    severity = "minor"
                else:
                    severity = "minor"
            title = "M" + (str(round(mag, 1)) if mag is not None else "?") + " — " + place
            desc = place + (" (depth " + str(round(depth_km, 1)) + " km)" if depth_km is not None else "")
            geom_wkt = "POINT(" + str(lng) + " " + str(lat) + ")"
            out.append({
                "source": "usgs",
                "external_id": str(ext_id),
                "hazard_type": "earthquake",
                "severity": severity,
                "magnitude": mag,
                "title": title,
                "description": desc,
                "url": props.get("url"),
                "occurred_at": occurred_at,
                "geom_wkt": geom_wkt,
                "raw": feat,
            })
        return out

    # ── NWS Severe Weather (CAP alerts) ───────────────────────────────────────
    # Public GeoJSON feed at api.weather.gov/alerts/active. No auth, no key,
    # but they require a User-Agent identifying the app. CAP polygons get
    # converted to representative-point geometry (centroid via lat/lng if no
    # geometry block) so the spatial-join still works. Note: ~10% of CAP alerts
    # ship with no geometry at all (zone-based only) — we skip those for now.
    @staticmethod
    def fetch_nws() -> List[dict]:
        url = "https://api.weather.gov/alerts/active"
        data = Sources._http_get_json(url)
        if not data or "features" not in data:
            return []
        out = []
        for feat in data.get("features", []):
            props = feat.get("properties") or {}
            geom = feat.get("geometry")
            if not geom:
                continue  # zone-only alert, no point/polygon — skip for v0.1.3
            geom_type = geom.get("type")
            coords = geom.get("coordinates") or []
            geom_wkt = None
            try:
                if geom_type == "Polygon" and coords and coords[0]:
                    ring = coords[0]
                    pts = ",".join([str(p[0]) + " " + str(p[1]) for p in ring])
                    geom_wkt = "POLYGON((" + pts + "))"
                elif geom_type == "MultiPolygon" and coords:
                    polys = []
                    for poly in coords:
                        if not poly or not poly[0]:
                            continue
                        ring = poly[0]
                        pts = ",".join([str(p[0]) + " " + str(p[1]) for p in ring])
                        polys.append("((" + pts + "))")
                    if polys:
                        geom_wkt = "MULTIPOLYGON(" + ",".join(polys) + ")"
                elif geom_type == "Point" and len(coords) >= 2:
                    geom_wkt = "POINT(" + str(coords[0]) + " " + str(coords[1]) + ")"
            except Exception:
                continue
            if not geom_wkt:
                continue

            ext_id = props.get("id") or feat.get("id")
            if not ext_id:
                continue

            # Map NWS severity (Extreme|Severe|Moderate|Minor|Unknown) to ours.
            nws_sev = (props.get("severity") or "").lower()
            sev_map = {"extreme": "extreme", "severe": "severe", "moderate": "moderate", "minor": "minor"}
            severity = sev_map.get(nws_sev, "minor")

            event_name = props.get("event") or "Weather Alert"  # e.g. "Tornado Warning"
            headline = props.get("headline") or event_name
            desc = (props.get("description") or "")[:600]
            sent = props.get("sent")
            occurred_at = None
            if sent:
                try:
                    s = sent.replace("Z", "+00:00")
                    occurred_at = datetime.fromisoformat(s).astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    occurred_at = None

            # Map event name to broad hazard_type for the icon.
            ev_lower = event_name.lower()
            if "tornado" in ev_lower:
                hazard_type = "tornado"
            elif "hurricane" in ev_lower or "tropical" in ev_lower:
                hazard_type = "hurricane"
            elif "tsunami" in ev_lower:
                hazard_type = "tsunami"
            elif "fire" in ev_lower or "smoke" in ev_lower:
                hazard_type = "wildfire"
            elif "flood" in ev_lower:
                hazard_type = "flood"
            elif "winter" in ev_lower or "snow" in ev_lower or "ice" in ev_lower or "blizzard" in ev_lower:
                hazard_type = "winter_storm"
            else:
                hazard_type = "severe_weather"

            out.append({
                "source": "nws",
                "external_id": str(ext_id),
                "hazard_type": hazard_type,
                "severity": severity,
                "magnitude": None,
                "title": event_name,
                "description": headline,
                "url": props.get("@id") or props.get("id"),
                "occurred_at": occurred_at,
                "geom_wkt": geom_wkt,
                "raw": {"properties": props},  # skip the full feature to keep payload sane
            })
        return out

    # ── NASA EONET v3 (wildfires only) ────────────────────────────────────────
    # Free, no auth, no key. v0.1.11: volcanoes moved to fetch_hvo() for real-time
    # HANS data; EONET retained for wildfires >=500 acres (IRWIN US, GDACS intl).
    # Severe storms still skipped -- NWS covers US; sea/lake ice -- no icon.
    @staticmethod
    def fetch_eonet() -> List[dict]:
        url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open"
        data = Sources._http_get_json(url)
        if not data or "events" not in data:
            return []
        # EONET v3 uses string slug ids on the category objects, not integers.
        cat_map = {
            "wildfires": "wildfire",
        }
        out = []
        for ev in data.get("events", []) or []:
            ext_id = ev.get("id")
            if not ext_id:
                continue
            # Pick the first category we recognize. Skip everything else.
            hazard_type = None
            for cat in ev.get("categories", []) or []:
                cid = cat.get("id")
                if cid in cat_map:
                    hazard_type = cat_map[cid]
                    break
            if not hazard_type:
                continue
            # Geometry is a chronological array; last entry = current position.
            geoms = ev.get("geometry") or ev.get("geometries") or []
            if not geoms:
                continue
            last = geoms[-1]
            gtype = (last.get("type") or "").lower()
            coords = last.get("coordinates") or []
            lng = None
            lat = None
            if gtype == "point" and len(coords) >= 2:
                lng, lat = coords[0], coords[1]
            elif gtype == "polygon" and coords and coords[0]:
                # Use the centroid of the first ring as a representative point.
                ring = coords[0]
                if ring:
                    lng = sum(p[0] for p in ring) / float(len(ring))
                    lat = sum(p[1] for p in ring) / float(len(ring))
            if lng is None or lat is None:
                continue
            # Timestamp on the most recent geometry. ISO 8601 UTC, "Z" suffix.
            occurred_at = None
            ts = last.get("date")
            if ts:
                try:
                    occurred_at = datetime.fromisoformat(
                        ts.replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                except Exception:
                    occurred_at = None
            # EONET doesn't carry alert levels per event. Any "open" volcano or
            # >=500-acre wildfire is worth surfacing. Default moderate; can be
            # refined later using magnitudeValue (cyclone kts, fire acres).
            severity = "moderate"
            title = ev.get("title") or "Earth event"
            # Build a short description from category titles.
            desc_parts = []
            for cat in ev.get("categories", []) or []:
                ctitle = cat.get("title")
                if ctitle:
                    desc_parts.append(ctitle)
            desc = " | ".join(desc_parts) if desc_parts else None
            # Prefer the authoritative source URL (USGS HVO, AVO, InciWeb, etc.)
            # over the EONET event link. Fall back to EONET if no sources.
            url_out = None
            sources_list = ev.get("sources") or []
            if sources_list:
                url_out = sources_list[0].get("url")
            if not url_out:
                url_out = ev.get("link") or (
                    "https://eonet.gsfc.nasa.gov/api/v3/events/" + str(ext_id)
                )
            geom_wkt = "POINT(" + str(lng) + " " + str(lat) + ")"
            out.append({
                "source": "eonet",
                "external_id": str(ext_id),
                "hazard_type": hazard_type,
                "severity": severity,
                "magnitude": None,
                "title": title,
                "description": desc,
                "url": url_out,
                "occurred_at": occurred_at,
                "geom_wkt": geom_wkt,
                "raw": {"id": ext_id, "categories": ev.get("categories")},
            })
        return out

    # ── USGS HVO / HANS (real-time elevated US volcanoes) ─────────────────────
    # Hazard Notification System feed of every US volcano currently at
    # ADVISORY/WATCH/WARNING. Real-time, no curator lag, no auth, no key.
    # Covers HVO (Hawaii), AVO (Alaska), CVO (Cascades), CalVO (California),
    # YVO (Yellowstone), NMI (N. Mariana Islands). v0.1.11: replaces EONET as
    # the volcano source -- EONET timestamps can lag actual activity by weeks.
    #
    # Lat/lng is not in the elevated feed; we cache it once from volcanoesUS
    # (module-level _VOLCANO_COORDS, lazy-loaded on first call).
    @staticmethod
    def fetch_hvo() -> List[dict]:
        _ensure_volcano_coords()
        url = "https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes"
        data = Sources._http_get_json(url)
        if not isinstance(data, list):
            return []
        sev_map = {
            "WARNING":  "extreme",
            "WATCH":    "severe",
            "ADVISORY": "moderate",
        }
        out = []
        for v in data:
            vnum = str(v.get("vnum") or "")
            if not vnum:
                continue
            coords = _VOLCANO_COORDS.get(vnum)
            if not coords:
                # No lat/lng on file -- skip rather than emit an unplaceable event.
                continue
            lat, lng, _name = coords
            alert_level = (v.get("alert_level") or "").upper()
            color_code = (v.get("color_code") or "").upper()
            severity = sev_map.get(alert_level, "moderate")
            volcano_name = v.get("volcano_name") or "Volcano"
            obs_fullname = v.get("obs_fullname") or "USGS"
            title = volcano_name + " - " + (alert_level or "ELEVATED")
            desc = obs_fullname
            if color_code:
                desc = desc + " | Aviation: " + color_code
            occurred_at = None
            ts = v.get("sent_utc")
            if ts:
                try:
                    # Format: "2026-05-15 18:55:00" (naive UTC).
                    occurred_at = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    occurred_at = None
            geom_wkt = "POINT(" + str(lng) + " " + str(lat) + ")"
            out.append({
                "source": "hvo",
                "external_id": vnum,    # one row per volcano, updated in place
                "hazard_type": "volcano",
                "severity": severity,
                "magnitude": None,
                "title": title,
                "description": desc,
                "url": v.get("notice_url"),
                "occurred_at": occurred_at,
                "geom_wkt": geom_wkt,
                "raw": {"vnum": vnum, "alert_level": alert_level, "color_code": color_code},
            })
        return out

    # ── GDACS (global multi-hazard aggregator) ────────────────────────────────
    # Stub for v0.1.0. Used as international fill-in for places outside US.
    @staticmethod
    def fetch_gdacs() -> List[dict]:
        # TODO v0.1.3: pull https://www.gdacs.org/xml/rss.xml (or geo JSON
        # equivalent), parse alert level (Green/Orange/Red) → severity.
        return []

    @staticmethod
    def all_sources() -> List[str]:
        return ["usgs", "nws", "eonet", "hvo", "gdacs"]

    @staticmethod
    def fetch(source: str) -> List[dict]:
        if source == "usgs":
            return Sources.fetch_usgs()
        if source == "nws":
            return Sources.fetch_nws()
        if source == "eonet":
            return Sources.fetch_eonet()
        if source == "hvo":
            return Sources.fetch_hvo()
        if source == "gdacs":
            return Sources.fetch_gdacs()
        return []


# ── Cron: pull all sources, spatial-join against places, fire alerts ──────────

def upsert_events(conn, normalized: List[dict]) -> List[int]:
    """Upsert normalized events. Returns the list of event ids that were
    newly inserted (so we only spatial-join the new ones)."""
    if not normalized:
        return []
    new_ids = []
    c = conn.cursor()
    for ev in normalized:
        try:
            c.execute("""
                INSERT INTO ew_events (
                    source, external_id, hazard_type, severity, magnitude,
                    title, description, url, occurred_at, raw_payload, geom
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                    ST_SetSRID(ST_GeomFromText(%s), 4326)::geography
                )
                ON CONFLICT (source, external_id) DO NOTHING
                RETURNING id
            """, (
                ev["source"], ev["external_id"], ev.get("hazard_type"),
                ev.get("severity"), ev.get("magnitude"),
                ev.get("title"), ev.get("description"), ev.get("url"),
                ev.get("occurred_at"),
                json_lib.dumps(ev.get("raw") or {}, default=str),
                ev.get("geom_wkt"),
            ))
            row = c.fetchone()
            if row:
                new_ids.append(row[0])
        except Exception as e:
            print("[upsert_events] " + ev.get("source", "?") + ":" + ev.get("external_id", "?") + " — " + str(e))
            conn.rollback()
            c = conn.cursor()
    conn.commit()
    return new_ids


def spatial_join_and_alert(conn, new_event_ids: List[int]) -> int:
    """For every newly-inserted event, find every active place whose geofence
    intersects the event geometry, insert a match row, and write a notification
    row for any match that hasn't been alerted on yet. Returns alerts fired."""
    if not new_event_ids:
        return 0
    c = conn.cursor()
    # Spatial join: ST_DWithin with geography uses meters. Convert miles → meters.
    c.execute("""
        INSERT INTO ew_event_matches (event_id, place_id, distance_mi)
        SELECT e.id, p.id,
               ST_Distance(e.geom, p.geom) / 1609.344 AS distance_mi
        FROM ew_events e
        JOIN ew_places p ON ST_DWithin(e.geom, p.geom, p.radius_mi * 1609.344)
        WHERE e.id = ANY(%s)
          AND p.alert_level <> 'off'
        ON CONFLICT (event_id, place_id) DO NOTHING
    """, (new_event_ids,))
    conn.commit()

    # Fire alerts for every new match (alerted_at IS NULL).
    c.execute("""
        SELECT m.id, m.event_id, m.place_id, m.distance_mi,
               e.title, e.hazard_type, e.severity,
               p.user_id, p.name
        FROM ew_event_matches m
        JOIN ew_events e ON m.event_id = e.id
        JOIN ew_places p ON m.place_id = p.id
        WHERE m.alerted_at IS NULL
          AND m.event_id = ANY(%s)
    """, (new_event_ids,))
    rows = c.fetchall()
    fired = 0
    for row in rows:
        match_id, event_id, place_id, distance_mi, title, hazard_type, severity, user_id, place_name = row
        try:
            distance_str = (str(round(distance_mi, 1)) + " mi from ") if distance_mi is not None else ""
            message = (title or "Hazard event") + " — " + distance_str + (place_name or "your place")
            c.execute("""
                INSERT INTO notifications (user_id, watchlist_id, message, source_type, source_ref_id)
                VALUES (%s, %s, %s, 'ew_event', %s)
            """, (user_id, place_id, message, match_id))
            c.execute("UPDATE ew_event_matches SET alerted_at = CURRENT_TIMESTAMP WHERE id = %s", (match_id,))
            fired += 1
        except Exception as e:
            print("[spatial_join_and_alert] alert write failed for match " + str(match_id) + ": " + str(e))
            conn.rollback()
            c = conn.cursor()
    conn.commit()
    return fired


def run_check_cycle():
    """Pull every source once, upsert into ew_events, spatial-join new events
    against active places, write notifications for new matches."""
    print("[cron] EW check cycle starting at " + datetime.utcnow().isoformat())
    total_new = 0
    total_alerts = 0
    for source in Sources.all_sources():
        try:
            normalized = Sources.fetch(source)
            if not normalized:
                print("[cron] " + source + ": 0 events")
                continue
            with get_db() as conn:
                new_ids = upsert_events(conn, normalized)
                total_new += len(new_ids)
                fired = spatial_join_and_alert(conn, new_ids)
                total_alerts += fired
                print("[cron] " + source + ": fetched=" + str(len(normalized))
                      + " new=" + str(len(new_ids)) + " alerts=" + str(fired))
        except Exception as e:
            print("[cron] source " + source + " failed: " + str(e))
    print("[cron] EW check cycle complete. new_events=" + str(total_new) + " alerts_fired=" + str(total_alerts))


def run_scheduler():
    # v0.1.1: 12-hour cycle for free tier. Per-user check_interval enforcement
    # (premium = faster cycle) is v0.2.
    # Uses plain time.time() — no scheduler library needed (sidesteps Render
    # cached-venv issues with packages like `schedule` / `apscheduler`).
    INTERVAL_SEC = 60 * 60 * 12  # 12 hours
    last_run = 0.0
    while True:
        now = time.time()
        if now - last_run >= INTERVAL_SEC:
            try:
                run_check_cycle()
            except Exception as e:
                print("[scheduler] check cycle failed: " + str(e))
            last_run = now
        time.sleep(60)


@app.get("/admin/signup-stats")
async def admin_signup_stats(x_admin_token: str = Header(None, alias="X-Admin-Token")):
    """Read-only signup metrics for the 3Brains scoreboard.
    Requires X-Admin-Token header matching ADMIN_STATS_TOKEN env var."""
    expected = os.environ.get("ADMIN_STATS_TOKEN")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=403, detail="Forbidden")
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '24 hours'")
        signups_24h = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '7 days'")
        signups_7d = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '30 days'")
        signups_30d = c.fetchone()[0]
        c.execute("SELECT MAX(created_at) FROM users")
        latest_row = c.fetchone()
        latest = latest_row[0].isoformat() if latest_row and latest_row[0] else None
        return {
            "total_users": total_users,
            "signups_24h": signups_24h,
            "signups_7d": signups_7d,
            "signups_30d": signups_30d,
            "latest_signup_at": latest
        }


@app.on_event("startup")
async def startup_event():
    init_db()
    print("Database initialized (EarthWatch v" + VERSION + ")")
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("Background scheduler started (12-hour check cycle: usgs + nws + eonet + hvo + gdacs)")
    threading.Thread(target=run_check_cycle, daemon=True).start()
    print("Initial EW check cycle started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
