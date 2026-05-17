================================================================================
 EarthWatch backend — patch v0.1.8 → v0.1.9
 Wires up fetch_eonet() to NASA EONET v3.
 Fixes: Hilo / Kīlauea volcano events not appearing in EW.
 Bonus fix: wildfires also start working (same single bug).
 File:    main.py
 Surgical: 2 str_replace blocks. No schema changes. No frontend changes.
================================================================================


────────────────────────────────────────────────────────────────────────────────
 PATCH 1 of 2 — Version bump
────────────────────────────────────────────────────────────────────────────────

FIND (exactly, line 36):

VERSION = "0.1.8"

REPLACE WITH:

VERSION = "0.1.9"


────────────────────────────────────────────────────────────────────────────────
 PATCH 2 of 2 — Replace fetch_eonet() stub with working adapter
────────────────────────────────────────────────────────────────────────────────

FIND (exactly, the whole stub including the TODO comment, around line 972):

    # ── NASA EONET (fires, volcanoes, storms, ice) ────────────────────────────
    # Stub for v0.1.0.
    @staticmethod
    def fetch_eonet() -> List[dict]:
        # TODO v0.1.2: pull https://eonet.gsfc.nasa.gov/api/v3/events?status=open,
        # walk geometry array (last point = current), category id → hazard_type.
        return []

REPLACE WITH:

    # ── NASA EONET v3 (volcanoes + wildfires) ─────────────────────────────────
    # Free, no auth, no key. Single endpoint covers volcanoes worldwide
    # (curated from USGS HVO, AVO, Smithsonian GVP, etc.) and wildfires >=500
    # acres (IRWIN for US, GDACS for intl). Severe storms intentionally skipped
    # for now — NWS already covers the US and we don't want duplicate alerts.
    # Sea/lake ice skipped — no frontend icon mapping yet.
    @staticmethod
    def fetch_eonet() -> List[dict]:
        url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open"
        data = Sources._http_get_json(url)
        if not data or "events" not in data:
            return []
        # EONET v3 uses string slug ids on the category objects, not integers.
        cat_map = {
            "volcanoes": "volcano",
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
            # Build a short description from category titles ("Volcanoes",
            # "Wildfires"). Useful in the drawer card under the title line.
            desc_parts = []
            for cat in ev.get("categories", []) or []:
                ctitle = cat.get("title")
                if ctitle:
                    desc_parts.append(ctitle)
            desc = " · ".join(desc_parts) if desc_parts else None
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


================================================================================
 What this changes
================================================================================

  • Backend version: 0.1.8 → 0.1.9
  • fetch_eonet() now returns real volcano + wildfire events instead of []
  • all_sources(), fetch() dispatcher, cron loop, upsert_events, spatial-join,
    notifications fire-up — ALL UNCHANGED. They already routed "eonet" through;
    they were just receiving an empty list every cycle.

================================================================================
 What this does NOT change
================================================================================

  • Database schema (ew_events already has hazard_type, severity, source,
    geom_wkt — no migration needed)
  • Frontend (index.html v0.1.7 already maps 🌋 volcano and 🔥 wildfire icons)
  • GDACS stub (still returns []; can be wired up later if needed)
  • Severe storms / sea ice from EONET (deliberately skipped — NWS covers US
    storms; sea ice has no frontend icon)

================================================================================
 How to verify after deploy
================================================================================

  1. Wait for the next cron cycle (12-hour interval) — OR — manually trigger it
     by hitting the cron endpoint if one is exposed.
  2. Render logs should show:
        [cron] eonet: fetched=N
     where N is roughly 30–80 (typical EONET open-event count globally).
  3. Add a place at Hilo, HI (lat 19.7297, lng -155.0900, radius 50 mi).
     The Halemaʻumaʻu / Kīlauea EONET volcano event should appear in the drawer
     with the 🌋 icon, MODERATE severity, source "eonet", and a "More info →"
     link to USGS HVO.
  4. Spot-check: a place near any active US wildfire >=500 acres should also
     pick up 🔥 events in the same drawer.

================================================================================
