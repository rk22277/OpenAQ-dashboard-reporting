import argparse, os, sys, yaml, requests
from datetime import datetime, timedelta, timezone
import psycopg2, psycopg2.extras

API = "https://api.openaq.org/v3"
API_KEY = os.getenv("OPENAQ_API_KEY")
HDRS = {"X-API-Key": API_KEY} if API_KEY else {}

PG = dict(
    host=os.getenv("PG_HOST", "postgres"),
    port=int(os.getenv("PG_PORT", "5432")),
    user=os.getenv("PG_USER", "air"),
    password=os.getenv("PG_PASSWORD", "air"),
    dbname=os.getenv("PG_DB", "air_quality"),
)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS raw.openaq_v3_calls (
  called_at timestamptz NOT NULL,
  endpoint  text        NOT NULL,
  params    jsonb       NOT NULL,
  payload   jsonb       NOT NULL
);

CREATE TABLE IF NOT EXISTS core.measurements_hourly (
  requested_city text         NOT NULL,
  observed_utc   timestamptz  NOT NULL,
  country_code   text,
  locality       text,
  locations_id   int,
  sensors_id     int,
  parameter_name text,
  value          double precision,
  units          text,
  pulled_at      timestamptz  NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_city_time
  ON core.measurements_hourly (requested_city, observed_utc);
-- Prevent duplicate rows when the ingestion job is re-run for an overlapping
-- window; without this a re-run would double-count hours and skew dbt averages.
CREATE UNIQUE INDEX IF NOT EXISTS uq_measurement_hour
  ON core.measurements_hourly (requested_city, sensors_id, parameter_name, observed_utc);

-- NEW: metadata per requested_city for mapping (country + lat/lon)
CREATE TABLE IF NOT EXISTS core.city_metadata (
  requested_city text PRIMARY KEY,
  country_code   text,
  latitude       double precision,
  longitude      double precision
);
"""

def conn():
    return psycopg2.connect(**PG)

def api_get(path, params=None):
    r = requests.get(f"{API}{path}", params=params, headers=HDRS, timeout=30)
    r.raise_for_status()
    return r.json()

def save_raw(cur, endpoint, params, payload):
    cur.execute(
        """
        INSERT INTO raw.openaq_v3_calls(called_at, endpoint, params, payload)
        VALUES (%s, %s, %s, %s)
        """,
        (
            datetime.now(timezone.utc),
            endpoint,
            psycopg2.extras.Json(params),
            psycopg2.extras.Json(payload),
        ),
    )

def iso(dt):
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def is_recent(iso_str, days=14):
    """Return True if iso_str (UTC ISO date) is within the last `days` days."""
    if not iso_str:
        return False
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt >= (datetime.now(timezone.utc) - timedelta(days=days))
    except Exception:
        return False

def extract_country_code(country_field):
    """
    OpenAQ v3 /locations usually returns:
      "country": "IN"
    but be defensive if it ever becomes an object.
    """
    if isinstance(country_field, dict):
        return country_field.get("code") or country_field.get("id")
    return country_field

def upsert_city_metadata(cur, requested_city, loc_meta):
    """
    Store per-city metadata for mapping (country + lat/lon).
    """
    country_code = extract_country_code(loc_meta.get("country"))
    coords = loc_meta.get("coordinates") or {}
    lat = coords.get("latitude")
    lon = coords.get("longitude")

    cur.execute(
        """
        INSERT INTO core.city_metadata (requested_city, country_code, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (requested_city)
        DO UPDATE SET
          country_code = EXCLUDED.country_code,
          latitude     = EXCLUDED.latitude,
          longitude    = EXCLUDED.longitude;
        """,
        (requested_city, country_code, lat, lon),
    )

def pick_recent_pm25_location(lat, lon, radii, min_recency_days=14):
    """
    Return (location_dict, pm25_sensor_id, is_recent) or (None, None, False)
    """
    # The API rejects radius > 25000, so clamp and drop duplicates while keeping order.
    radii = list(dict.fromkeys(min(int(r), 25000) for r in radii))

    def dt_last_utc(obj):
        d = obj.get("datetimeLast")
        if isinstance(d, dict):
            return d.get("utc")
        return d  # string or None

    fallback = None  # (loc, sensor_id) for the freshest non-recent sensor seen

    for radius in radii:
        params = {
            "coordinates": f"{lat},{lon}",
            "radius": radius,      # must be <= 25000
            "parameters_id": 2,    # PM2.5
            "limit": 50,
            "page": 1,
        }
        resp = api_get("/locations", params)
        results = resp.get("results", [])

        # sort locations by latest activity (most recent first)
        results.sort(key=lambda loc: dt_last_utc(loc) or "", reverse=True)
        if not results:
            continue

        for loc in results:
            loc_id = loc.get("id")
            sensors_data = api_get(f"/locations/{loc_id}/sensors", {"limit": 100})
            sensors = sensors_data.get("results", [])
            pm25_sensors = [
                s for s in sensors
                if (s.get("parameter") or {}).get("id") == 2
            ]
            if not pm25_sensors:
                # no PM2.5 at this location
                continue

            for s in pm25_sensors:
                dl = dt_last_utc(s)
                if is_recent(dl, min_recency_days):
                    print(f"Found recent PM2.5 sensor {s['id']} (last={dl}) for location {loc_id}")
                    return loc, s["id"], True
                print(f"⚠️ Sensor {s['id']} at location {loc_id} is stale (last={dl})")

            # Remember the freshest stale sensor, but keep scanning other
            # locations/radii for one with recent data before giving up.
            if fallback is None:
                fallback = (loc, pm25_sensors[0]["id"])

    if fallback is not None:
        loc, sensor_id = fallback
        print(
            f"⚠️ No recent PM2.5 sensor within provided radii; "
            f"falling back to sensor {sensor_id} at location {loc.get('id')}."
        )
        return loc, sensor_id, False

    print("No PM2.5 sensor found within provided radii.")
    return None, None, False

def fetch_hours(sensor_id, date_from_iso, cur, requested_city, loc_meta, pulled_at):
    page, total = 1, 0
    while True:
        params = {"datetime_from": date_from_iso, "limit": 1000, "page": page}
        r = api_get(f"/sensors/{sensor_id}/hours", params)
        rows = r.get("results", [])

        # raw log
        save_raw(cur, f"/sensors/{sensor_id}/hours", params, r)
        if not rows:
            break

        country_code = extract_country_code(loc_meta.get("country"))
        locality = (loc_meta.get("locality") or loc_meta.get("name"))
        locations_id = loc_meta.get("id")

        for h in rows:
            period = h.get("period", {}) or {}
            utc = (
                (period.get("datetimeTo")   or {}).get("utc")
                or (period.get("datetimeFrom") or {}).get("utc")
            )
            if not utc:
                # observed_utc is NOT NULL; a row without a timestamp is unusable.
                continue
            param = h.get("parameter", {}) or {}

            cur.execute(
                """
                INSERT INTO core.measurements_hourly(
                  requested_city, observed_utc, country_code, locality,
                  locations_id, sensors_id, parameter_name, value, units, pulled_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (requested_city, sensors_id, parameter_name, observed_utc)
                DO NOTHING
                """,
                (
                    requested_city,
                    utc,
                    country_code,
                    locality,
                    locations_id,
                    sensor_id,
                    param.get("name"),
                    h.get("value"),
                    param.get("units"),
                    pulled_at,
                ),
            )
            total += cur.rowcount

        if len(rows) < 1000:
            break
        page += 1

    return total

def main(hours=48):
    if not API_KEY:
        print("ERROR: OPENAQ_API_KEY not set (v3 requires a key).", file=sys.stderr)
        sys.exit(1)

    with open(os.path.join(os.path.dirname(__file__), "city_list.yaml")) as f:
        cfg = yaml.safe_load(f) or {}
    cities = cfg.get("cities", {})
    radii = cfg.get("radii_m", [25000, 50000, 100000])

    date_from = datetime.now(timezone.utc) - timedelta(hours=hours)
    date_from_iso = iso(date_from)
    pulled_at = datetime.now(timezone.utc)

    with conn() as c, c.cursor() as cur:
        # ensure schemas/tables exist (including city_metadata)
        cur.execute(DDL)

        max_radius = max(radii) if radii else 0
        for city, geo in cities.items():
            lat, lon = geo["lat"], geo["lon"]
            loc, sensor_id, recent = pick_recent_pm25_location(lat, lon, radii)
            if not sensor_id:
                print(f"[{city}] No PM2.5 sensor found within {max_radius} m — skipping.")
                continue

            # store / update metadata for this city (country + lat/lon)
            upsert_city_metadata(cur, city, loc)

            print(
                f"[{city}] Using location {loc.get('id')} "
                f"({loc.get('locality') or loc.get('name')}); recent={recent}"
            )
            inserted = fetch_hours(sensor_id, date_from_iso, cur, city, loc, pulled_at)
            print(f"[{city}] Inserted {inserted} hourly rows since {date_from_iso}")

            # Commit per city so a later failure doesn't discard earlier cities' data.
            c.commit()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest recent OpenAQ v3 PM2.5 hours.")
    ap.add_argument(
        "--hours",
        type=int,
        default=int(os.getenv("HOURS", "48")),
        help="Look-back window in hours (default: $HOURS or 48).",
    )
    args = ap.parse_args()
    main(args.hours)
