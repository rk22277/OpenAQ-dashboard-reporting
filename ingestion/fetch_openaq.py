import os, sys, yaml, requests
from datetime import datetime, timedelta, timezone
import psycopg2, psycopg2.extras

API = "https://api.openaq.org/v3"
API_KEY = os.getenv("OPENAQ_API_KEY")
HDRS = {"X-API-Key": API_KEY} if API_KEY else {}

PG = dict(
    host=os.getenv("PG_HOST","postgres"), port=int(os.getenv("PG_PORT","5432")),
    user=os.getenv("PG_USER","air"), password=os.getenv("PG_PASSWORD","air"),
    dbname=os.getenv("PG_DB","air_quality")
)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS raw.openaq_v3_calls (
  called_at timestamptz NOT NULL,
  endpoint text NOT NULL,
  params jsonb NOT NULL,
  payload jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS core.measurements_hourly (
  requested_city text NOT NULL,
  observed_utc timestamptz NOT NULL,
  country_code text,
  locality text,
  locations_id int,
  sensors_id int,
  parameter_name text,
  value double precision,
  units text,
  pulled_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_city_time ON core.measurements_hourly (requested_city, observed_utc);
"""

def conn(): return psycopg2.connect(**PG)

def api_get(path, params=None):
    r = requests.get(f"{API}{path}", params=params, headers=HDRS, timeout=30)
    r.raise_for_status()
    return r.json()

def save_raw(cur, endpoint, params, payload):
    cur.execute(
        "INSERT INTO raw.openaq_v3_calls(called_at,endpoint,params,payload) VALUES (%s,%s,%s,%s)",
        (datetime.now(timezone.utc), endpoint, psycopg2.extras.Json(params), psycopg2.extras.Json(payload))
    )

def iso(dt): return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")

def is_recent(iso_str, days=14):
    """Return True if iso_str (UTC ISO date) is within the last `days` days."""
    if not iso_str:
        return False
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt >= (datetime.now(timezone.utc) - timedelta(days=days))
    except Exception:
        return False


def pick_recent_pm25_location(lat, lon, radii, min_recency_days=14):
    """
    Return (location, any_pm25_sensor_id, is_recent) or (None, None, False)
    """
    for radius in radii:
        # order by most recently updated locations
        params = {
            "coordinates": f"{lat},{lon}",
            "radius": radius,              # must be <= 25000
            "parameters_id": 2,
            "limit": 50, "page": 1
        }
        resp = api_get("/locations", params); 
        # data = resp.json()
        results = resp.get("results", [])

        # sort by recency yourself
        def last_utc(loc):
            d = loc.get("datetimeLast")
            if not d: return ""
            # handle both object {"utc": "..."} and raw string (defensive)
            if isinstance(d, dict): return d.get("utc", "")
            return d
        results.sort(key=lambda l: last_utc(l), reverse=True)

        
        if not results: continue
        # try each location until we find a pm25 sensor
        for loc in results:
            loc_id = loc.get("id")
            sensors_data = api_get(f"/locations/{loc_id}/sensors", {"limit":100})  # returns dict
            sensors=sensors_data.get("results",[])
            pm25_sensors = [s for s in sensors if (s.get("parameter") or {}).get("id") == 2]
            if not pm25_sensors: continue
            # no pm25 at this location
            pm25 = pm25_sensors[0]
            sensor_id = pm25["id"]
            recent_cutoff = datetime.now(timezone.utc) - timedelta(days=min_recency_days)

            sensors = api_get(f"/locations/{loc_id}/sensors", {"limit": 100}).get("results", [])
            pm25_sensors = [s for s in sensors if (s.get("parameter") or {}).get("id") == 2]
            if not pm25_sensors: continue
            # recent?
            for s in pm25_sensors:
                dl = (s.get("datetimeLast") or {}).get("utc")
                if is_recent(dl, min_recency_days):
                    print(f"✅ Found recent PM2.5 sensor {s['id']} (last={dl}) for location {loc_id}")
                    return loc, s["id"], True
                else:
                    print(f"⚠️ Sensor {s['id']} at location {loc_id} is stale (last={dl})")

    # no recent sensor found in any radius
    print("❌ No recent PM2.5 sensor found within provided radii.")
    return None, None, False


def fetch_hours(sensor_id, date_from_iso, cur, requested_city, loc_meta, pulled_at):
    page, total = 1, 0
    while True:
        params = {"datetime_from": date_from_iso, "limit": 1000, "page": 1}
        r= api_get(f"/sensors/{sensor_id}/hours", {"datetime_from": date_from_iso, "limit": 1000, "page": 1})
        rows = r.get("results", [])
        # raw log
        save_raw(cur, f"/sensors/{sensor_id}/hours", params, r)
        if not rows: break
        for h in rows:
            period = h.get("period", {}) or {}
            utc = ((period.get("datetimeTo") or {}).get("utc")) or ((period.get("datetimeFrom") or {}).get("utc"))
            param = h.get("parameter", {}) or {}
            cur.execute("""
                INSERT INTO core.measurements_hourly(
                  requested_city, observed_utc, country_code, locality, locations_id, sensors_id,
                  parameter_name, value, units, pulled_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                requested_city, utc, loc_meta.get("country",{}).get("code"),
                (loc_meta.get("locality") or loc_meta.get("name")), loc_meta.get("id"), sensor_id,
                param.get("name"), h.get("value"), param.get("units"), pulled_at
            ))
            total += 1
        if len(rows) < 1000: break
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

    # loc, sensor_id, recent = pick_recent_pm25_location(lat, lon, radii)
    # if not sensor_id:
    #     print(f"[{city}] No recent PM2.5 sensor found — skipping.")
    #     continue

    with conn() as c, c.cursor() as cur:
        cur.execute(DDL)
        for city, geo in cities.items():
            lat, lon = geo["lat"], geo["lon"]
            loc, sensor_id, recent = pick_recent_pm25_location(lat, lon, radii)
            if not sensor_id:
                print(f"[{city}] No PM2.5 sensor found within {radii[-1]} m — skipping.")
                continue
            print(f"[{city}] Using location {loc.get('id')} ({loc.get('locality') or loc.get('name')}); recent={recent}")
            inserted = fetch_hours(sensor_id, date_from_iso, cur, city, loc, pulled_at)
            print(f"[{city}] Inserted {inserted} hourly rows since {date_from_iso}")

if __name__ == "__main__":
    import os
    main(int(os.getenv("HOURS", "48")))
