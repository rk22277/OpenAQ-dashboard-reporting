import os, sys, yaml, requests, datetime
from datetime import timezone, timedelta

API_BASE = "https://api.openaq.org/v3"
API_KEY = os.getenv("OPENAQ_API_KEY")
HEADERS = {"X-API-Key": API_KEY} if API_KEY else {}

def api_get(path, params=None):
    r = requests.get(f"{API_BASE}{path}", params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def iso_utc(dt): return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def check_city(city, lat, lon, radius_m):
    result = {
        "city": city, "pm25_locations": 0, "sample_location_id": None,
        "sample_locality": None, "last_pm25_time": None, "recommendation": "Review"
    }
    # 1) Find locations with PM2.5 near city
    locs = api_get("/locations", {
        "coordinates": f"{lat},{lon}",
        "radius": radius_m,
        "parameters_id": 2,   # pm25
        "limit": 50, "page": 1
    }).get("results", [])
    result["pm25_locations"] = len(locs)
    if not locs:
        result["recommendation"] = "No PM2.5 stations in radius"
        return result

    loc = locs[0]
    result["sample_location_id"] = loc.get("id")
    result["sample_locality"] = loc.get("locality") or loc.get("name") or loc.get("city")

    # 2) Pull sensors at that location; keep pm25
    sensors = api_get(f"/locations/{loc['id']}/sensors").get("results", [])
    pm25_sensors = [s for s in sensors if (s.get("parameter") or {}).get("id") == 2]
    if not pm25_sensors:
        result["recommendation"] = "Location has no PM2.5 sensors"
        return result

    sensor_id = pm25_sensors[0].get("id")

    # 3) Get most recent hourly value in last 30 days (limit 1)
    since = iso_utc(datetime.datetime.now(timezone.utc) - timedelta(days=30))
    hours = api_get(f"/sensors/{sensor_id}/hours", {
        # v3 uses `datetime_from` (not `date_from`); sort desc so limit=1 is the newest.
        "datetime_from": since, "sort_order": "desc", "limit": 1, "page": 1
    }).get("results", [])

    if hours:
        # Try to extract a timestamp defensively
        h = hours[0]
        observed = None
        # Common shapes: period.datetimeTo.utc OR period.datetimeFrom.utc
        period = h.get("period", {})
        if isinstance(period, dict):
            observed = (
                (((period.get("datetimeTo") or {}).get("utc")) or
                 ((period.get("datetimeFrom") or {}).get("utc")))
            )
        if not observed:
            # fallback keys sometimes appear as 'date'/'datetime'
            observed = h.get("date") or h.get("datetime") or None

        result["last_pm25_time"] = observed

        # Recommend include if we got a reading in last 14 days.
        # Use an aware cutoff: `observed` parses to an aware datetime, and comparing
        # aware vs naive raises TypeError (which is why every row previously fell
        # through to the "timestamp parse unsure" branch).
        fresh_cutoff = datetime.datetime.now(timezone.utc) - timedelta(days=14)
        try:
            obs_dt = datetime.datetime.fromisoformat(observed.replace("Z", "+00:00"))
            if obs_dt.tzinfo is None:
                obs_dt = obs_dt.replace(tzinfo=timezone.utc)
            result["recommendation"] = "Include ✅" if obs_dt >= fresh_cutoff else "Stale (<14d) ⚠️"
        except Exception:
            result["recommendation"] = "Include (timestamp parse unsure) ⚠️"
    else:
        result["recommendation"] = "No hourly data in last 30d"

    return result

def main():
    if not API_KEY:
        print("ERROR: OPENAQ_API_KEY is not set. v3 requires an API key.", file=sys.stderr)
        sys.exit(1)

    cfg_path = os.path.join(os.path.dirname(__file__), "city_list.yaml")
    with open(cfg_path, "r") as f:
        raw = yaml.safe_load(f) or {}
    cities = raw.get("cities", {})

    rows = []
    for city, geo in cities.items():
        lat, lon, radius_m = geo["lat"], geo["lon"], geo.get("radius_m", 25000)
        try:
            info = check_city(city, lat, lon, radius_m)
        except requests.HTTPError as e:
            info = {"city": city, "pm25_locations": 0, "sample_location_id": None,
                    "sample_locality": None, "last_pm25_time": None,
                    "recommendation": f"HTTP error: {e.response.status_code}"}
        rows.append(info)

    # Pretty print
    header = ["City","PM2.5 locations","Sample location","Last PM2.5 time (UTC)","Recommendation"]
    print("\n" + " | ".join(header))
    print("-"*100)
    for r in rows:
        print(f"{r['city']} | {r['pm25_locations']} | "
              f"{(str(r['sample_location_id'])+' / '+str(r['sample_locality'])) if r['sample_location_id'] else '-'} | "
              f"{r['last_pm25_time'] or '-'} | {r['recommendation']}")

    # Save CSV for reference
    out = os.path.join(os.path.dirname(__file__), "coverage_report.csv")
    import csv
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["city","pm25_locations","sample_location_id","sample_locality","last_pm25_time","recommendation"])
        for r in rows:
            w.writerow([r["city"], r["pm25_locations"], r["sample_location_id"],
                        r["sample_locality"], r["last_pm25_time"], r["recommendation"]])
    print(f"\nSaved: {out}")

if __name__ == "__main__":
    main()
