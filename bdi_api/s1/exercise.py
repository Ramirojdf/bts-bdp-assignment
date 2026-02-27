import os
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """
    Downloads the `file_limit` files AS IS inside the folder data/raw/day=20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """

    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    # Ensure folder exists
    os.makedirs(download_dir, exist_ok=True)

    # Clean previous files
    for name in os.listdir(download_dir):
        path = os.path.join(download_dir, name)
        if os.path.isfile(path):
            os.remove(path)

    import re
    import requests

    headers = {
        "User-Agent": "Mozilla/5.0",
    }

    # Get directory listing
    response = requests.get(base_url, headers=headers, timeout=30)
    response.raise_for_status()
    html = response.text

    # Extract .json.gz filenames
    filenames = re.findall(r'href="([^"]+\.json\.gz)"', html)
    filenames = sorted(set(filenames))  # unique + ascending

    downloaded = 0
    minute = 0

    while downloaded < file_limit:
        filename = f"{minute:06d}Z.json.gz"
        url = base_url + filename
        out_path = os.path.join(download_dir, filename)

        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                with open(out_path, "wb") as f:
                    f.write(r.content)
                downloaded += 1
        except requests.RequestException:
            pass

        minute += 5

        # Stop after full day
        if minute > 1440:
            break

    return f"OK - downloaded {downloaded}"







@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """
    Prepare the data for analysis (to be implemented)
    """
    # stdlib only
    import gzip
    import json

    raw_dir = os.path.join(settings.raw_dir, "day=20231101")

    # Settings may or may not have prepared_dir; fallback to data/prepared
    prepared_root = getattr(settings, "prepared_dir", os.path.join("data", "prepared"))
    prepared_dir = os.path.join(prepared_root, "day=20231101")

    # Clean + create prepared folder
    os.makedirs(prepared_dir, exist_ok=True)
    for name in os.listdir(prepared_dir):
        path = os.path.join(prepared_dir, name)
        if os.path.isfile(path):
            os.remove(path)

    # Input validation
    if not os.path.exists(raw_dir):
        return f"ERROR - raw folder not found: {raw_dir}"

    raw_files = sorted([f for f in os.listdir(raw_dir) if f.endswith(".json.gz")])
    if not raw_files:
        return f"ERROR - no .json.gz files found in: {raw_dir}"

    positions_path = os.path.join(prepared_dir, "positions.jsonl")
    aircraft_path = os.path.join(prepared_dir, "aircraft.jsonl")

    # Keep unique aircraft info
    seen_aircraft: dict[str, dict] = {}

    positions_written = 0

    with open(positions_path, "w", encoding="utf-8") as pos_out:
        for fname in raw_files:
            fpath = os.path.join(raw_dir, fname)
            try:
                # Try gzip first
                with gzip.open(fpath, "rt", encoding="utf-8") as f:
                    payload = json.load(f)
            except OSError:
            # Not gzipped (or bad gzip header) -> try plain JSON
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        payload = json.load(f)
                except Exception:
                    continue
            except Exception:
                continue

            timestamp = payload.get("now")
            aircraft_list = payload.get("aircraft", [])
            if timestamp is None or not isinstance(aircraft_list, list):
                continue

            for a in aircraft_list:
                if not isinstance(a, dict):
                    continue

                icao = a.get("hex")
                lat = a.get("lat")
                lon = a.get("lon")

                # Only keep rows with position + icao
                if not icao or lat is None or lon is None:
                    continue

                # Save aircraft metadata once
                if icao not in seen_aircraft:
                    seen_aircraft[icao] = {
                        "icao": icao,
                        "registration": a.get("r"),
                        "type": a.get("t"),
                    }

                def to_number(v):
                    if v is None:
                        return None
                    if isinstance(v, (int, float)):
                        return v
                    if isinstance(v, str):
                        try:
                            return float(v)
                        except ValueError:
                            return None
                    return None

                row = {
                    "timestamp": timestamp,
                    "icao": icao,
                    "lat": lat,
                    "lon": lon,
                    # keep a couple of useful fields (optional but helpful later)
                    "alt_baro": to_number(a.get("alt_baro")),
                    "gs": to_number(a.get("gs")),
                    "emergency": a.get("emergency"),
                }

                pos_out.write(json.dumps(row) + "\n")
                positions_written += 1

    # Write aircraft list (unique)
    with open(aircraft_path, "w", encoding="utf-8") as ac_out:
        for icao in sorted(seen_aircraft.keys()):
            ac_out.write(json.dumps(seen_aircraft[icao]) + "\n")

    return (
        f"OK - prepared {len(raw_files)} raw files, "
        f"{positions_written} positions, {len(seen_aircraft)} aircraft "
        f"into {prepared_dir}"
    )




@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """
    List all available aircraft ordered by icao asc
    """
    import json

    # Same prepared path logic as /prepare
    prepared_root = getattr(settings, "prepared_dir", os.path.join("data", "prepared"))
    prepared_dir = os.path.join(prepared_root, "day=20231101")
    aircraft_path = os.path.join(prepared_dir, "aircraft.jsonl")

    if not os.path.exists(aircraft_path):
        return []

    # Load all aircraft (unique already), then sort by icao
    aircraft = []
    with open(aircraft_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                aircraft.append(json.loads(line))
            except Exception:
                continue

    aircraft.sort(key=lambda x: x.get("icao", ""))

    # Pagination
    if page < 0:
        page = 0
    if num_results <= 0:
        num_results = 100

    start = page * num_results
    end = start + num_results
    return aircraft[start:end]

@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    import json

    # Prepared path (same pattern as /prepare and /aircraft/)
    prepared_root = getattr(settings, "prepared_dir", os.path.join("data", "prepared"))
    prepared_dir = os.path.join(prepared_root, "day=20231101")
    positions_path = os.path.join(prepared_dir, "positions.jsonl")

    if not os.path.exists(positions_path):
        return []

    target = (icao or "").strip().lower()
    if not target:
        return []

    results: list[dict] = []

    # Load and filter
    with open(positions_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue

            if (row.get("icao") or "").lower() != target:
                continue

            ts = row.get("timestamp")
            lat = row.get("lat")
            lon = row.get("lon")

            if ts is None or lat is None or lon is None:
                continue

            results.append({"timestamp": ts, "lat": lat, "lon": lon})

    # Sort by time ascending
    results.sort(key=lambda x: x["timestamp"])

    # Pagination
    if page < 0:
        page = 0
    if num_results <= 0:
        num_results = 1000

    start = page * num_results
    end = start + num_results
    return results[start:end]

@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    import json

    prepared_root = getattr(settings, "prepared_dir", os.path.join("data", "prepared"))
    prepared_dir = os.path.join(prepared_root, "day=20231101")
    positions_path = os.path.join(prepared_dir, "positions.jsonl")

    if not os.path.exists(positions_path):
        return {
            "max_altitude_baro": None,
            "max_ground_speed": None,
            "had_emergency": False,
        }

    target = (icao or "").strip().lower()

    max_alt = None
    max_gs = None
    had_emergency = False

    with open(positions_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue

            if (row.get("icao") or "").lower() != target:
                continue

            alt = row.get("alt_baro")
            gs = row.get("gs")
            emergency = row.get("emergency")

            if alt is not None:
                max_alt = alt if max_alt is None else max(max_alt, alt)

            if gs is not None:
                max_gs = gs if max_gs is None else max(max_gs, gs)

            if emergency not in (None, "", "none"):
                had_emergency = True

    return {
        "max_altitude_baro": max_alt,
        "max_ground_speed": max_gs,
        "had_emergency": had_emergency,
    }