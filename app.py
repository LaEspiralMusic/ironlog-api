import os, io, json, uuid, hashlib
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import FastAPI, Header, HTTPException, Query, Path
from pydantic import BaseModel, Field, field_validator

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

API_KEY = os.getenv("API_KEY", "")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "")

GDRIVE_CLIENT_ID = os.getenv("GDRIVE_CLIENT_ID", "")
GDRIVE_CLIENT_SECRET = os.getenv("GDRIVE_CLIENT_SECRET", "")
GDRIVE_REFRESH_TOKEN = os.getenv("GDRIVE_REFRESH_TOKEN", "")
TOKEN_URI = "https://oauth2.googleapis.com/token"
SCOPES = ["https://www.googleapis.com/auth/drive"]

if not API_KEY:
    raise RuntimeError("API_KEY not set")
if not DRIVE_FOLDER_ID:
    raise RuntimeError("DRIVE_FOLDER_ID not set")
if not (GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET and GDRIVE_REFRESH_TOKEN):
    raise RuntimeError("GDRIVE_CLIENT_ID / GDRIVE_CLIENT_SECRET / GDRIVE_REFRESH_TOKEN not set")

creds = Credentials(
    token=None,
    refresh_token=GDRIVE_REFRESH_TOKEN,
    token_uri=TOKEN_URI,
    client_id=GDRIVE_CLIENT_ID,
    client_secret=GDRIVE_CLIENT_SECRET,
    scopes=SCOPES,
)
DRIVE = build("drive", "v3", credentials=creds, cache_discovery=False)

class Set(BaseModel):
    reps: int
    weight: float
    @field_validator("reps")
    @classmethod
    def reps_positive(cls, v):
        if v < 1: raise ValueError("reps must be >= 1")
        return v
    @field_validator("weight")
    @classmethod
    def weight_nonneg(cls, v):
        if v < 0: raise ValueError("weight must be >= 0")
        return v

class Exercise(BaseModel):
    name: str
    sets: List[Set]
    target_muscles: Optional[List[str]] = None
    @field_validator("name")
    @classmethod
    def name_nonempty(cls, v):
        if not v.strip(): raise ValueError("exercise name required")
        return v
    @field_validator("target_muscles")
    @classmethod
    def muscles_norm(cls, v):
        if v is None: return v
        return [m.strip().lower() for m in v if isinstance(m, str) and m.strip()]

class WorkoutLog(BaseModel):
    schema_version: int = 2
    date: str
    workout_type: Optional[str] = None  # "push" | "pull" | "legs"
    exercises: List[Exercise]
    notes: Optional[str] = None
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    @field_validator("date")
    @classmethod
    def date_iso(cls, v):
        from datetime import datetime
        try: datetime.strptime(v, "%Y-%m-%d")
        except ValueError: raise ValueError("date must be YYYY-MM-DD")
        return v
    @field_validator("workout_type")
    @classmethod
    def workout_type_enum(cls, v):
        if v is None: return v
        allowed = {"push","pull","legs"}
        vv = v.strip().lower()
        if vv not in allowed:
            raise ValueError("workout_type must be one of: push, pull, legs")
        return vv
    @field_validator("exercises")
    @classmethod
    def at_least_one_set(cls, v):
        if not v or not any(e.sets for e in v):
            raise ValueError("at least one exercise with sets is required")
        return v

def _find_file_by_name_in_folder(name: str, folder_id: str):
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = DRIVE.files().list(q=q, fields="files(id, name, md5Checksum)").execute()
    files = res.get("files", [])
    return files[0] if files else None

def _list_json_files_in_folder(folder_id: str):
    q = f"'{folder_id}' in parents and mimeType = 'application/json' and trashed = false"
    res = DRIVE.files().list(q=q, fields="files(id, name, md5Checksum)").execute()
    return res.get("files", [])

def _create_json_file(name: str, folder_id: str, data: dict):
    meta = {"name": name, "parents": [folder_id], "mimeType": "application/json"}
    buf = io.BytesIO(json.dumps(data, separators=(',', ':')).encode())
    media = MediaIoBaseUpload(buf, mimetype="application/json", resumable=False)
    return DRIVE.files().create(body=meta, media_body=media, fields="id,name").execute()

def _update_json_file(file_id: str, data: dict):
    buf = io.BytesIO(json.dumps(data, separators=(',', ':')).encode())
    media = MediaIoBaseUpload(buf, mimetype="application/json", resumable=False)
    return DRIVE.files().update(fileId=file_id, media_body=media, fields="id,name").execute()

def _read_json_file(file_id: str) -> dict:
    request = DRIVE.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return json.loads(buf.read().decode())

def _load_manifest_entries():
    idx = _find_file_by_name_in_folder("index.json", DRIVE_FOLDER_ID)
    if idx:
        manifest = _read_json_file(idx["id"])
        entries = manifest.get("entries", [])
        return sorted(entries, key=lambda e: e["date"])
    files = _list_json_files_in_folder(DRIVE_FOLDER_ID)
    entries = []
    for f in files:
        name = f.get("name", "")
        if name == "index.json":
            continue
        if name.endswith(".json"):
            date = name[:-5]
            entries.append({"date": date, "file": name})
    entries.sort(key=lambda e: e["date"])
    return entries

def _upsert_manifest(entry: dict):
    name = "index.json"
    existing = _find_file_by_name_in_folder(name, DRIVE_FOLDER_ID)
    if existing:
        manifest = _read_json_file(existing["id"])
    else:
        manifest = {"schema_version": 2, "entries": []}
    by_date = {e["date"]: e for e in manifest.get("entries", [])}
    by_date[entry["date"]] = entry
    manifest["entries"] = sorted(by_date.values(), key=lambda e: e["date"])
    if existing:
        _update_json_file(existing["id"], manifest)
    else:
        _create_json_file(name, DRIVE_FOLDER_ID, manifest)

from fastapi.middleware.cors import CORSMiddleware
app = FastAPI(title="IronLog Logs API", version="1.3.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

def require_auth(authorization: str):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(401, "Unauthorized")

@app.post("/logs")
def save_log(log: WorkoutLog, authorization: str = Header(default="")):
    require_auth(authorization)
    filename = f"{log.date}.json"
    payload = log.model_dump()
    sha = hashlib.sha256(json.dumps(payload, separators=(',', ':')).encode()).hexdigest()
    existing = _find_file_by_name_in_folder(filename, DRIVE_FOLDER_ID)
    if existing:
        _update_json_file(existing["id"], payload)
    else:
        _create_json_file(filename, DRIVE_FOLDER_ID, payload)
    _upsert_manifest({
        "date": log.date,
        "file": filename,
        "hash": sha,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": log.schema_version,
        "session_id": log.session_id,
        "workout_type": log.workout_type
    })
    return {"ok": True, "file": filename, "sha256": sha}

@app.get("/logs/index")
def list_logs(authorization: str = Header(default="")):
    require_auth(authorization)
    entries = _load_manifest_entries()
    return {"entries": entries}

@app.get("/logs/latest")
def latest_log(authorization: str = Header(default=""), before: Optional[str] = Query(default=None)):
    require_auth(authorization)
    entries = _load_manifest_entries()
    if before:
        entries = [e for e in entries if e["date"] < before]
    if not entries:
        raise HTTPException(404, "No logs found")
    latest = entries[-1]
    file = _find_file_by_name_in_folder(latest["file"], DRIVE_FOLDER_ID)
    data = _read_json_file(file["id"]) if file else {}
    return {"meta": latest, "log": data}

@app.get("/logs/latest_for_workout")
def latest_for_workout(authorization: str = Header(default=""), type: str = Query(..., pattern="^(push|pull|legs)$"), before: Optional[str] = Query(default=None)):
    require_auth(authorization)
    t = type.strip().lower()
    entries = _load_manifest_entries()
    if before:
        entries = [e for e in entries if e["date"] < before]
    for e in reversed(entries):
        wt = (e.get("workout_type") or "").strip().lower()
        if wt == t:
            file = _find_file_by_name_in_folder(e["file"], DRIVE_FOLDER_ID)
            if not file: 
                continue
            data = _read_json_file(file["id"])
            if (data.get("workout_type") or "").strip().lower() == t:
                return {"meta": e, "log": data}
            else:
                if any((ex.get("target_muscles") or []) for ex in data.get("exercises", [])):
                    return {"meta": e, "log": data}
    for e in reversed(entries):
        file = _find_file_by_name_in_folder(e["file"], DRIVE_FOLDER_ID)
        if not file: 
            continue
        data = _read_json_file(file["id"])
        if (data.get("workout_type") or "").strip().lower() == t:
            return {"meta": e, "log": data}
    raise HTTPException(404, f"No logs found for workout_type '{t}'")

@app.get("/logs/latest_for_muscle")
def latest_for_muscle(authorization: str = Header(default=""), muscle: str = Query(...), before: Optional[str] = Query(default=None)):
    require_auth(authorization)
    needle = muscle.strip().lower()
    entries = _load_manifest_entries()
    if before:
        entries = [e for e in entries if e["date"] < before]
    for e in reversed(entries):
        file = _find_file_by_name_in_folder(e["file"], DRIVE_FOLDER_ID)
        if not file: 
            continue
        data = _read_json_file(file["id"])
        for ex in data.get("exercises", []):
            muscles = [m.strip().lower() for m in (ex.get("target_muscles") or [])]
            if needle in muscles:
                return {"meta": e, "log": data, "matched_exercise": ex.get("name")}
    raise HTTPException(404, f"No logs found containing muscle '{muscle}'")

@app.get("/logs/by-date/{date}")
def fetch_log(
    date: str = Path(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    authorization: str = Header(default="")
):
    require_auth(authorization)
    filename = f"{date}.json"
    existing = _find_file_by_name_in_folder(filename, DRIVE_FOLDER_ID)
    if not existing: raise HTTPException(404, f"No log for {date}")
    return _read_json_file(existing["id"])
