from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

DEFAULT_REGISTRY_PATH = Path("app/config/location_registry.json")
DEFAULT_GEOCODE_CACHE_PATH = Path("var/location_geocode_cache.json")
DEFAULT_UNKNOWN_QUEUE_PATH = Path("var/location_unknown_queue.json")


@dataclass(slots=True)
class LocationRecord:
    canonical: str
    lat: float
    lon: float
    aliases: set[str]
    metros: set[str]


class LocationResolver:
    def __init__(
        self,
        preferences: dict[str, Any],
        *,
        registry_path: Path | None = None,
        geocode_cache_path: Path | None = None,
        unknown_queue_path: Path | None = None,
    ) -> None:
        self.preferences = preferences
        pref_registry = str(preferences.get("registry_path", "")).strip()
        pref_cache = str(preferences.get("geocode_cache_path", "")).strip()
        pref_unknown = str(preferences.get("unknown_queue_path", "")).strip()
        self.registry_path = registry_path or (
            Path(pref_registry) if pref_registry else DEFAULT_REGISTRY_PATH
        )
        self.geocode_cache_path = geocode_cache_path or (
            Path(pref_cache) if pref_cache else DEFAULT_GEOCODE_CACHE_PATH
        )
        self.unknown_queue_path = unknown_queue_path or (
            Path(pref_unknown) if pref_unknown else DEFAULT_UNKNOWN_QUEUE_PATH
        )
        self.alias_index: dict[str, LocationRecord] = {}
        self.canonical_index: dict[str, LocationRecord] = {}
        self.geocode_cache: dict[str, dict[str, Any]] = {}
        self._load_registry()
        self._load_geocode_cache()

    def matches_preference(self, job: dict[str, Any]) -> bool:
        enabled = bool(self.preferences.get("enabled", False))
        if not enabled:
            return True

        allowed_work_types = {
            str(item).strip().lower()
            for item in self.preferences.get("allowed_work_types", ["remote", "hybrid"])
        }
        work_type = str(job.get("work_type", "")).strip().lower()
        if work_type == "remote":
            return "remote" in allowed_work_types and self._is_north_america_remote(job)
        if work_type == "hybrid" and "hybrid" not in allowed_work_types:
            return False

        metro_target = str(self.preferences.get("metro", "dfw")).strip().lower()
        city_tokens = self._extract_location_candidates(job)
        for token in city_tokens:
            matched = self._lookup_alias(token)
            if matched and metro_target in matched.metros:
                return True

        # Unknown cities: deterministically geocode and cache.
        for token in city_tokens:
            if self._maybe_geocode_and_match(token, metro_target):
                return True

        return False

    def _is_north_america_remote(self, job: dict[str, Any]) -> bool:
        location = str(job.get("location", ""))
        title = str(job.get("title", ""))
        url = str(job.get("job_url", ""))
        text = f"{location} | {title} | {url}".lower()

        if any(
            marker in text
            for marker in (
                "north america",
                "united states",
                "united states of america",
                "usa",
                "u.s.",
                "canada",
                "mexico",
                "us-only",
                "us only",
                "us/canada",
                "canada/us",
                "remote us",
                "remote - us",
                "remote-us",
                "remote canada",
                "remote - canada",
                "/us/",
                "/ca/",
                "/mx/",
            )
        ):
            return True

        normalized_location = _normalize_location_key(location)
        us_state_suffix = re.search(r"\b[a-z .'-]+,\s*[a-z]{2}\b", normalized_location)
        if us_state_suffix:
            return True

        return False

    def _extract_location_candidates(self, job: dict[str, Any]) -> list[str]:
        values = [
            str(job.get("location", "")),
            str(job.get("title", "")),
            str(job.get("job_url", "")),
        ]
        text = " | ".join(value for value in values if value).strip()
        if not text:
            return []
        chunks = re.split(r"[|/]+|\bin\b|\bat\b|,", text, flags=re.IGNORECASE)
        tokens: list[str] = []
        for chunk in chunks:
            normalized = _normalize_location_key(chunk)
            if len(normalized) < 3:
                continue
            tokens.append(normalized)
        return list(dict.fromkeys(tokens))

    def _lookup_alias(self, token: str) -> LocationRecord | None:
        return self.alias_index.get(token)

    def _maybe_geocode_and_match(self, token: str, metro_target: str) -> bool:
        if token in self.alias_index:
            return metro_target in self.alias_index[token].metros

        cached = self.geocode_cache.get(token)
        if cached is None:
            cached = self._geocode_location(token)
            self.geocode_cache[token] = cached
            self._persist_geocode_cache()

        if not cached.get("ok", False):
            self._append_unknown(token)
            return False

        lat = float(cached["lat"])
        lon = float(cached["lon"])
        if metro_target == "dfw":
            center = self.preferences.get("dfw_center", [32.8998, -97.0403])
            radius = float(self.preferences.get("dfw_radius_miles", 45))
            if _haversine_miles(lat, lon, float(center[0]), float(center[1])) <= radius:
                record = LocationRecord(
                    canonical=token,
                    lat=lat,
                    lon=lon,
                    aliases={token},
                    metros={"dfw"},
                )
                self.alias_index[token] = record
                self.canonical_index[token] = record
                return True
        return False

    def _load_registry(self) -> None:
        if not self.registry_path.exists():
            return
        payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
        entries = payload.get("locations", [])
        if not isinstance(entries, list):
            return
        for item in entries:
            if not isinstance(item, dict):
                continue
            canonical = _normalize_location_key(str(item.get("canonical", "")))
            if not canonical:
                continue
            lat = float(item.get("lat", 0.0))
            lon = float(item.get("lon", 0.0))
            aliases = {
                _normalize_location_key(str(alias))
                for alias in item.get("aliases", [])
                if _normalize_location_key(str(alias))
            }
            aliases.add(canonical)
            metros = {str(metro).strip().lower() for metro in item.get("metros", []) if str(metro)}
            record = LocationRecord(
                canonical=canonical,
                lat=lat,
                lon=lon,
                aliases=aliases,
                metros=metros,
            )
            self.canonical_index[canonical] = record
            for alias in aliases:
                self.alias_index[alias] = record

    def _load_geocode_cache(self) -> None:
        if not self.geocode_cache_path.exists():
            return
        payload = json.loads(self.geocode_cache_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            self.geocode_cache = payload

    def _persist_geocode_cache(self) -> None:
        self.geocode_cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.geocode_cache_path.write_text(json.dumps(self.geocode_cache), encoding="utf-8")

    def _append_unknown(self, token: str) -> None:
        try:
            existing: list[str] = []
            if self.unknown_queue_path.exists():
                payload = json.loads(self.unknown_queue_path.read_text(encoding="utf-8"))
                if isinstance(payload, list):
                    existing = [str(item) for item in payload]
            if token not in existing:
                existing.append(token)
                self.unknown_queue_path.parent.mkdir(parents=True, exist_ok=True)
                self.unknown_queue_path.write_text(json.dumps(existing), encoding="utf-8")
        except Exception:
            return

    def _geocode_location(self, token: str) -> dict[str, Any]:
        try:
            query = urlencode({"q": token, "format": "jsonv2", "limit": 1, "countrycodes": "us"})
            url = f"https://nominatim.openstreetmap.org/search?{query}"
            request = Request(
                url,
                headers={
                    "User-Agent": "resume-job-agent-location/1.0",
                    "Accept": "application/json",
                },
            )
            with urlopen(request, timeout=2.0) as response:
                if int(getattr(response, "status", 200)) != 200:
                    return {"ok": False}
                items = json.loads(response.read().decode("utf-8", errors="ignore"))
                if not isinstance(items, list) or not items:
                    return {"ok": False}
                first = items[0]
                return {
                    "ok": True,
                    "lat": float(first.get("lat", 0.0)),
                    "lon": float(first.get("lon", 0.0)),
                }
        except Exception:
            return {"ok": False}


def _normalize_location_key(value: str) -> str:
    lowered = value.lower().strip()
    lowered = lowered.replace("\u2013", "-")
    lowered = re.sub(r"[^a-z0-9,\-\s]", " ", lowered)
    lowered = lowered.replace(" tx", " texas")
    lowered = re.sub(r"\s+", " ", lowered).strip(" ,-")
    return lowered


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c
