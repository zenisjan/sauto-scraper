"""
Sauto.cz Scraper - Scraping tool for sauto.cz car marketplace (Seznam.cz).

This Actor scrapes vehicle listings from sauto.cz via its JSON API across multiple
vehicle categories and extracts detailed information including vehicle specs,
seller contact details, GPS and images.
Includes PostgreSQL database integration.

Completeness: the search API serves at most 10,000 results per query, so large
categories are split into price bands (each paged in the stable price order); a run
that could not reach every listing it was asked for is recorded as incomplete and
marked 'failed' so the web app's sold-detection ignores it.
"""

from __future__ import annotations

import os
import asyncio
import math
import re
import sys
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

from apify import Actor
from httpx import AsyncClient, HTTPError

# Import our database manager and gazetteer - handle different import paths
try:
    from .database import db_manager
    from .czech_cities import CZECH_CITIES
except ImportError:
    try:
        from database import db_manager
        from czech_cities import CZECH_CITIES
    except ImportError:
        # Add current directory to path if needed
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        from database import db_manager
        from czech_cities import CZECH_CITIES

BASE_URL = "https://www.sauto.cz"
API_BASE = "https://www.sauto.cz/api/v1"

# Sauto.cz category IDs mapped from SEO names
CATEGORY_MAP = {
    "osobni": 838,
    "uzitkova": 839,
    "nakladni": 840,
    "motorky": 841,
    "ctyrkolky": 842,
    "privesy-navesy": 843,
    "obytne": 844,
    "pracovni-stroje": 845,
    "autobusy": 846,
}

# Default categories to scrape when none specified
DEFAULT_CATEGORIES = list(CATEGORY_MAP.keys())

# Listings per search page. The site's list pages ask for 20, its seller admin for up
# to 100; the API serves 100 (verified 2026-10-25), so a full scan needs 5x fewer pages.
LISTINGS_PER_PAGE = 100

# Every page re-reads the previous page's last PAGE_OVERLAP listings and must share at
# least one listing with it. Sharing none means listings above our offset disappeared
# between the two requests and the rest shifted up past us, so we step back and re-read
# instead of silently skipping the shifted listings.
PAGE_OVERLAP = 5
MAX_PAGE_STEPBACKS = 5  # consecutive, per query

# The search API refuses offset >= 10,000 ("422 too_high_offset"), so one query reaches
# at most 10,000 listings. Bigger result sets are split into price bands of at most
# BAND_TARGET listings (headroom for listings added while we page a band).
API_MAX_OFFSET = 10_000
BAND_TARGET = 9_000
# Open-ended top band [lo, ∞) is split at max(2 * lo, lo + this).
OPEN_BAND_STEP = 100_000
# Churn allowed between two counts a second apart before we call listings unreachable.
BAND_COUNT_TOLERANCE = 5

# Full scans page in the site's "od nejlevnějších" order: price ascending, ties by id
# descending — a total order that bumps/edits do not change. The default order
# (sorting_date desc) moves every bumped listing to the top while we page, so offset
# paging over it skips listings.
FULL_SCAN_SORT = "price"

# A catch-up pass in the default (most recently changed first) order re-reads listings
# changed or added since the category scan started, minus this margin — e.g. a price
# change that moved a listing into a band we had already paged.
CATCH_UP_MARGIN = timedelta(minutes=10)

# Search requests start at least this far apart (one at a time).
SEARCH_MIN_INTERVAL_SECS = 1.0

# Detail records are fetched DETAIL_CONCURRENCY at a time; request starts stay
# DETAIL_MIN_INTERVAL_SECS apart (at most 4 requests/s).
DETAIL_CONCURRENCY = 4
DETAIL_MIN_INTERVAL_SECS = 0.25

# Listings are enriched and saved in batches so a long category keeps memory bounded
# and its rows reach the database as it goes.
SAVE_BATCH = 500

# The Postgres `price` column is INTEGER; anything above this can't be stored (it raises
# "integer out of range" and fails the whole insert), so such values are dropped to NULL.
_PRICE_MAX = 2_147_483_647

# Regions / districts of the site's "Místo prodeje" filter (region_cb / district_cb in
# /api/v1/items/filter_page); the search API filters by their SEO names via
# region_seo / district_seo (the old region_seo_name param is ignored by the API).
REGIONS = {
    "hlavni-mesto-praha": "Hlavní město Praha",
    "jihocesky-kraj": "Jihočeský kraj",
    "jihomoravsky-kraj": "Jihomoravský kraj",
    "karlovarsky-kraj": "Karlovarský kraj",
    "kraj-vysocina": "Kraj Vysočina",
    "kralovehradecky-kraj": "Královéhradecký kraj",
    "liberecky-kraj": "Liberecký kraj",
    "moravskoslezsky-kraj": "Moravskoslezský kraj",
    "olomoucky-kraj": "Olomoucký kraj",
    "pardubicky-kraj": "Pardubický kraj",
    "plzensky-kraj": "Plzeňský kraj",
    "stredocesky-kraj": "Středočeský kraj",
    "ustecky-kraj": "Ústecký kraj",
    "zlinsky-kraj": "Zlínský kraj",
}
DISTRICTS = {
    "hlavni-mesto-praha": "Hlavní město Praha",
    "ceske-budejovice": "České Budějovice", "cesky-krumlov": "Český Krumlov",
    "jindrichuv-hradec": "Jindřichův Hradec", "pisek": "Písek", "prachatice": "Prachatice",
    "strakonice": "Strakonice", "tabor": "Tábor",
    "blansko": "Blansko", "breclav": "Břeclav", "brno-mesto": "Brno-město",
    "brno-venkov": "Brno-venkov", "hodonin": "Hodonín", "vyskov": "Vyškov", "znojmo": "Znojmo",
    "cheb": "Cheb", "karlovy-vary": "Karlovy Vary", "sokolov": "Sokolov",
    "havlickuv-brod": "Havlíčkův Brod", "jihlava": "Jihlava", "pelhrimov": "Pelhřimov",
    "trebic": "Třebíč", "zdar-nad-sazavou": "Žďár nad Sázavou",
    "hradec-kralove": "Hradec Králové", "jicin": "Jičín", "nachod": "Náchod",
    "rychnov-nad-kneznou": "Rychnov nad Kněžnou", "trutnov": "Trutnov",
    "ceska-lipa": "Česká Lípa", "jablonec-nad-nisou": "Jablonec nad Nisou",
    "liberec": "Liberec", "semily": "Semily",
    "bruntal": "Bruntál", "frydek-mistek": "Frýdek-Místek", "karvina": "Karviná",
    "novy-jicin": "Nový Jičín", "opava": "Opava", "ostrava-mesto": "Ostrava-město",
    "jesenik": "Jeseník", "olomouc": "Olomouc", "prerov": "Přerov", "prostejov": "Prostějov",
    "sumperk": "Šumperk",
    "chrudim": "Chrudim", "pardubice": "Pardubice", "svitavy": "Svitavy",
    "usti-nad-orlici": "Ústí nad Orlicí",
    "domazlice": "Domažlice", "klatovy": "Klatovy", "plzen-jih": "Plzeň-jih",
    "plzen-mesto": "Plzeň-město", "plzen-sever": "Plzeň-sever", "rokycany": "Rokycany",
    "tachov": "Tachov",
    "benesov": "Benešov", "beroun": "Beroun", "kladno": "Kladno", "kolin": "Kolín",
    "kutna-hora": "Kutná Hora", "melnik": "Mělník", "mlada-boleslav": "Mladá Boleslav",
    "nymburk": "Nymburk", "praha-vychod": "Praha-východ", "praha-zapad": "Praha-západ",
    "pribram": "Příbram", "rakovnik": "Rakovník",
    "chomutov": "Chomutov", "decin": "Děčín", "litomerice": "Litoměřice", "louny": "Louny",
    "most": "Most", "teplice": "Teplice", "usti-nad-labem": "Ústí nad Labem",
    "kromeriz": "Kroměříž", "uherske-hradiste": "Uherské Hradiště", "vsetin": "Vsetín",
    "zlin": "Zlín",
}
_LOCATION_ALIASES = {
    "praha": "hlavni-mesto-praha",
    "prague": "hlavni-mesto-praha",
    "vysocina": "kraj-vysocina",
}


def _slugify(text: str) -> str:
    """Lowercase, strip Czech diacritics and non-url chars -> a-z0-9- slug."""
    normalized = unicodedata.normalize("NFKD", text or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")


def resolve_location(location: Optional[str]) -> Optional[Dict[str, str]]:
    """Map the `location` input to the search API's region/district filter.

    Accepts a region or district SEO name or Czech name ("jihomoravsky-kraj",
    "Jihomoravský kraj", "jihomoravsky", "praha", "brno-mesto", "Brno-město").
    Returns None when it is none of them — the caller must then fail the run rather
    than scrape all of Czechia under a regional slice.
    """
    slug = _slugify(location or "")
    if not slug:
        return None
    slug = _LOCATION_ALIASES.get(slug, slug)
    if slug in REGIONS:
        return {"region_seo": slug}
    if slug in DISTRICTS:
        return {"district_seo": slug}
    for candidate in (f"{slug}-kraj", f"kraj-{slug}"):
        if candidate in REGIONS:
            return {"region_seo": candidate}
    return None


# --- Coordinates -------------------------------------------------------------------
# Order of preference (never another town's coordinates):
#   1. the listing's own GPS when the API has it finer than municipality level,
#   2. the dealer premise's GPS when the premise is in the listing's own municipality
#      (or, for district-level listings, its own district),
#   3. the listing's own GPS at municipality / district level (the API's point),
#   4. the gazetteer point of the listing's own municipality (exact name, no substring
#      matching; rejected when it lies > 60 km from the listing's district — that is a
#      same-named town elsewhere),
#   5. for the single-town districts Praha and Brno-město, that town,
#   6. nothing (district-only listings elsewhere get no point rather than the district
#      capital's).
_GAZETTEER = {_slugify(name): coords for name, coords in CZECH_CITIES.items()}
_SINGLE_TOWN_DISTRICTS = {"hlavni-mesto-praha": "Praha", "brno-mesto": "Brno"}
_DISTRICT_MAX_KM = 60.0
_COARSE_ENTITY_TYPES = {"municipality", "district", "region", "country"}


def _gazetteer(name: Optional[str]) -> Optional[Tuple[float, float]]:
    return _GAZETTEER.get(_slugify(name)) if name else None


def _km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Great-circle distance in km."""
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(h)))


def _valid_gps(lat: Any, lon: Any) -> Optional[Tuple[float, float]]:
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180) or (lat_f == 0 and lon_f == 0):
        return None
    return lat_f, lon_f


def _town_coordinates(locality: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """Gazetteer point of the listing's own town (steps 4-5 above), else None."""
    district_seo = locality.get("district_seo_name") or _slugify(locality.get("district") or "")
    municipality = (locality.get("municipality") or "").strip()
    if municipality:
        coords = _gazetteer(municipality)
        if not coords:
            return None
        anchor = _gazetteer(_SINGLE_TOWN_DISTRICTS.get(district_seo) or locality.get("district"))
        if anchor and _km(coords, anchor) > _DISTRICT_MAX_KM:
            return None
        return coords
    if district_seo in _SINGLE_TOWN_DISTRICTS:
        return _gazetteer(_SINGLE_TOWN_DISTRICTS[district_seo])
    return None


def _same_place(locality: Dict[str, Any], premise_locality: Dict[str, Any]) -> bool:
    """Is the dealer premise in the listing's own municipality (or district)?"""
    municipality_id = locality.get("municipality_id")
    if municipality_id:
        return premise_locality.get("municipality_id") == municipality_id
    district_id = locality.get("district_id")
    if district_id:
        return premise_locality.get("district_id") == district_id
    return not locality.get("municipality") and not locality.get("district")


def _detail_coordinates(data: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    locality = data.get("locality") if isinstance(data.get("locality"), dict) else {}
    own = _valid_gps(locality.get("gps_lat"), locality.get("gps_lon"))
    if own and (locality.get("entity_type") or "").lower() not in _COARSE_ENTITY_TYPES:
        return own
    premise = data.get("premise") if isinstance(data.get("premise"), dict) else {}
    premise_locality = premise.get("locality") if isinstance(premise.get("locality"), dict) else {}
    premise_gps = _valid_gps(premise_locality.get("gps_lat"), premise_locality.get("gps_lon"))
    if premise_gps and _same_place(locality, premise_locality):
        return premise_gps
    if own:
        return own
    return _town_coordinates(locality)


def _location_text(locality: Dict[str, Any]) -> str:
    municipality = locality.get("municipality") or ""
    district = locality.get("district") or ""
    parts = [municipality] if municipality else []
    if district and district != municipality:
        parts.append(district)
    return ", ".join(parts)


# --- Prices -------------------------------------------------------------------------

def _clean_price(value: Any) -> Optional[int]:
    """A storable CZK amount (1 .. INTEGER max) or None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        amount = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    if amount <= 0 or amount > _PRICE_MAX:
        return None
    return amount


def _format_int(value: Any) -> str:
    """12345 -> '12 345' (anything that is not a number is passed through)."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{int(value):,}".replace(",", " ")
    return str(value)


def _format_czk(amount: int) -> str:
    return f"{_format_int(amount)} Kč"


def _price_text(price: int, data: Dict[str, Any]) -> str:
    """The buyer-facing price, plus the VAT terms the API states for it.

    `price` always includes VAT: when the seller entered a price without VAT
    (price_is_without_vat) the API adds 21 % and keeps the seller's amount in
    price_original; the site then shows "Bez DPH <amount>" under the price, as it does
    for VAT-deductible offers (price_without_vat when the API sends it).
    """
    text = _format_czk(price)
    if data.get("price_is_without_vat"):
        net = _clean_price(data.get("price_without_vat")) or _clean_price(data.get("price_original"))
        text += " vč. DPH" + (f" ({_format_czk(net)} bez DPH)" if net and net != price else "")
    elif data.get("price_is_vat_deductible"):
        net = _clean_price(data.get("price_without_vat"))
        text += " vč. DPH, odpočet DPH" + (f" ({_format_czk(net)} bez DPH)" if net and net != price else "")
    return text[:100]


def _parse_api_datetime(value: Any) -> Optional[datetime]:
    """API timestamps are naive local time ("2026-10-24T15:24:59")."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None)


def _parse_max_listings(value: Any) -> int:
    """maxListings per category; absent / invalid / 0 = unlimited."""
    try:
        cap = int(value)
    except (TypeError, ValueError):
        return 0
    return max(cap, 0)


# Safety margin (seconds) subtracted from the Apify hard timeout so the run can stop
# scraping, flush the listings it already has (card-only when their details were not
# fetched yet) and write the actor_run's final status before the platform kills it
# (which would otherwise leave the run stuck as "running").
_DEADLINE_MARGIN_SECS = 300
# Stop writing card-only rows this close to the hard timeout (the status update must
# still fit in).
_FLUSH_RESERVE_SECS = 45


def _timeout_at() -> Optional[datetime]:
    iso = os.environ.get("APIFY_TIMEOUT_AT") or os.environ.get("ACTOR_TIMEOUT_AT")
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None


def _run_deadline() -> Optional[datetime]:
    """Return the UTC time to stop scraping by, derived from the Apify timeout.

    Returns None (no limit) for local runs where the var is absent.
    """
    timeout_at = _timeout_at()
    if timeout_at is None:
        return None
    return timeout_at - timedelta(seconds=_DEADLINE_MARGIN_SECS)


def _seconds_to_timeout() -> Optional[float]:
    timeout_at = _timeout_at()
    return None if timeout_at is None else (timeout_at - _now_utc()).total_seconds()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class ConsentWallError(RuntimeError):
    """Seznam's CMP consent wall intercepted a request.

    The hardcoded consent cookies have rotated, so EVERY request will fail the
    same way — fail the whole run loudly (sold-detection then ignores the run)
    instead of quietly completing with zero listings.
    """


class ApiError(RuntimeError):
    """A sauto.cz API call failed (HTTP error incl. 422, transport, unparseable body)."""


# Bounded exponential backoff for transient fetch failures (timeouts, resets,
# 429/5xx). A single flaky response used to abort an entire category scrape.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_RETRY_BASE_DELAY_SECS = 2.0


async def _request_with_retry(client, method, url, attempts=3, **kwargs):
    """Issue a request, retrying transient failures with exponential backoff.

    Returns the final response without raising for status (callers keep their
    own raise_for_status()); re-raises the last transport error if the
    connection itself keeps failing.
    """
    for attempt in range(1, attempts + 1):
        try:
            response = await client.request(method, url, **kwargs)
        except HTTPError as e:
            if attempt == attempts:
                raise
            reason = repr(e)
        else:
            if response.status_code not in _RETRYABLE_STATUS or attempt == attempts:
                return response
            reason = f"HTTP {response.status_code}"
        delay = _RETRY_BASE_DELAY_SECS * (2 ** (attempt - 1))
        Actor.log.warning(
            f"Transient fetch failure ({reason}); retry {attempt}/{attempts - 1} "
            f"in {delay:.0f}s: {url}"
        )
        await asyncio.sleep(delay)


class _FailRunOnCrash:
    """Marks the actor_runs row 'failed' if the run dies with an unhandled error.

    Historically a terminal status was written only on the happy path, so a crash
    (or an Apify hard-timeout kill) left the row 'running' forever and the web app
    could not tell a dead run from a live one — sold-detection then mistreated the
    run's listings. Used as `async with Actor, _FailRunOnCrash():` so it exits
    before Actor; the exception still propagates and fails the platform run too.
    """

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            try:
                Actor.log.error(f"Run failed with unhandled error: {exc!r}")
                db_manager.update_actor_run_status("failed", 0)
                db_manager.close_pool()
            except Exception as finalize_error:
                Actor.log.error(f"Could not mark run failed: {finalize_error}")
        return False


def _price_params(lo: Optional[int], hi: Optional[int]) -> Dict[str, int]:
    params: Dict[str, int] = {}
    if lo is not None:
        params["price_from"] = lo
    if hi is not None:
        params["price_to"] = hi
    return params


def _band_label(lo: Optional[int], hi: Optional[int]) -> str:
    if lo is None and hi is None:
        return "all prices"
    return f"price band {lo if lo is not None else 0}-{hi if hi is not None else '∞'} CZK"


class SautoScraper:
    """Main scraper class for Sauto.cz listings via JSON API."""

    def __init__(self, client: AsyncClient):
        self.client = client
        self.scraped_listings: set[str] = set()
        # UTC instant to stop scraping by (set from the Apify timeout in main()).
        self.deadline: Optional[datetime] = None
        # {"region_seo": ...} / {"district_seo": ...} from the location input.
        self.location_filter: Optional[Dict[str, str]] = None
        # Why this run is incomplete (paging stopped early, the time budget ran out,
        # the consent wall); main() adds its own reasons and marks such a run 'failed'.
        self.incomplete_reasons: List[str] = []
        # Detail records that still failed after retries (saved from their card).
        self.detail_failures = 0
        # Detail records not fetched: time budget spent / consent wall hit.
        self.details_skipped_deadline = 0
        self.details_skipped_blocked = 0
        self._details_blocked = False
        self.details_requested = 0
        # Event-loop times the next detail / search request may start.
        self._next_detail_start = 0.0
        self._next_search_start = 0.0
        self.search_requests = 0
        # Search results that could not be turned into a listing (format change).
        self.parse_failures = 0
        self._set_consent_cookies()

    def _past_deadline(self) -> bool:
        return self.deadline is not None and _now_utc() >= self.deadline

    def _set_consent_cookies(self):
        """Set Seznam.cz CMP consent cookies to bypass the consent redirect.

        Sauto.cz is owned by Seznam.cz and uses the same consent system.
        Without these cookies, requests redirect through cmp.seznam.cz.
        """
        # szncmpone=1 is the Seznam.cz "consent given" flag
        self.client.cookies.set("szncmpone", "1", domain=".seznam.cz")
        self.client.cookies.set("szncmpone", "1", domain=".sauto.cz")

        # euconsent-v2 is the IAB TCF v2.0 consent string.
        # This is a real consent string captured from the Seznam.cz CMP
        # with all purposes and vendors accepted. Valid for 13 months.
        euconsent = (
            "CQgzIcAQgzIcAD3ACQCSCUFsAP_gAEPgAATIJNQJgAFAAQAAqABkAEAAKAAZAA0"
            "ACSAEwAJwAWwAvwBhAGIAQEAggCEAEUAI4ATgAoQBxADuAIQAUgA04COgE2gKkAV"
            "kAtwBeYDGQGWAMuAf4BAcCMwEmgSrgKgAVABAADIAGgATAAxAB-AEIAI4ATgA7gCE"
            "AEWATaAqQBWQC3AF5gMsAZcBKsAA.IJVwKgAFAAQAAqABkAEAAKAAZAA0ACSAEwA"
            "JwAWwAvwBhAGIAPwAgIBBAEIAIoARwAnABQgDNAHEAO4AhABFgCkAGnAR0Am0BUg"
            "CsgFuALzAYyAywBlwD_AIDgRmAk0BKsAA.YAAAAAAAAWAA"
        )
        self.client.cookies.set("euconsent-v2", euconsent, domain=".seznam.cz")
        self.client.cookies.set("euconsent-v2", euconsent, domain=".sauto.cz")

    async def _fetch_json(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """GET an API endpoint and return its JSON object.

        Raises ApiError for anything but a 200 JSON object (HTTP errors such as
        422 too_high_offset, transport failures after retries, unparseable bodies)
        and ConsentWallError when the request was redirected to the consent wall.
        """
        url = f"{API_BASE}/{endpoint}"
        try:
            response = await _request_with_retry(
                self.client, "GET", url, params=params, follow_redirects=True
            )
        except HTTPError as e:
            raise ApiError(f"request failed: {e!r}") from e

        final_url = str(response.url)
        if "cmp.seznam.cz" in final_url:
            raise ConsentWallError(
                f"CMP consent wall intercepted the API call ({final_url}) — "
                f"consent cookies rotated; update _set_consent_cookies()"
            )
        if response.status_code != 200:
            codes = ""
            try:
                errors = response.json().get("errors") or []
                codes = ",".join(
                    str(err.get("error_code")) for err in errors
                    if isinstance(err, dict) and err.get("error_code")
                )
            except Exception:
                pass
            raise ApiError(f"HTTP {response.status_code}" + (f" ({codes})" if codes else ""))
        try:
            data = response.json()
        except ValueError as e:
            raise ApiError(
                f"unparseable response ({response.headers.get('content-type', '?')})"
            ) from e
        if not isinstance(data, dict):
            raise ApiError("unparseable response (not a JSON object)")
        return data

    async def _pace_search(self) -> None:
        """Wait for this search request's start slot, SEARCH_MIN_INTERVAL_SECS apart."""
        loop = asyncio.get_running_loop()
        start = max(loop.time(), self._next_search_start)
        self._next_search_start = start + SEARCH_MIN_INTERVAL_SECS
        await asyncio.sleep(start - loop.time())

    async def _search(self, params: Dict[str, Any]) -> dict:
        """One search-API call; raises ApiError unless it returns a usable page."""
        await self._pace_search()
        self.search_requests += 1
        data = await self._fetch_json("items/search", params)
        pagination = data.get("pagination")
        results = data.get("results")
        if (
            not isinstance(pagination, dict)
            or not isinstance(pagination.get("total"), int)
            or not isinstance(results, list)
        ):
            raise ApiError(f"unexpected search response (keys: {sorted(data)[:8]})")
        # The API answers unknown filters with a warning and ignores them — scraping
        # on would silently return a wider result set than asked for.
        unsupported = (data.get("warnings") or {}).get("unsupported_filters")
        if unsupported:
            raise ApiError(f"the API ignored filter(s) {unsupported}")
        return data

    async def _count(self, base: Dict[str, Any], lo: Optional[int], hi: Optional[int]) -> int:
        data = await self._search({**base, **_price_params(lo, hi), "limit": 0, "offset": 0})
        return data["pagination"]["total"]

    def _matches_location(self, item: dict) -> bool:
        if not self.location_filter:
            return True
        locality = item.get("locality") if isinstance(item.get("locality"), dict) else {}
        if "region_seo" in self.location_filter:
            return locality.get("region_seo_name") == self.location_filter["region_seo"]
        return locality.get("district_seo_name") == self.location_filter.get("district_seo")

    async def scrape_category_listings(
        self,
        category: str,
        max_listings: int = 0,
        search_query: Optional[str] = None,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Collect a category's listing cards.

        - max_listings 1..BAND_TARGET: the newest listings first (the site's default
          order), stopping at the cap.
        - otherwise (0 = unlimited): every listing — price bands of <= BAND_TARGET
          listings, each paged in price order, then a catch-up pass over listings
          changed while we paged.

        Every early stop (a failed / unparseable page incl. 422, the time budget, an
        unreachable remainder) is recorded in self.incomplete_reasons; stopping at
        max_listings is a normal end. ConsentWallError propagates (it ends the run).
        """
        category_id = CATEGORY_MAP.get(category)
        if category_id is None:
            Actor.log.error(f"Invalid category: {category}")
            self.incomplete_reasons.append(f"{category}: unknown category")
            return []

        base: Dict[str, Any] = {"category_id": category_id}
        if search_query:
            base["phrase"] = search_query
        if self.location_filter:
            base.update(self.location_filter)
        user_prices = _price_params(price_min or None, price_max or None)
        listings: List[Dict[str, Any]] = []

        Actor.log.info(f"Starting to scrape category: {category} (id={category_id})")
        try:
            if 0 < max_listings <= BAND_TARGET:
                await self._page_query(
                    category, {**base, **user_prices}, "newest listings", listings,
                    cap=max_listings,
                )
            else:
                await self._full_scan(
                    category, base, price_min or None, price_max or None, listings,
                    cap=max_listings,
                )
        except ApiError as e:
            Actor.log.error(f"{category}: search request failed: {e}")
            self.incomplete_reasons.append(f"{category}: search request failed: {e}")

        Actor.log.info(f"Scraped {len(listings)} listings from category {category}")
        return listings

    async def _full_scan(
        self,
        category: str,
        base: Dict[str, Any],
        price_min: Optional[int],
        price_max: Optional[int],
        listings: List[Dict[str, Any]],
        cap: int = 0,
    ) -> bool:
        # Newest listing first: the total, and the reference time for the catch-up.
        first = await self._search({**base, **_price_params(price_min, price_max),
                                    "limit": 1, "offset": 0})
        total = first["pagination"]["total"]
        Actor.log.info(f"{category}: {total} listings to scan")
        if total == 0:
            return True
        newest = first["results"][0] if first["results"] else {}
        since = _parse_api_datetime(newest.get("sorting_date"))

        complete = await self._scan_price_bands(
            category, base, price_min, price_max, total, listings, cap
        )
        if not complete or (cap and len(listings) >= cap):
            return complete
        if since is None:
            Actor.log.warning(f"{category}: no reference time; skipping the catch-up pass")
            return True
        before = len(listings)
        complete = await self._page_query(
            category, {**base, **_price_params(price_min, price_max)}, "catch-up",
            listings, cap=cap, stop_before=since - CATCH_UP_MARGIN,
        )
        Actor.log.info(
            f"{category}: catch-up pass found {len(listings) - before} listings "
            f"changed or added during the scan"
        )
        return complete

    async def _scan_price_bands(
        self,
        category: str,
        base: Dict[str, Any],
        lo: Optional[int],
        hi: Optional[int],
        total: int,
        listings: List[Dict[str, Any]],
        cap: int,
    ) -> bool:
        """Page every listing of the query, split into price bands of <= BAND_TARGET.

        Bands are processed cheapest first; a band over BAND_TARGET is split in two
        (by value; an open top band [lo, ∞) at max(2·lo, lo + OPEN_BAND_STEP)) and
        both halves are counted. Prices are integers and both bounds inclusive, so
        [lo, mid] + [mid+1, hi] covers the band exactly; only the first split of a
        query without priceMin (price_from=0) could lose listings that have no price —
        that difference is checked and recorded.
        """
        stack: List[Tuple[Optional[int], Optional[int], int]] = [(lo, hi, total)]
        bands = 0
        while stack:
            lo, hi, total = stack.pop()
            label = _band_label(lo, hi)
            if self._past_deadline():
                self.incomplete_reasons.append(
                    f"{category}: run time budget reached before {label}"
                )
                return False
            if total > BAND_TARGET:
                lo_n = lo if lo is not None else 0
                if hi is None:
                    mid: Optional[int] = max(2 * lo_n, lo_n + OPEN_BAND_STEP)
                elif lo_n < hi:
                    mid = (lo_n + hi) // 2
                else:
                    mid = None  # one price point with > BAND_TARGET listings
                if mid is not None:
                    left = (lo_n, mid)
                    right = (mid + 1, hi)
                    t_left = await self._count(base, *left)
                    t_right = await self._count(base, *right)
                    if lo is None and t_left + t_right < total - BAND_COUNT_TOLERANCE:
                        missing = total - t_left - t_right
                        Actor.log.error(
                            f"{category}: {missing} listings have no price usable by "
                            f"the price filter and cannot be reached through price bands"
                        )
                        self.incomplete_reasons.append(
                            f"{category}: {missing} of {total} listings are not reachable "
                            f"through price bands (no price)"
                        )
                    Actor.log.info(
                        f"{category}: split {label} ({total}) into "
                        f"{_band_label(*left)} ({t_left}) + {_band_label(*right)} ({t_right})"
                    )
                    stack.append((right[0], right[1], t_right))
                    stack.append((left[0], left[1], t_left))
                    continue
            if total == 0:
                continue
            bands += 1
            Actor.log.info(f"{category}: paging {label} ({total} listings)")
            remaining_cap = cap - len(listings) if cap else 0
            band_listings: List[Dict[str, Any]] = []
            ok = await self._page_query(
                category, {**base, **_price_params(lo, hi), "sort": FULL_SCAN_SORT},
                label, band_listings, cap=remaining_cap,
            )
            listings.extend(band_listings)
            if not ok:
                return False
            if cap and len(listings) >= cap:
                Actor.log.info(f"{category}: reached maxListings ({cap})")
                return True
        Actor.log.info(f"{category}: paged {bands} price band(s)")
        return True

    async def _page_query(
        self,
        category: str,
        params: Dict[str, Any],
        label: str,
        listings: List[Dict[str, Any]],
        cap: int = 0,
        stop_before: Optional[datetime] = None,
    ) -> bool:
        """Page one query (at most API_MAX_OFFSET results) into `listings`.

        Offsets advance by LISTINGS_PER_PAGE - PAGE_OVERLAP; each page must share a
        listing with the previous one (see PAGE_OVERLAP) or we step back. Returns
        True at the natural end (the API's total reached), at the cap, or — with
        stop_before (catch-up, newest first) — once a page ends with a listing older
        than stop_before. Returns False after recording why it stopped early.
        ApiError from a failing request is recorded here too.
        """
        step = LISTINGS_PER_PAGE - PAGE_OVERLAP
        offset = 0
        prev_ids: Optional[set] = None
        stepbacks = 0
        location_mismatches = 0
        sort_warned = params.get("sort") != FULL_SCAN_SORT
        while True:
            if self._past_deadline():
                Actor.log.warning(f"Run time budget reached while paging {category} {label}")
                self.incomplete_reasons.append(
                    f"{category}: run time budget reached while paging {label} "
                    f"(offset {offset})"
                )
                return False
            if offset >= API_MAX_OFFSET:
                self.incomplete_reasons.append(
                    f"{category}: {label} has more listings than the API's "
                    f"{API_MAX_OFFSET:,}-result window"
                )
                return False

            try:
                data = await self._search({**params, "limit": LISTINGS_PER_PAGE, "offset": offset})
            except ApiError as e:
                Actor.log.error(f"{category}: {label} page at offset {offset} failed: {e}")
                self.incomplete_reasons.append(
                    f"{category}: {label} page at offset {offset} failed: {e}"
                )
                return False

            results = [r for r in data["results"] if isinstance(r, dict)]
            total = data["pagination"]["total"]
            ids = {str(r.get("id")) for r in results if r.get("id")}

            if prev_ids is not None and not ids & prev_ids:
                stepbacks += 1
                if stepbacks > MAX_PAGE_STEPBACKS:
                    self.incomplete_reasons.append(
                        f"{category}: {label} kept shifting under us at offset {offset}"
                    )
                    return False
                Actor.log.warning(
                    f"{category}: {label} page at offset {offset} does not overlap the "
                    f"previous page (listings removed meanwhile); stepping back"
                )
                offset = max(0, offset - step)
                continue
            stepbacks = 0

            if self.location_filter and results:
                # With the filter applied every listing is in the location (verified
                # live); mostly outside it means the API ignored the filter.
                mismatched = sum(1 for r in results if not self._matches_location(r))
                if mismatched * 2 > len(results):
                    self.incomplete_reasons.append(
                        f"{category}: the API did not apply the location filter "
                        f"{self.location_filter} ({mismatched} of {len(results)} listings "
                        f"outside it)"
                    )
                    return False
                location_mismatches += mismatched

            if not sort_warned:
                prices = [r.get("price") for r in results if isinstance(r.get("price"), (int, float))]
                if any(a > b for a, b in zip(prices, prices[1:])):
                    sort_warned = True
                    Actor.log.warning(
                        f"{category}: {label} is not in price order — the API may have "
                        f"dropped sort={FULL_SCAN_SORT}; bumped listings can be skipped"
                    )

            new_listings = self._extract_listings_from_api(results, category)
            listings.extend(new_listings)
            Actor.log.info(
                f"{category} {label}: offset {offset}, +{len(new_listings)} new "
                f"(total so far: {len(listings)}, available: {total})"
            )

            if cap and len(listings) >= cap:
                del listings[cap:]
                Actor.log.info(f"Reached maximum listings limit ({cap})")
                return True
            if stop_before is not None and results:
                oldest = _parse_api_datetime(results[-1].get("sorting_date"))
                if oldest is not None and oldest < stop_before:
                    return True
            if offset + len(results) >= total:
                if location_mismatches:
                    Actor.log.warning(
                        f"{category}: {location_mismatches} listings outside "
                        f"{self.location_filter} in {label}"
                    )
                return True
            if len(results) < LISTINGS_PER_PAGE:
                self.incomplete_reasons.append(
                    f"{category}: {label} page at offset {offset} returned "
                    f"{len(results)} listings, but the API reports {total}"
                )
                return False

            prev_ids = ids
            offset += step

    def _extract_listings_from_api(
        self, results: List[dict], category: str
    ) -> List[Dict[str, Any]]:
        """Extract listing data from API result objects (listings new to the run)."""

        listings = []

        for item in results:
            try:
                listing = self._extract_listing_data(item, category)
                if listing and listing["id"] not in self.scraped_listings:
                    listings.append(listing)
                    self.scraped_listings.add(listing["id"])
            except Exception as e:
                # A dropped listing would read as "sold"; surface the format change.
                Actor.log.error(f"Error extracting listing {item.get('id')}: {e!r}")
                self.parse_failures += 1
                if self.parse_failures == 1:
                    self.incomplete_reasons.append(
                        f"{category}: could not parse listing {item.get('id')}: {e!r}"
                    )
                continue

        return listings

    def _extract_listing_data(
        self, item: dict, category: str
    ) -> Optional[Dict[str, Any]]:
        """Extract data from a single API item object."""

        item_id = item.get("id")
        if not item_id:
            return None

        listing_id = str(item_id)[:50]
        name = item.get("name") or ""

        # Build URL: https://www.sauto.cz/{category}/detail/{manufacturer}/{model}/{id}
        cat_seo = (item.get("category") or {}).get("seo_name") or category
        mfr_seo = (item.get("manufacturer_cb") or {}).get("seo_name") or ""
        model_seo = (item.get("model_cb") or {}).get("seo_name") or ""

        if mfr_seo and model_seo:
            url = f"{BASE_URL}/{cat_seo}/detail/{mfr_seo}/{model_seo}/{listing_id}"
        elif mfr_seo:
            url = f"{BASE_URL}/{cat_seo}/detail/{mfr_seo}/{listing_id}"
        else:
            url = f"{BASE_URL}/{cat_seo}/detail/{listing_id}"

        # Price (VAT terms come only with the detail record)
        price = _clean_price(item.get("price"))
        if item.get("price_by_agreement"):
            price = None
            price_text = "Dohodou"
        else:
            price_text = _format_czk(price) if price is not None else ""

        # Location from locality; the search item has no GPS, so only the gazetteer
        # point of the listing's own town (the detail record may add precise GPS).
        locality = item.get("locality") if isinstance(item.get("locality"), dict) else {}
        location = _location_text(locality)
        coords = _town_coordinates(locality)

        # Image URL (first image)
        image_url = ""
        images_list = item.get("images") or []
        if images_list and isinstance(images_list[0], dict):
            first_img = images_list[0].get("url", "")
            if first_img:
                image_url = first_img if first_img.startswith("http") else f"https:{first_img}"

        # Build short description from vehicle specs
        desc_parts = []
        fuel = (item.get("fuel_cb") or {}).get("name", "")
        gearbox = (item.get("gearbox_cb") or {}).get("name", "")
        tachometer = item.get("tachometer")
        mfg_date = item.get("manufacturing_date") or ""

        if fuel:
            desc_parts.append(fuel)
        if gearbox:
            desc_parts.append(gearbox)
        if tachometer is not None:
            desc_parts.append(f"{_format_int(tachometer)} km")
        if mfg_date:
            year = str(mfg_date)[:4]
            desc_parts.append(f"Rok {year}")

        description = ", ".join(desc_parts)

        # Date
        date = str(item.get("edit_date") or item.get("create_date") or "")[:100]

        # Top/promoted status
        is_top = bool(item.get("topped", False))

        return {
            "id": listing_id,
            "title": name,
            "url": url,
            "category": category,
            "price": price,
            "price_text": price_text,
            "description": description,
            "location": location,
            "coordinates_lat": coords[0] if coords else None,
            "coordinates_lng": coords[1] if coords else None,
            "views": 0,  # Not available in API
            "date": date,
            "is_top": is_top,
            "image_url": image_url,
            "scraped_at": datetime.now().isoformat(),
        }

    async def scrape_details(
        self, category: str, listings: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Merge detail-record data into listings, keeping their order.

        Up to DETAIL_CONCURRENCY detail requests are in flight at once. Once the run's
        time budget is spent (or the consent wall blocks us) the remaining listings
        keep their card data, so we still save what we have; main() records the run
        as incomplete.
        """
        semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)

        async def enrich(listing: Dict[str, Any]) -> Dict[str, Any]:
            async with semaphore:
                if self._details_blocked:
                    self.details_skipped_blocked += 1
                    return listing
                if self._past_deadline():
                    self.details_skipped_deadline += 1
                    return listing
                await self._pace_detail_request()
                detailed = await self.scrape_detailed_data(listing)
            self.details_requested += 1
            if self.details_requested % 500 == 0:
                Actor.log.info(f"Requested {self.details_requested} detail records so far")
            return detailed

        return list(await asyncio.gather(*(enrich(item) for item in listings)))

    async def _pace_detail_request(self) -> None:
        """Wait for this detail request's start slot, DETAIL_MIN_INTERVAL_SECS apart."""
        loop = asyncio.get_running_loop()
        start = max(loop.time(), self._next_detail_start)
        self._next_detail_start = start + DETAIL_MIN_INTERVAL_SECS
        await asyncio.sleep(start - loop.time())

    async def scrape_detailed_data(self, listing: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch and merge the detail record for a single listing.

        If it still fails after retries the listing keeps its card data; the failure
        is counted (self.detail_failures), not treated as an early stop. The consent
        wall stops all further detail requests and is recorded as a reason.
        """
        item_id = listing["id"]
        try:
            data = await self._fetch_json(f"items/{item_id}")
            result = data.get("result")
            if not isinstance(result, dict):
                raise ApiError("detail response has no result object")
            details = self._extract_detailed_data(result)
        except ConsentWallError as e:
            self.detail_failures += 1
            if not self._details_blocked:
                self._details_blocked = True
                Actor.log.error(f"Consent wall on detail requests: {e}")
                self.incomplete_reasons.append(
                    f"consent wall on detail requests; remaining listings saved card-only: {e}"
                )
            return listing
        except Exception as e:
            self.detail_failures += 1
            Actor.log.warning(f"Error scraping detail {item_id}: {e}")
            return listing

        # Only override listing card data with non-empty detail values
        detailed_listing = {**listing}
        for key, value in details.items():
            if value is not None and value != "" and value != []:
                detailed_listing[key] = value
        return detailed_listing

    def _extract_detailed_data(self, data: dict) -> Dict[str, Any]:
        """Extract detailed information from item detail API response."""

        details: Dict[str, Any] = {}

        # Full description
        description = data.get("description", "")
        if description:
            details["full_description"] = description

        # Title
        name = data.get("name", "")
        if name:
            details["title"] = name

        # Price as buyers see it (VAT included), with the VAT terms in price_text
        price = _clean_price(data.get("price"))
        if data.get("price_by_agreement"):
            details["price_text"] = "Dohodou"
        elif price is not None:
            details["price"] = price
            details["price_text"] = _price_text(price, data)

        # Location & coordinates (API GPS first, see _detail_coordinates)
        locality = data.get("locality")
        if isinstance(locality, dict):
            location = _location_text(locality)
            if location:
                details["location"] = location
        coords = _detail_coordinates(data)
        if coords:
            details["coordinates_lat"], details["coordinates_lng"] = coords

        # Phone
        phone = data.get("phone", "")
        if phone:
            details["phone"] = str(phone)[:100]

        # Seller info from user
        user = data.get("user", {})
        if isinstance(user, dict):
            user_service = user.get("user_service", {})
            if isinstance(user_service, dict):
                shop_name = user_service.get("shop_name", "")
                if shop_name:
                    details["contact_name"] = str(shop_name)[:255]

        # Premise (dealer) info
        premise = data.get("premise")
        if premise and isinstance(premise, dict):
            premise_name = premise.get("name", "")
            if premise_name:
                details["contact_name"] = str(premise_name)[:255]

        # All images
        images_data = data.get("images", [])
        if images_data:
            images = []
            for img in images_data:
                if isinstance(img, dict):
                    img_url = img.get("url", "")
                    if img_url:
                        if not img_url.startswith("http"):
                            img_url = f"https:{img_url}"
                        images.append(img_url)
            if images:
                details["images"] = images
                details["image_url"] = images[0]

        # Date
        edit_date = data.get("edit_date", "")
        create_date = data.get("create_date", "")
        if edit_date:
            details["date"] = str(edit_date)[:100]
        elif create_date:
            details["date"] = str(create_date)[:100]

        # Vehicle-specific details as enriched description
        spec_parts = []
        if data.get("engine_power"):
            spec_parts.append(f"Vykon: {data['engine_power']} kW")
        if data.get("engine_volume"):
            spec_parts.append(f"Objem: {data['engine_volume']} ccm")
        if data.get("tachometer") is not None:
            spec_parts.append(f"Najeto: {_format_int(data['tachometer'])} km")
        fuel = data.get("fuel_cb", {})
        if isinstance(fuel, dict) and fuel.get("name"):
            spec_parts.append(f"Palivo: {fuel['name']}")
        gearbox = data.get("gearbox_cb", {})
        if isinstance(gearbox, dict) and gearbox.get("name"):
            spec_parts.append(f"Prevodovka: {gearbox['name']}")
        body = data.get("vehicle_body_cb", {})
        if isinstance(body, dict) and body.get("name"):
            spec_parts.append(f"Karoserie: {body['name']}")
        color = data.get("color_cb", {})
        if isinstance(color, dict) and color.get("name"):
            spec_parts.append(f"Barva: {color['name']}")
        condition = data.get("condition_cb", {})
        if isinstance(condition, dict) and condition.get("name"):
            spec_parts.append(f"Stav: {condition['name']}")
        if data.get("vin"):
            spec_parts.append(f"VIN: {data['vin']}")
        if data.get("stk_date"):
            spec_parts.append(f"STK: {data['stk_date']}")
        doors = data.get("doors")
        if doors:
            spec_parts.append(f"Dvere: {doors}")
        capacity = data.get("capacity")
        if capacity:
            spec_parts.append(f"Pocet mist: {capacity}")

        # Equipment list
        equipment = data.get("equipment_cb", [])
        if equipment:
            equip_names = [
                eq.get("name", "") for eq in equipment
                if isinstance(eq, dict) and eq.get("name")
            ]
            if equip_names:
                spec_parts.append(f"Vybava: {', '.join(equip_names)}")

        # Append specs to description
        if spec_parts:
            existing = details.get("full_description", "")
            spec_text = "\n".join(spec_parts)
            details["full_description"] = (
                f"{existing}\n\n{spec_text}" if existing else spec_text
            )

        # Top/promoted
        details["is_top"] = bool(data.get("topped", False))

        return details


async def main() -> None:
    """Main entry point for the Sauto.cz scraper."""

    async with Actor, _FailRunOnCrash():
        # Get input configuration
        actor_input = await Actor.get_input() or {}

        # Extract configuration with proper defaults
        categories = actor_input.get("categories")
        if not categories or len(categories) == 0:
            categories = DEFAULT_CATEGORIES

        # Per category; absent or 0 = unlimited (every listing).
        max_listings = _parse_max_listings(actor_input.get("maxListings"))
        include_detailed_data = actor_input.get("includeDetailedData", True)
        search_query = actor_input.get("searchQuery")
        location = (actor_input.get("location") or "").strip() or None
        price_min = actor_input.get("priceMin")
        price_max = actor_input.get("priceMax")
        location_filter = resolve_location(location) if location else None

        # Get actor run information
        actor_run_id = os.environ.get("APIFY_ACTOR_RUN_ID") or os.environ.get(
            "ACTOR_RUN_ID"
        )
        if not actor_run_id:
            actor_run_id = f"local-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        actor_run_start = datetime.now()
        run_deadline = _run_deadline()

        Actor.log.info(f"Starting Sauto.cz scraper with categories: {categories}")
        Actor.log.info(
            f"Max listings per category: {max_listings} (0 = unlimited)"
        )
        Actor.log.info(f"Include detailed data: {include_detailed_data}")
        Actor.log.info(f"Actor Run ID: {actor_run_id}")
        if run_deadline:
            Actor.log.info(f"Run time budget until (UTC): {run_deadline.isoformat()}")
        if search_query:
            Actor.log.info(f"Search query: {search_query}")
        if location:
            Actor.log.info(f"Location filter: {location!r} -> {location_filter}")
        if price_min or price_max:
            Actor.log.info(
                f"Price range: {price_min or 0} - {price_max or 'unlimited'} CZK"
            )

        # Initialize database connection
        try:
            scraper_name = os.environ.get("SCRAPER_NAME", "sauto")
            db_manager.scraper_name = scraper_name

            db_manager.initialize_pool()
            db_manager.set_actor_run_info(actor_run_id, actor_run_start)

            db_manager.create_actor_run(
                categories=categories,
                max_listings=max_listings,
                search_query=search_query,
                location=location,
                price_min=price_min,
                price_max=price_max,
            )

            Actor.log.info("Database connection established and actor run created")
            db_manager_available = True

        except Exception as e:
            Actor.log.error(f"Failed to initialize database: {e}")
            Actor.log.warning("Continuing without database integration")
            db_manager_available = False

        # Create HTTP client with proper headers.
        # Sauto.cz is owned by Seznam.cz and uses the same consent system.
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/webp,*/*;q=0.8",
            "Accept-Language": "cs,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        # Sauto.cz (Seznam) returns HTTP 404 to datacenter / non-Czech IPs, so
        # requests must be routed through a Czech residential proxy. The proxy
        # input defaults to Apify Proxy (RESIDENTIAL, country CZ) — see
        # input_schema.json — so scheduled runs get it without extra config.
        proxy_configuration = await Actor.create_proxy_configuration(
            actor_proxy_input=actor_input.get("proxyConfiguration")
        )
        proxy_url = await proxy_configuration.new_url() if proxy_configuration else None
        if proxy_url:
            Actor.log.info("Routing requests through Apify Proxy")
        else:
            Actor.log.warning(
                "No proxy configured — Seznam blocks datacenter IPs and will "
                "return 404. Enable Apify Proxy (residential, Czechia)."
            )

        async with AsyncClient(
            headers=headers, timeout=30.0, proxy=proxy_url
        ) as client:
            scraper = SautoScraper(client)
            scraper.deadline = run_deadline
            scraper.location_filter = location_filter
            scraped = 0  # listings produced by the run
            saved = 0  # rows actually inserted into the database
            # Run-level list of why the run is incomplete: the scraper records
            # paging/time-budget/consent stops, the loop below category and DB failures.
            incomplete_reasons = scraper.incomplete_reasons

            run_categories = categories
            if location and location_filter is None:
                Actor.log.error(
                    f"Location {location!r} is not a sauto.cz region or district"
                )
                incomplete_reasons.append(
                    f"location {location!r} is not a sauto.cz region or district "
                    f"(e.g. 'jihomoravsky-kraj', 'praha', 'brno-mesto'); not scraping "
                    f"all of Czechia under it"
                )
                run_categories = []

            for category_index, category in enumerate(run_categories):
                try:
                    if scraper._past_deadline():
                        Actor.log.warning(
                            "Run time budget reached; stopping before remaining categories"
                        )
                        incomplete_reasons.append(
                            "run time budget reached before "
                            + ", ".join(run_categories[category_index:])
                        )
                        break

                    Actor.log.info(f"=== Starting category: {category} ===")

                    # Refresh database connection pool every few categories
                    if (
                        db_manager_available
                        and category_index > 0
                        and category_index % 3 == 0
                    ):
                        try:
                            Actor.log.info(
                                "Refreshing database connection pool for long-running operation"
                            )
                            db_manager.refresh_pool()
                        except Exception as e:
                            Actor.log.warning(
                                f"Failed to refresh connection pool: {e}"
                            )

                    category_listings = await scraper.scrape_category_listings(
                        category=category,
                        max_listings=max_listings,
                        search_query=search_query,
                        price_min=price_min,
                        price_max=price_max,
                    )
                    category_total = len(category_listings)
                    scraped += category_total
                    if include_detailed_data and category_listings:
                        Actor.log.info(
                            f"Scraping detailed data for {category_total} "
                            f"listings from {category}"
                        )

                    # Enrich and save in batches; rows reach the DB as we go.
                    skipped_before = scraper.details_skipped_deadline
                    category_saved = 0
                    failed_rows = 0
                    first_db_error: Optional[Exception] = None
                    unsaved = 0
                    while category_listings:
                        batch = category_listings[:SAVE_BATCH]
                        del category_listings[:SAVE_BATCH]
                        if include_detailed_data:
                            batch = await scraper.scrape_details(category, batch)

                        left = _seconds_to_timeout()
                        if left is not None and left < _FLUSH_RESERVE_SECS:
                            unsaved = len(batch) + len(category_listings)
                            category_listings.clear()
                            break

                        if db_manager_available:
                            try:
                                db_manager.insert_listings(batch)
                                category_saved += len(batch)
                                saved += len(batch)
                            except Exception as e:
                                Actor.log.error(f"Failed to save listings to database: {e}")
                                # Refresh the connection pool and retry once
                                try:
                                    db_manager.refresh_pool()
                                    db_manager.insert_listings(batch)
                                    category_saved += len(batch)
                                    saved += len(batch)
                                    Actor.log.info(
                                        f"Saved {len(batch)} listings to database after retry"
                                    )
                                except Exception as retry_e:
                                    Actor.log.error(
                                        f"Failed to save listings to database even "
                                        f"after retry: {retry_e}"
                                    )
                                    failed_rows += len(batch)
                                    first_db_error = first_db_error or retry_e
                        try:
                            await Actor.push_data(batch)
                        except Exception as e:
                            Actor.log.warning(f"Could not push {len(batch)} items to the dataset: {e}")

                    if category_total:
                        Actor.log.info(
                            f"Saved {category_saved} of {category_total} listings to the "
                            f"database from category {category}"
                        )
                    skipped = scraper.details_skipped_deadline - skipped_before
                    if skipped:
                        Actor.log.warning(
                            f"Run time budget reached mid-detail; {skipped} of "
                            f"{category_total} listings saved card-only"
                        )
                        incomplete_reasons.append(
                            f"{category}: run time budget reached; {skipped} of "
                            f"{category_total} detail records not fetched"
                        )
                    if failed_rows:
                        incomplete_reasons.append(
                            f"{category}: database insert failed for {failed_rows} of "
                            f"{category_total} listings: {first_db_error!r}"
                        )
                    if unsaved:
                        incomplete_reasons.append(
                            f"{category}: out of time; {unsaved} of {category_total} "
                            f"listings not saved"
                        )
                        break

                    Actor.log.info(f"=== Completed category: {category} ===")

                except ConsentWallError as e:
                    Actor.log.error(f"Consent wall while scraping {category}: {e}")
                    incomplete_reasons.append(f"{category}: consent wall: {e}")
                    break
                except Exception as e:
                    Actor.log.error(f"Error scraping category {category}: {e}")
                    incomplete_reasons.append(f"{category}: {e!r}")
                    continue

            # Final summary and database cleanup
            Actor.log.info("=== SCRAPING COMPLETED ===")
            Actor.log.info(f"Total listings scraped: {scraped}; saved to the database: {saved}")
            Actor.log.info(f"Categories processed: {len(run_categories)}")
            Actor.log.info(f"Search requests: {scraper.search_requests}")
            if scraper.parse_failures:
                Actor.log.error(f"Listings that could not be parsed: {scraper.parse_failures}")
            if include_detailed_data:
                Actor.log.info(
                    f"Detail records requested: {scraper.details_requested}; failed after "
                    f"retries (saved from their card): {scraper.detail_failures}"
                )

            # Zero listings almost always means upstream breakage (site change,
            # consent wall, block) rather than an empty market — mark the run
            # 'failed' so sold-detection ignores it instead of flagging every
            # previously-seen listing as sold. The same goes for a run that
            # stopped early (a page failed, the time budget ran out, an insert
            # failed): sold-detection would flag everything it didn't reach.
            run_status = "failed" if incomplete_reasons or not saved else "completed"
            for reason in incomplete_reasons:
                Actor.log.error(f"Run incomplete — {reason}")

            # Update actor run status in database
            if db_manager_available:
                try:
                    db_manager.update_actor_run_status(run_status, saved)
                    db_manager.close_pool()
                    Actor.log.info("Database connection closed successfully")
                except Exception as e:
                    Actor.log.error(f"Failed to update actor run status: {e}")

            if incomplete_reasons:
                more = (
                    f" (+{len(incomplete_reasons) - 1} more)"
                    if len(incomplete_reasons) > 1
                    else ""
                )
                status_message = f"Stopped early: {incomplete_reasons[0]}{more}"
            else:
                status_message = (
                    f"Completed: {scraped} listings scraped "
                    f"from {len(run_categories)} categories"
                )
            try:
                await Actor.set_status_message(status_message)
            except Exception as e:
                Actor.log.warning(f"Could not set the run's status message: {e}")


if __name__ == "__main__":
    asyncio.run(main())
