"""
Narrative source: GDELT DOC 2.0 API, TimelineTone mode.

Tone is GDELT's native output. Negative tone on anxiety keywords =
narrative stress, used directly without sign flip.

Availability strategy (v1.1): GDELT's free endpoint rate-limits
aggressively per IP, and GitHub Actions runners share egress IPs with
every other GDELT scraper, so 429s are routine rather than exceptional.
Three layers keep the axis honest under that pressure:

1. Fresh cache (1 h): tone over a 1-day window moves slowly, so a
   reading is reused across runs for an hour. The cache file is
   committed alongside state.json, so this survives one-shot runs and
   cuts request volume by ~75%.
2. 429-aware backoff: Retry-After is honoured when present, with
   escalating delays otherwise. 429 is logged as rate limiting, not a
   generic failure.
3. Carry-forward (<= 24 h): when GDELT refuses entirely, the last good
   reading is reused and reported as "stale" — real data slightly old,
   never fabricated. Only past 24 h does the axis go to None, and the
   frontend's synthetic fallback is then a documented last resort.

Output: scalar in (-1, +1) per region. Negative = stress dominant.
Status: "live" (any region fresh), "stale" (carried forward only),
"simulated" (nothing usable). Ages are returned for meta transparency.
"""

import asyncio
import logging
import time

import httpx

from cache import cache
from normalise import tanh_squash, clip

log = logging.getLogger("animal-spirits.narrative")

GDELT_QUERY = '("recession" OR "unemployment" OR "inflation" OR "crisis" OR "layoffs" OR "bankruptcy")'

COUNTRY_CODES = {
    "us": "US",
    "uk": "UK",
    "india": "IN",
}

FRESH_TTL = 3600           # reuse a fetched tone for an hour across runs
LAST_GOOD_TTL = 7 * 86400  # keep last-good entries around for a week
MAX_STALE_S = 24 * 3600    # carry forward at most this long
REQUEST_PAUSE = 12.0       # between regions
MAX_ATTEMPTS = 3
RETRY_DELAYS = (20.0, 60.0)  # attempt 1 -> 2, attempt 2 -> 3
USER_AGENT = "AnimalSpirits/1.1 (research instrument; propensities.nz; ltan@unitec.ac.nz)"


def _retry_delay(attempt: int, response=None) -> float:
    """Backoff before the next attempt, honouring Retry-After if sent."""
    base = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
    if response is not None:
        ra = response.headers.get("Retry-After")
        if ra:
            try:
                return max(base, float(ra))
            except ValueError:
                pass
    return base


async def _gdelt_timeline_tone(client, country):
    """Return latest tone for a country, or None. Fresh-cached for an hour."""
    cache_key = f"gdelt_tone:{country}"
    cached = cache.get(cache_key)
    if cached is not None:
        log.info("GDELT TimelineTone %s: using fresh-cached tone %+.2f", country, cached)
        return cached

    full_query = f"{GDELT_QUERY} sourcecountry:{country}"
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": full_query,
        "mode": "TimelineTone",
        "format": "json",
        "timespan": "1d",
    }

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 429:
                log.warning("GDELT TimelineTone %s attempt %d: HTTP 429 (rate limited)", country, attempt)
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(_retry_delay(attempt, r))
                    continue
                return None
            if r.status_code != 200:
                log.warning("GDELT TimelineTone %s attempt %d: HTTP %d", country, attempt, r.status_code)
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(_retry_delay(attempt))
                    continue
                return None
            try:
                data = r.json()
            except Exception as e:
                log.warning("GDELT TimelineTone %s attempt %d: JSON parse failed: %s", country, attempt, e)
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(_retry_delay(attempt))
                    continue
                return None

            timeline = data.get("timeline", [])
            if not timeline:
                log.warning("GDELT TimelineTone %s: empty timeline", country)
                return None
            points = timeline[0].get("data", [])
            if not points:
                log.warning("GDELT TimelineTone %s: empty data points", country)
                return None

            latest_tone = None
            for point in reversed(points):
                v = point.get("value")
                if v is not None and isinstance(v, (int, float)):
                    latest_tone = float(v)
                    break

            if latest_tone is None:
                log.warning("GDELT TimelineTone %s: no numeric values", country)
                return None

            log.info("GDELT TimelineTone %s: latest tone = %+.2f (from %d points, attempt %d)",
                     country, latest_tone, len(points), attempt)
            cache.set(cache_key, latest_tone, FRESH_TTL)
            return latest_tone

        except httpx.ConnectTimeout:
            log.warning("GDELT TimelineTone %s attempt %d: ConnectTimeout", country, attempt)
            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(_retry_delay(attempt))
                continue
            return None
        except httpx.ReadTimeout:
            log.warning("GDELT TimelineTone %s attempt %d: ReadTimeout", country, attempt)
            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(_retry_delay(attempt))
                continue
            return None
        except Exception as e:
            log.warning("GDELT TimelineTone %s attempt %d: %s", country, attempt, repr(e))
            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(_retry_delay(attempt))
                continue
            return None

    return None


def _tone_to_scalar(tone: float) -> float:
    tone_norm = clip(tone / 5.0)
    return tanh_squash(tone_norm, scale=1.0)


async def fetch_narrative():
    """Return (values, status, meta_extra).

    status: "live" if any region carries a fresh reading, "stale" if only
    carried-forward readings are available, "simulated" if nothing usable.
    meta_extra reports the age of each region's reading in minutes, so
    staleness is documented in state.json rather than concealed.
    """
    out = {}
    ages_min = {}
    any_fresh = False
    any_carried = False
    now = time.time()

    timeout = httpx.Timeout(30.0, connect=15.0)
    limits = httpx.Limits(max_keepalive_connections=1, keepalive_expiry=60.0)

    async with httpx.AsyncClient(timeout=timeout, limits=limits,
                                 headers={"User-Agent": USER_AGENT}) as client:
        for region in ("us", "uk", "india"):
            await asyncio.sleep(REQUEST_PAUSE)

            country = COUNTRY_CODES[region]
            last_good_key = f"gdelt_tone_last_good:{country}"
            tone = await _gdelt_timeline_tone(client, country)

            if tone is not None:
                out[region] = _tone_to_scalar(tone)
                ages_min[region] = 0
                any_fresh = True
                cache.set(last_good_key, {"tone": tone, "observed_at": now}, LAST_GOOD_TTL)
                log.info("narrative[%s]: raw_tone=%+.2f -> scalar=%+.3f", region, tone, out[region])
                continue

            # Carry the last good reading forward, bounded and disclosed.
            last = cache.get(last_good_key)
            if last is not None and isinstance(last, dict):
                age = now - float(last.get("observed_at", 0))
                if 0 <= age <= MAX_STALE_S:
                    out[region] = _tone_to_scalar(float(last["tone"]))
                    ages_min[region] = round(age / 60)
                    any_carried = True
                    log.warning("narrative[%s]: GDELT unavailable, carrying forward last good "
                                "tone %+.2f (age %.0f min)", region, last["tone"], age / 60)
                    continue
                log.warning("narrative[%s]: last good reading too old (%.1f h), dropping",
                            region, age / 3600)

            out[region] = None
            ages_min[region] = None

    if any_fresh:
        status = "live"
    elif any_carried:
        status = "stale"
    else:
        status = "simulated"

    meta_extra = {"narrative_age_minutes": ages_min}
    return out, status, meta_extra
