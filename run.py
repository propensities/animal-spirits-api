"""
Animal Spirits — scheduled state composer.
Runs once per invocation (via GitHub Actions cron), composes the unified
field state from all three sources, and writes data/state.json.
No server, no cold start, no continuous billing — just a function
that produces a static artefact on a schedule.
The frontend polls the raw JSON file from GitHub Pages (or the raw.
githubusercontent.com CDN), which is instant and free.
"""
import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from sources.market import fetch_market
from sources.attention import fetch_attention
from sources.narrative import fetch_narrative
from cache import cache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("animal-spirits")

REGIONS = ("us", "uk", "india")
OUTPUT_PATH = Path(__file__).parent / "data" / "state.json"
HISTORY_PATH = Path(__file__).parent / "data" / "history.jsonl"


async def _safe_source(coro, name: str, timeout: float = 180.0):
    """Run a source coroutine with a hard timeout.

    Always returns (data, status, meta_extra): sources may return either a
    2-tuple or a 3-tuple; the missing meta_extra is padded with {}.
    """
    try:
        result = await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        log.warning("%s timed out", name)
        return {r: None for r in REGIONS}, "simulated", {}
    except Exception as e:
        log.warning("%s failed: %s", name, e, exc_info=True)
        return {r: None for r in REGIONS}, "simulated", {}
    if len(result) == 2:
        data, status = result
        return data, status, {}
    return result


def _prune_history(history_path: Path):
    """Keep only last 90 days of history.jsonl"""
    if not history_path.exists():
        return
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    
    try:
        lines = history_path.read_text().strip().split('\n')
        lines = [l for l in lines if l.strip()]  # Remove empty lines
    except Exception as e:
        log.warning("Failed to read history.jsonl: %s", e)
        return
    
    recent_lines = []
    for line in lines:
        try:
            entry = json.loads(line)
            entry_time = datetime.fromisoformat(entry['timestamp'])
            if entry_time > cutoff_date:
                recent_lines.append(line)
        except Exception as e:
            log.warning("Failed to parse history line: %s", e)
    
    try:
        history_path.write_text('\n'.join(recent_lines) + '\n' if recent_lines else '')
        log.info("Pruned history.jsonl to %d entries (90 days)", len(recent_lines))
    except Exception as e:
        log.warning("Failed to write pruned history.jsonl: %s", e)


async def compose_and_write() -> dict:
    """Compose state and write to data/state.json and data/history.jsonl."""
    log.info("Starting state composition...")
    
    # Parallel fetch across all three axes. Narrative gets a longer budget:
    # its 429 backoff can legitimately take several minutes.
    (market_data,    market_status,    _), \
    (attention_data, attention_status, _), \
    (narrative_data, narrative_status, narrative_meta) = await asyncio.gather(
        _safe_source(fetch_market(),    "market"),
        _safe_source(fetch_attention(), "attention"),
        _safe_source(fetch_narrative(), "narrative", timeout=480.0),
    )
    
    regions_out: dict[str, dict[str, Optional[float]]] = {}
    for r in REGIONS:
        regions_out[r] = {
            "attention": attention_data.get(r),
            "market":    market_data.get(r),
            "narrative": narrative_data.get(r),
        }
    
    state = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "regions": regions_out,
        "meta": {
            "attention": attention_status,
            "market":    market_status,
            "narrative": narrative_status,
            **narrative_meta,
        },
    }
    
    # Ensure output directory exists and write state.json
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(state, indent=2))
    log.info("Wrote state to %s", OUTPUT_PATH)
    
    # Append to history.jsonl
    with open(HISTORY_PATH, 'a') as f:
        timestamp = state['timestamp']
        for region in REGIONS:
            entry = {
                'timestamp': timestamp,
                'region': region,
                'A': state['regions'][region]['attention'],
                'M': state['regions'][region]['market'],
                'N': state['regions'][region]['narrative']
            }
            f.write(json.dumps(entry) + '\n')
    log.info("Appended to history.jsonl")
    
    # Prune history to keep only last 90 days
    _prune_history(HISTORY_PATH)
    
    # Persist cache for next run (rolling baselines survive across invocations)
    cache.flush()
    log.info("Flushed cache with %d active keys", len(cache.keys()))
    
    # Log a summary
    log.info("State summary:")
    log.info("  meta: %s", state["meta"])
    for r, axes in state["regions"].items():
        log.info("  %s: %s", r, {k: (f"{v:+.3f}" if isinstance(v, (int, float)) else v) for k, v in axes.items()})
    
    return state


def main() -> int:
    try:
        state = asyncio.run(compose_and_write())
        # Exit non-zero only if ALL sources failed, so CI reflects real outages
        all_simulated = all(v == "simulated" for v in state["meta"].values())
        if all_simulated:
            log.error("All sources returned simulated — treating as failure")
            return 1
        return 0
    except Exception as e:
        log.exception("Fatal error in compose_and_write: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
