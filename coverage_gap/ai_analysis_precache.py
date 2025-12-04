"""
Pre-caching service for AI analysis.
Automatically caches AI analysis results every 24 hours.
"""
import asyncio
import json
from typing import AsyncGenerator, Optional
from datetime import datetime, timezone
from warehouse.warehouse_service import _cache

# Cache key for AI analysis precache
AI_ANALYSIS_PRECACHE_KEY = "coverage_gap:ai_analysis:precached"
# Cache key for last AI analysis precache timestamp
LAST_AI_ANALYSIS_PRECACHE_TIMESTAMP_KEY = "coverage_gap:ai_analysis:precache:last_timestamp"

# Maximum number of retry attempts
MAX_RETRIES = 3
# Base delay for exponential backoff (in seconds)
RETRY_BASE_DELAY = 5

def save_last_ai_analysis_precache_timestamp() -> str:
    """
    Save the current timestamp as the last AI analysis precache completion time.
    Returns the ISO format timestamp string.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    # Store with a long TTL (30 days) so it persists even if cache expires
    _cache.set(LAST_AI_ANALYSIS_PRECACHE_TIMESTAMP_KEY, timestamp, ttl=2592000)  # 30 days
    print(f"[AI_ANALYSIS_PRECACHE] Saved last precache timestamp: {timestamp}")
    return timestamp

def get_last_ai_analysis_precache_timestamp() -> Optional[str]:
    """
    Get the last AI analysis precache completion timestamp.
    Returns ISO format timestamp string or None if never run.
    """
    return _cache.get(LAST_AI_ANALYSIS_PRECACHE_TIMESTAMP_KEY)

async def precache_ai_analysis() -> bool:
    """
    Pre-cache AI analysis for no filters (most common case).
    Includes automatic retry mechanism for failures with exponential backoff.
    Returns True if successful, False otherwise.
    """
    # Initial attempt
    success = await _precache_ai_analysis_once()
    
    # Retry mechanism if initial attempt failed
    retry_attempt = 0
    while not success and retry_attempt < MAX_RETRIES:
        retry_attempt += 1
        # Exponential backoff: 5s, 10s, 20s
        retry_delay = RETRY_BASE_DELAY * (2 ** (retry_attempt - 1))
        
        print(f"[AI_ANALYSIS_PRECACHE] Retry attempt {retry_attempt}/{MAX_RETRIES}")
        print(f"[AI_ANALYSIS_PRECACHE] Waiting {retry_delay} seconds before retry...")
        await asyncio.sleep(retry_delay)
        
        success = await _precache_ai_analysis_once()
    
    if success:
        print(f"[AI_ANALYSIS_PRECACHE] Completed successfully" + (f" after {retry_attempt} retry attempt(s)" if retry_attempt > 0 else ""))
    else:
        print(f"[AI_ANALYSIS_PRECACHE] Failed after {MAX_RETRIES} retry attempts")
    
    return success

async def _precache_ai_analysis_once() -> bool:
    """
    Single attempt to pre-cache AI analysis.
    Returns True if successful, False otherwise.
    """
    try:
        # Lazy import to avoid circular dependency
        from coverage_gap.coverage_gap_service import get_ai_analysis_only
        
        print("[AI_ANALYSIS_PRECACHE] Starting pre-cache for AI analysis")
        
        # Run analysis with no filters (most common case)
        # Pass skip_cache=True to force fresh analysis
        result = await get_ai_analysis_only(filters=None, skip_cache=True)
        
        # Note: The result is already cached by get_ai_analysis_only when filters=None
        # But we still save the timestamp here to track when precache ran
        
        # Save timestamp after successful cache
        save_last_ai_analysis_precache_timestamp()
        
        print("[AI_ANALYSIS_PRECACHE] ✓ Successfully cached AI analysis")
        return True
        
    except Exception as e:
        print(f"[AI_ANALYSIS_PRECACHE] ✗ Error caching AI analysis: {str(e)}")
        return False

async def precache_ai_analysis_stream() -> AsyncGenerator[str, None]:
    """
    Pre-cache AI analysis with streaming progress updates via SSE.
    Includes automatic retry mechanism for failures with exponential backoff.
    Yields progress messages and final results.
    """
    def format_log(message: str, progress: Optional[float] = None) -> str:
        """Helper to format SSE log messages"""
        log_data = {"type": "log", "message": message}
        if progress is not None:
            log_data["progress"] = progress
        return f"data: {json.dumps(log_data)}\n\n"
    
    def format_data(data: dict) -> str:
        """Helper to format final result"""
        return f"data: {json.dumps({'type': 'data', 'data': data})}\n\n"
    
    def format_error(error: str) -> str:
        """Helper to format error message"""
        return f"data: {json.dumps({'type': 'error', 'message': error})}\n\n"
    
    try:
        yield format_log("Starting AI analysis pre-cache job", 0)
        print("[AI_ANALYSIS_PRECACHE] ===== Starting AI analysis pre-cache job =====")
        
        # Initial attempt
        yield format_log("Fetching fresh AI analysis data...", 25)
        success = await _precache_ai_analysis_once()
        
        # Retry mechanism if initial attempt failed
        retry_attempt = 0
        while not success and retry_attempt < MAX_RETRIES:
            retry_attempt += 1
            # Exponential backoff: 5s, 10s, 20s
            retry_delay = RETRY_BASE_DELAY * (2 ** (retry_attempt - 1))
            
            yield format_log(f"AI analysis pre-cache failed. Retrying (attempt {retry_attempt}/{MAX_RETRIES})...", 50)
            print(f"[AI_ANALYSIS_PRECACHE] Retry attempt {retry_attempt}/{MAX_RETRIES}")
            
            yield format_log(f"Waiting {retry_delay} seconds before retry...", 60)
            await asyncio.sleep(retry_delay)
            
            yield format_log(f"Retrying AI analysis pre-cache...", 70)
            success = await _precache_ai_analysis_once()
        
        if success:
            timestamp = get_last_ai_analysis_precache_timestamp()
            yield format_log("AI analysis pre-cache completed successfully", 100)
            print("[AI_ANALYSIS_PRECACHE] ===== AI analysis pre-cache job completed =====")
            
            final_data = {
                "message": "AI analysis pre-cache job completed",
                "status": "success",
                "retriesUsed": retry_attempt,
                "lastPrecacheTimestamp": timestamp
            }
            yield format_data(final_data)
        else:
            yield format_log(f"AI analysis pre-cache failed after {MAX_RETRIES} retries", 100)
            yield format_error(f"AI analysis pre-cache failed after {MAX_RETRIES} retry attempts")
        
    except Exception as e:
        error_msg = str(e)
        print(f"[AI_ANALYSIS_PRECACHE] Error in AI analysis pre-cache job: {error_msg}")
        yield format_error(f"AI analysis pre-cache failed: {error_msg}")

