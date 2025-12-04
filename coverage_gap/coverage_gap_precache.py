"""
Pre-caching service for coverage gap analysis.
Automatically caches results for common radius values every 24 hours.
"""
import asyncio
import json
from datetime import datetime, timezone
from typing import List, Dict, AsyncGenerator, Optional
from warehouse.warehouse_service import _cache

# Pre-cached radius values (as floats to match query parameter types)
PRECACHED_RADII = [25.0, 50.0, 100.0, 250.0, 500.0]

# Cache key for last precache timestamp
LAST_PRECACHE_TIMESTAMP_KEY = "coverage_gap:precache:last_timestamp"

# Maximum number of retry attempts for failed radii
MAX_RETRIES = 3

# Base delay for exponential backoff (in seconds)
RETRY_BASE_DELAY = 5

def get_precache_key(radius: float) -> str:
    """Generate cache key for pre-cached results."""
    return f"coverage_gap:precached:radius_{radius}"

def save_last_precache_timestamp() -> str:
    """
    Save the current timestamp as the last precache completion time.
    Returns the ISO format timestamp string.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    # Store with a long TTL (30 days) so it persists even if cache expires
    _cache.set(LAST_PRECACHE_TIMESTAMP_KEY, timestamp, ttl=2592000)  # 30 days
    print(f"[PRECACHE] Saved last precache timestamp: {timestamp}")
    return timestamp

def get_last_precache_timestamp() -> Optional[str]:
    """
    Get the last precache completion timestamp.
    Returns ISO format timestamp string or None if never run.
    """
    return _cache.get(LAST_PRECACHE_TIMESTAMP_KEY)

async def precache_coverage_gap_analysis(radius: float) -> bool:
    """
    Pre-cache coverage gap analysis for a specific radius.
    Returns True if successful, False otherwise.
    """
    try:
        # Lazy import to avoid circular dependency
        from coverage_gap.coverage_gap_service import get_coverage_gap_analysis
        
        print(f"[PRECACHE] Starting pre-cache for radius: {radius} miles")
        
        # Generate cache key
        cache_key = get_precache_key(radius)
        
        # Run analysis with no filters (most common case)
        # Use skip_precache=True to force fresh analysis and bypass existing cache
        result = await get_coverage_gap_analysis(filters=None, radius_miles=radius, skip_precache=True)
        
        # Cache with 25 hour TTL (slightly longer than 24h to ensure overlap)
        _cache.set(cache_key, result, ttl=90000)  # 25 hours in seconds
        
        print(f"[PRECACHE] ✓ Successfully cached radius {radius} miles")
        return True
        
    except Exception as e:
        print(f"[PRECACHE] ✗ Error caching radius {radius}: {str(e)}")
        return False

async def precache_all_radii() -> Dict[float, str]:
    """
    Pre-cache all configured radius values.
    Includes automatic retry mechanism for failed radii with exponential backoff.
    Returns status for each radius.
    """
    print("[PRECACHE] ===== Starting pre-cache job =====")
    results = {}
    
    # Initial attempt for all radii
    for radius in PRECACHED_RADII:
        success = await precache_coverage_gap_analysis(radius)
        results[radius] = "success" if success else "failed"
    
    # Retry mechanism for failed radii
    retry_attempt = 0
    while retry_attempt < MAX_RETRIES:
        # Get list of failed radii
        failed_radii = [radius for radius, status in results.items() if status == "failed"]
        
        if not failed_radii:
            # All radii succeeded, break out of retry loop
            break
        
        retry_attempt += 1
        # Exponential backoff: 5s, 10s, 20s
        retry_delay = RETRY_BASE_DELAY * (2 ** (retry_attempt - 1))
        
        print(f"[PRECACHE] ===== Retry attempt {retry_attempt}/{MAX_RETRIES} for {len(failed_radii)} failed radii =====")
        print(f"[PRECACHE] Waiting {retry_delay} seconds before retry...")
        await asyncio.sleep(retry_delay)
        
        # Retry each failed radius
        for failed_radius in failed_radii:
            print(f"[PRECACHE] Retrying pre-cache for radius: {failed_radius} miles")
            success = await precache_coverage_gap_analysis(failed_radius)
            results[failed_radius] = "success" if success else "failed"
            
            status_msg = "✓ Retry successful" if success else "✗ Retry failed"
            print(f"[PRECACHE] {status_msg} for radius {failed_radius} miles")
    
    # Save timestamp after completion (even if some failed, we still record the run)
    save_last_precache_timestamp()
    
    print(f"[PRECACHE] ===== Pre-cache job completed =====")
    print(f"[PRECACHE] Results: {results}")
    if retry_attempt > 0:
        print(f"[PRECACHE] Used {retry_attempt} retry attempt(s)")
    
    return results

async def precache_all_radii_stream() -> AsyncGenerator[str, None]:
    """
    Pre-cache all configured radius values with streaming progress updates via SSE.
    Includes automatic retry mechanism for failed radii with exponential backoff.
    Yields progress messages and final results.
    """
    def format_log(message: str, progress: Optional[float] = None) -> str:
        """Helper to format SSE log messages"""
        log_data = {"type": "log", "message": message}
        if progress is not None:
            log_data["progress"] = progress
        return f"data: {json.dumps(log_data)}\n\n"
    
    def format_data(data: Dict) -> str:
        """Helper to format final result"""
        return f"data: {json.dumps({'type': 'data', 'data': data})}\n\n"
    
    def format_error(error: str) -> str:
        """Helper to format error message"""
        return f"data: {json.dumps({'type': 'error', 'message': error})}\n\n"
    
    try:
        yield format_log("Starting pre-cache job for all radii", 0)
        print("[PRECACHE] ===== Starting pre-cache job =====")
        
        results = {}
        total_radii = len(PRECACHED_RADII)
        
        # Initial attempt for all radii
        for index, radius in enumerate(PRECACHED_RADII):
            # Calculate progress percentage
            progress = (index / total_radii) * 100
            
            yield format_log(f"Pre-caching radius {radius} miles ({index + 1}/{total_radii})...", progress)
            print(f"[PRECACHE] Starting pre-cache for radius: {radius} miles")
            
            success = await precache_coverage_gap_analysis(radius)
            results[radius] = "success" if success else "failed"
            
            status_msg = "✓ Successfully cached" if success else "✗ Failed to cache"
            yield format_log(f"{status_msg} radius {radius} miles", progress)
        
        # Retry mechanism for failed radii
        retry_attempt = 0
        while retry_attempt < MAX_RETRIES:
            # Get list of failed radii
            failed_radii = [radius for radius, status in results.items() if status == "failed"]
            
            if not failed_radii:
                # All radii succeeded, break out of retry loop
                break
            
            retry_attempt += 1
            # Exponential backoff: 5s, 10s, 20s
            retry_delay = RETRY_BASE_DELAY * (2 ** (retry_attempt - 1))
            
            yield format_log(f"Retrying {len(failed_radii)} failed radii (attempt {retry_attempt}/{MAX_RETRIES})...", 90)
            print(f"[PRECACHE] ===== Retry attempt {retry_attempt}/{MAX_RETRIES} for {len(failed_radii)} failed radii =====")
            
            # Wait before retry with exponential backoff
            yield format_log(f"Waiting {retry_delay} seconds before retry...", 90)
            await asyncio.sleep(retry_delay)
            
            # Retry each failed radius
            for failed_radius in failed_radii:
                yield format_log(f"Retrying radius {failed_radius} miles...", 92)
                print(f"[PRECACHE] Retrying pre-cache for radius: {failed_radius} miles")
                
                success = await precache_coverage_gap_analysis(failed_radius)
                results[failed_radius] = "success" if success else "failed"
                
                status_msg = "✓ Retry successful" if success else "✗ Retry failed"
                yield format_log(f"{status_msg} for radius {failed_radius} miles", 92)
        
        # Save timestamp after completion (even if some failed, we still record the run)
        timestamp = save_last_precache_timestamp()
        
        # Final result
        yield format_log("Pre-cache job completed", 100)
        print(f"[PRECACHE] ===== Pre-cache job completed =====")
        print(f"[PRECACHE] Results: {results}")
        
        # Count successes and failures
        success_count = sum(1 for status in results.values() if status == "success")
        failed_count = len(results) - success_count
        
        final_data = {
            "message": "Pre-cache job completed",
            "results": results,
            "summary": {
                "total": len(results),
                "successful": success_count,
                "failed": failed_count,
                "radii": list(results.keys()),
                "retriesUsed": retry_attempt if failed_count > 0 else 0
            },
            "lastPrecacheTimestamp": timestamp
        }
        
        yield format_data(final_data)
        
    except Exception as e:
        error_msg = str(e)
        print(f"[PRECACHE] Error in pre-cache job: {error_msg}")
        yield format_error(f"Pre-cache failed: {error_msg}")

