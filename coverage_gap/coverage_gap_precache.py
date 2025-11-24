"""
Pre-caching service for coverage gap analysis.
Automatically caches results for common radius values every 24 hours.
"""
import asyncio
from typing import List, Dict
from warehouse.warehouse_service import _cache

# Pre-cached radius values (as floats to match query parameter types)
PRECACHED_RADII = [25.0, 50.0, 100.0, 500.0]

def get_precache_key(radius: float) -> str:
    """Generate cache key for pre-cached results."""
    return f"coverage_gap:precached:radius_{radius}"

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
        result = await get_coverage_gap_analysis(filters=None, radius_miles=radius)
        
        # Cache with 25 hour TTL (slightly longer than 24h to ensure overlap)
        _cache.set(cache_key, result, ttl=90000)  # 25 hours in seconds
        
        print(f"[PRECACHE] ✓ Successfully cached radius {radius} miles")
        return True
        
    except Exception as e:
        print(f"[PRECACHE] ✗ Error caching radius {radius}: {str(e)}")
        return False

async def precache_all_radii() -> Dict[int, str]:
    """
    Pre-cache all configured radius values.
    Returns status for each radius.
    """
    print("[PRECACHE] ===== Starting pre-cache job =====")
    results = {}
    
    for radius in PRECACHED_RADII:
        success = await precache_coverage_gap_analysis(radius)
        results[radius] = "success" if success else "failed"
    
    print(f"[PRECACHE] ===== Pre-cache job completed =====")
    print(f"[PRECACHE] Results: {results}")
    
    return results

