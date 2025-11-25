"""
Coverage Gap AI Analysis Service

This module provides AI-powered analysis for warehouse coverage gaps,
identifying areas where warehouse coverage is insufficient relative to demand.
"""

import os
import httpx
import google.generativeai as genai
from typing import List, Dict
from datetime import datetime, timezone, timedelta
from warehouse.models import StaticWarehouseData, AIAnalysisData, CoverageGap, HighRequestArea, RequestTrends, Recommendation

# Constants for API access
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
REQUEST_TABLE_NAME = "Requests"


def load_us_cities() -> Dict[str, Dict]:
    """Load all US cities from us_cities.json and return as dict keyed by city,state"""
    import json
    
    try:
        with open('data/us_cities.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        cities_dict = {}
        for city_data in data.get('cities', []):
            city_key = f"{city_data['city']},{city_data['state']}"
            cities_dict[city_key] = city_data
        
        return cities_dict
    except FileNotFoundError:
        print("Warning: us_cities.json not found. Run generate_us_cities.py first.")
        return {}
    except Exception as e:
        print(f"Error loading US cities: {e}")
        return {}


async def get_request_counts_by_city() -> Dict[str, int]:
    """Get request counts per city from Requests table.
    
    Returns:
        Dict keyed by "city,state" with request count as value
    """
    try:
        url = f"https://api.airtable.com/v0/{BASE_ID}/{REQUEST_TABLE_NAME}"
        headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
        params = {}
        
        city_request_counts = {}
        
        async with httpx.AsyncClient() as client:
            offset = None
            while True:
                if offset:
                    params["offset"] = offset
                resp = await client.get(url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                
                records = data.get("records", [])
                for record in records:
                    fields = record.get("fields", {})
                    city = fields.get("City", "").strip() if fields.get("City") else ""
                    state = fields.get("State", "").strip() if fields.get("State") else ""
                    
                    if city and state:
                        city_key = f"{city},{state}"
                        city_request_counts[city_key] = city_request_counts.get(city_key, 0) + 1
                
                offset = data.get("offset")
                if not offset:
                    break
        
        print(f"Loaded request counts for {len(city_request_counts)} cities from Requests table")
        return city_request_counts
        
    except Exception as e:
        print(f"Error getting request counts by city: {e}")
        return {}


async def get_request_trends(total_requests: int) -> RequestTrends:
    """Calculate actual request trends based on historical data."""
    
    try:
        # Define time periods
        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=7)
        fourteen_days_ago = now - timedelta(days=14)
        three_months_ago = now - timedelta(days=90)
        six_months_ago = now - timedelta(days=180)
        
        # Fetch all requests
        url = f"https://api.airtable.com/v0/{BASE_ID}/{REQUEST_TABLE_NAME}"
        headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
        params = {}
        
        # Count requests in different time periods
        past_week_count = 0
        previous_week_count = 0
        past_3_months_count = 0
        previous_3_months_count = 0
        
        async with httpx.AsyncClient() as client:
            offset = None
            while True:
                if offset:
                    params["offset"] = offset
                resp = await client.get(url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                
                records = data.get("records", [])
                for record in records:
                    created_time_str = record.get("createdTime")
                    if created_time_str:
                        try:
                            created_time = datetime.fromisoformat(created_time_str.replace('Z', '+00:00'))
                            
                            # Past week (last 7 days)
                            if seven_days_ago <= created_time <= now:
                                past_week_count += 1
                            # Previous week (7-14 days ago)
                            elif fourteen_days_ago <= created_time < seven_days_ago:
                                previous_week_count += 1
                            
                            # Past 3 months
                            if three_months_ago <= created_time <= now:
                                past_3_months_count += 1
                            # Previous 3 months (3-6 months ago)
                            elif six_months_ago <= created_time < three_months_ago:
                                previous_3_months_count += 1
                                
                        except (ValueError, AttributeError):
                            continue
                
                offset = data.get("offset")
                if not offset:
                    break
        
        # Calculate changes
        past_week_change = past_week_count - previous_week_count
        past_3_months_change = past_3_months_count - previous_3_months_count
        
        # Determine trend direction
        if past_week_change > 2 and past_3_months_change > 10:
            trend_direction = "increasing"
        elif past_week_change < -2 and past_3_months_change < -10:
            trend_direction = "decreasing"
        else:
            trend_direction = "stable"
        
        return RequestTrends(
            pastWeekChange=float(past_week_change),
            past3MonthsChange=float(past_3_months_change),
            trendDirection=trend_direction
        )
        
    except Exception as e:
        print(f"Error calculating request trends: {e}")
        # Fallback to default values
        return RequestTrends(
            pastWeekChange=0.0,
            past3MonthsChange=0.0,
            trendDirection="stable"
        )


async def analyze_coverage_gaps_with_ai(city_warehouses_dict: Dict[str, Dict], total_requests: int, total_unique_warehouses: int) -> AIAnalysisData:
    """Analyze coverage gaps - CODE calculates data, AI explains recommendations.
    
    Args:
        city_warehouses_dict: Pre-grouped city data with warehouses (with radius expansion)
        total_requests: Total requests across all warehouses
        total_unique_warehouses: Total number of unique warehouses
    """
    
    print("=== Starting optimized AI analysis (CODE for data, AI for recommendations) ===")
    
    # Load all US cities
    us_cities = load_us_cities()
    print(f"Loaded {len(us_cities)} US cities")
    
    # Get request counts by city from Requests table
    print("Fetching request counts by city from Requests table...")
    city_request_counts = await get_request_counts_by_city()
    print(f"Found requests in {len(city_request_counts)} cities")
    
    # Build set of cities that have warehouses (from Airtable)
    cities_with_warehouses = set(city_warehouses_dict.keys())
    print(f"Cities with warehouses: {len(cities_with_warehouses)}")
    
    # ========== COVERAGE GAPS (CODE ONLY) ==========
    # Identify cities with 0 warehouses from all US cities
    print("Calculating coverage gaps (cities with 0 warehouses)...")
    coverage_gaps = []
    
    for city_key, city_info in us_cities.items():
        # Skip if city has warehouses
        if city_key in cities_with_warehouses:
            continue
        
        # Get request count for this city
        request_count = city_request_counts.get(city_key, 0)
        
        # Calculate gap score: prioritize cities with requests
        # If city has requests but no warehouses, it's a high priority gap
        if request_count > 0:
            gap_score = min(request_count / 10.0, 1.0)  # Higher requests = higher score
        else:
            gap_score = 0.1  # Lower priority for cities without requests
        
        coverage_gaps.append(CoverageGap(
            city=city_info['city'],
            state=city_info['state'],
            latitude=city_info.get('latitude', 0.0),
            longitude=city_info.get('longitude', 0.0),
            zipcodes=city_info.get('zipcodes', []),
            warehouseCount=0,  # No warehouses
            gapScore=gap_score,
            requestCount=request_count  # Total requests for this city
        ))
    
    # Rank coverage gaps: cities with requests first, then by request volume
    coverage_gaps.sort(key=lambda x: (x.gapScore == 0.1, -x.gapScore))  # Requests first, then by score
    print(f"Found {len(coverage_gaps)} cities with 0 warehouses")
    
    # ========== HIGH REQUEST AREAS (CODE ONLY) ==========
    # Query Requests table directly, group by city, sort by volume
    print("Calculating high request areas from Requests table...")
    high_request_areas = []
    
    for city_key, request_count in city_request_counts.items():
        if request_count == 0:
            continue
        
        # Get city info
        city_info = us_cities.get(city_key, {})
        if not city_info:
            # If city not in US cities, try to get from warehouse data
            if city_key in city_warehouses_dict:
                data = city_warehouses_dict[city_key]
                city_info = {
                    "city": data["city"],
                    "state": data["state"],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "zipcodes": []
                }
            else:
                continue
        
        # Get warehouse count for this city
        warehouse_count = len(city_warehouses_dict.get(city_key, {}).get("warehouses", []))
        
        # Calculate coverage ratio
        ideal_warehouses = request_count / 3.0  # 1 warehouse per 3 requests
        coverage_ratio = warehouse_count / ideal_warehouses if ideal_warehouses > 0 else 0
        
        high_request_areas.append(HighRequestArea(
            city=city_info['city'],
            state=city_info['state'],
            latitude=city_info.get('latitude', 0.0),
            longitude=city_info.get('longitude', 0.0),
            zipcodes=city_info.get('zipcodes', []),
            requestCount=request_count,
            warehouseCount=warehouse_count,
            coverageRatio=round(coverage_ratio, 2)
        ))
    
    # Sort by request count (highest first)
    high_request_areas.sort(key=lambda x: x.requestCount, reverse=True)
    print(f"Found {len(high_request_areas)} high request areas")
    
    # ========== REQUEST TRENDS (CODE ONLY) ==========
    print("Calculating request trends...")
    request_trends = await get_request_trends(total_requests)
    
    # ========== CALCULATE GOLD COVERAGE RATIOS (CODE ONLY) ==========
    print("Calculating request-to-gold warehouse ratios...")
    gold_coverage_ratios = []
    
    for city_key, data in city_warehouses_dict.items():
        # Count gold warehouses (includes "Gold" and "Potential Gold")
        gold_count = sum(1 for w in data["warehouses"] if w.tier in ["Gold", "Potential Gold"])
        request_count = city_request_counts.get(city_key, 0)
        
        # Only consider cities with requests and warehouses
        if request_count > 0 and len(data["warehouses"]) > 0:
            # Calculate ratio: requests ÷ gold warehouses (higher = worse coverage)
            if gold_count > 0:
                ratio = request_count / gold_count
            else:
                # If no gold warehouses, use a high ratio to prioritize
                ratio = request_count * 10  # Penalize lack of gold warehouses
            
            gold_coverage_ratios.append({
                "city": data["city"],
                "state": data["state"],
                "city_key": city_key,
                "request_count": request_count,
                "gold_count": gold_count,
                "total_warehouses": len(data["warehouses"]),
                "ratio": ratio
            })
    
    # Sort by ratio (highest first) - worst ratios first
    gold_coverage_ratios.sort(key=lambda x: x["ratio"], reverse=True)
    print(f"Found {len(gold_coverage_ratios)} cities with request-to-gold ratios")
    
    # ========== DATA QUALITY ISSUES (CODE ONLY) ==========
    print("Identifying data quality issues...")
    data_quality_issues = []
    
    for city_key, data in city_warehouses_dict.items():
        total_warehouses = len(data["warehouses"])
        gold_count = sum(1 for w in data["warehouses"] if w.tier in ["Gold", "Potential Gold"])
        silver_count = sum(1 for w in data["warehouses"] if w.tier == "Silver")
        bronze_count = sum(1 for w in data["warehouses"] if w.tier == "Bronze")
        standard_tiers = ["Gold", "Potential Gold", "Silver", "Bronze"]
        un_tiered_count = sum(1 for w in data["warehouses"] 
                             if not w.tier or (isinstance(w.tier, str) and w.tier.strip() == "") 
                             or (w.tier not in standard_tiers))
        request_count = city_request_counts.get(city_key, 0)
        
        # Flag if: 10+ warehouses but 0-1 gold warehouses, OR 5+ warehouses with 0 gold and some requests
        if (total_warehouses >= 10 and gold_count <= 1) or (total_warehouses >= 5 and gold_count == 0 and request_count > 0):
            data_quality_issues.append({
                "city": data["city"],
                "state": data["state"],
                "city_key": city_key,
                "total_warehouses": total_warehouses,
                "gold_count": gold_count,
                "silver_count": silver_count,
                "bronze_count": bronze_count,
                "un_tiered_count": un_tiered_count,
                "total_requests": request_count
            })
    
    # Sort by warehouse count (highest first) to prioritize most critical issues
    data_quality_issues.sort(key=lambda x: x["total_warehouses"], reverse=True)
    print(f"Found {len(data_quality_issues)} data quality issues")
    
    # ========== RECOMMENDATIONS (AI ONLY) ==========
    # Use AI to explain the pre-calculated data
    gemini_key = os.getenv("GEMINI_API_KEY")
    recommendations = []
    
    if gemini_key:
        try:
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel("gemini-2.5-flash")
            
            # Prepare summary of pre-calculated data for AI
            top_coverage_gaps = [gap for gap in coverage_gaps if gap.gapScore > 0.1][:20]  # Top 20 with requests
            top_high_request_areas = high_request_areas[:20]  # Top 20
            top_gold_ratios = gold_coverage_ratios[:20]  # Top 20 worst ratios
            top_data_quality = data_quality_issues[:10]  # Top 10
            
            # Generate separate AI analysis for each recommendation (2-3 phrases each)
            
            # Recommendation 1: Expand Warehouse Network in Underserved Areas
            top_gaps_with_requests = [gap for gap in coverage_gaps if gap.gapScore > 0.1][:10]
            if top_gaps_with_requests:
                gap_cities_str = ", ".join([f"{gap.city}, {gap.state}" for gap in top_gaps_with_requests[:5]])
                gap_requests = [city_request_counts.get(f"{gap.city},{gap.state}", 0) for gap in top_gaps_with_requests[:5]]
                max_requests = max(gap_requests) if gap_requests else 0
                
                prompt1 = f"""
Analyze these cities with zero warehouses but existing requests: {gap_cities_str}
Top city has {max_requests} requests but no warehouse coverage.
Provide a concise 2-3 sentence strategic recommendation explaining why expanding here is critical and the business impact.
"""
                ai_response1 = await model.generate_content_async(prompt1)
                reasoning1 = ai_response1.text.strip()[:300]  # Limit to 300 chars
                
                recommendations.append(Recommendation(
                    priority="high",
                    action="Expand Warehouse Network in Underserved Areas",
                    targetCities=[{"city": gap.city, "state": gap.state} for gap in top_gaps_with_requests],
                    reasoning=reasoning1
                ))
            
            # Recommendation 2: Focus on High Request Volume Areas
            if top_high_request_areas[:10]:
                top_area = top_high_request_areas[0]
                top_areas_str = ", ".join([f"{area.city}, {area.state}" for area in top_high_request_areas[:5]])
                
                prompt2 = f"""
These cities have the highest request volumes: {top_areas_str}
Top city: {top_area.city}, {top_area.state} with {top_area.requestCount} requests and {top_area.warehouseCount} warehouses.
Provide a concise 2-3 sentence recommendation explaining the strategic importance and demand patterns.
"""
                ai_response2 = await model.generate_content_async(prompt2)
                reasoning2 = ai_response2.text.strip()[:300]  # Limit to 300 chars
                
                recommendations.append(Recommendation(
                    priority="medium",
                    action="Focus on High Request Volume Areas",
                    targetCities=[{"city": area.city, "state": area.state} for area in top_high_request_areas[:10]],
                    reasoning=reasoning2
                ))
            
            # Recommendation 3: Prioritize Coverage Gaps with Low Gold Coverage
            if top_gold_ratios[:10]:
                top_ratio = top_gold_ratios[0]
                top_ratios_str = ", ".join([f"{r['city']}, {r['state']} ({r['ratio']:.1f}:1)" for r in top_gold_ratios[:5]])
                
                prompt3 = f"""
These cities have the worst request-to-gold warehouse ratios: {top_ratios_str}
Worst: {top_ratio['city']}, {top_ratio['state']} has {top_ratio['request_count']} requests but only {top_ratio['gold_count']} gold warehouses = {top_ratio['ratio']:.1f}:1 ratio.
Provide a concise 2-3 sentence recommendation explaining why improving gold coverage here is critical.
"""
                ai_response3 = await model.generate_content_async(prompt3)
                reasoning3 = ai_response3.text.strip()[:300]  # Limit to 300 chars
                
                recommendations.append(Recommendation(
                    priority="high",
                    action="Prioritize Coverage Gaps with Low Gold Coverage",
                    targetCities=[{"city": r["city"], "state": r["state"]} for r in top_gold_ratios[:10]],
                    reasoning=reasoning3
                ))
            
            # Recommendation 4: Data Quality Issues (last position)
            if top_data_quality:
                top_issue = top_data_quality[0]
                issues_str = ", ".join([f"{issue['city']}, {issue['state']}" for issue in top_data_quality[:5]])
                
                prompt4 = f"""
These cities have data quality issues - many warehouses but few/no gold tiers: {issues_str}
Example: {top_issue['city']}, {top_issue['state']} has {top_issue['total_warehouses']} warehouses but only {top_issue['gold_count']} gold ({top_issue['un_tiered_count']} untiered).
Provide a concise 2-3 sentence recommendation explaining why data cleanup is needed here.
"""
                ai_response4 = await model.generate_content_async(prompt4)
                reasoning4 = ai_response4.text.strip()[:300]  # Limit to 300 chars
                
                recommendations.append(Recommendation(
                    priority="low",
                    action="Data Quality Issues",
                    targetCities=[{"city": issue["city"], "state": issue["state"]} for issue in top_data_quality],
                    reasoning=reasoning4
                ))
            
            print(f"AI generated {len(recommendations)} recommendations")
            
        except Exception as e:
            print(f"Error generating AI recommendations: {e}")
            # Fallback to data-based recommendations
            top_gaps_with_requests = [gap for gap in coverage_gaps if gap.gapScore > 0.1][:10]
            if top_gaps_with_requests:
                recommendations.append(Recommendation(
                    priority="high",
                    action="Expand Warehouse Network in Underserved Areas",
                    targetCities=[{"city": gap.city, "state": gap.state} for gap in top_gaps_with_requests],
                    reasoning=f"Identified {len(coverage_gaps)} cities with 0 warehouses within this city. Prioritize cities with existing requests."
                ))
            
            if high_request_areas[:10]:
                recommendations.append(Recommendation(
                    priority="medium",
                    action="Focus on High Request Volume Areas",
                    targetCities=[{"city": area.city, "state": area.state} for area in high_request_areas[:10]],
                    reasoning=f"Top {len(high_request_areas[:10])} cities with highest request volumes need attention."
                ))
            
            if gold_coverage_ratios[:10]:
                recommendations.append(Recommendation(
                    priority="high",
                    action="Prioritize Coverage Gaps with Low Gold Coverage",
                    targetCities=[{"city": r["city"], "state": r["state"]} for r in gold_coverage_ratios[:10]],
                    reasoning=f"Cities with worst request-to-gold ratios. Higher ratio = worse coverage quality."
                ))
            
            if data_quality_issues[:10]:
                recommendations.append(Recommendation(
                    priority="low",
                    action="Data Quality Issues",
                    targetCities=[{"city": issue["city"], "state": issue["state"]} for issue in data_quality_issues[:10]],
                    reasoning=f"Data cleanup needed: {len(data_quality_issues)} cities with high warehouse count but zero/low gold warehouses."
                ))
    else:
        print("GEMINI_API_KEY not found, using data-based recommendations only")
        # Fallback recommendations
        top_gaps_with_requests = [gap for gap in coverage_gaps if gap.gapScore > 0.1][:10]
        if top_gaps_with_requests:
            recommendations.append(Recommendation(
                priority="high",
                action="Expand Warehouse Network in Underserved Areas",
                targetCities=[{"city": gap.city, "state": gap.state} for gap in top_gaps_with_requests],
                reasoning=f"Identified {len(coverage_gaps)} cities with 0 warehouses within this city. Prioritize cities with existing requests."
            ))
        
        if high_request_areas[:10]:
            recommendations.append(Recommendation(
                priority="medium",
                action="Focus on High Request Volume Areas",
                targetCities=[{"city": area.city, "state": area.state} for area in high_request_areas[:10]],
                reasoning=f"Top {len(high_request_areas[:10])} cities with highest request volumes."
            ))
        
        if gold_coverage_ratios[:10]:
            recommendations.append(Recommendation(
                priority="high",
                action="Prioritize Coverage Gaps with Low Gold Coverage",
                targetCities=[{"city": r["city"], "state": r["state"]} for r in gold_coverage_ratios[:10]],
                reasoning=f"Cities with worst request-to-gold ratios. Higher ratio = worse coverage quality."
            ))
        
        if data_quality_issues[:10]:
            recommendations.append(Recommendation(
                priority="low",
                action="Data Quality Issues",
                targetCities=[{"city": issue["city"], "state": issue["state"]} for issue in data_quality_issues[:10]],
                reasoning=f"Data cleanup needed: {len(data_quality_issues)} cities with high warehouse count but zero/low gold warehouses."
            ))
    
    return AIAnalysisData(
        coverageGaps=coverage_gaps,
        highRequestAreas=high_request_areas,
        requestTrends=request_trends,
        recommendations=recommendations
    )


async def analyze_coverage_gaps_without_ai(city_warehouses_dict: Dict[str, Dict], total_requests: int) -> AIAnalysisData:
    """Analyze coverage gaps using data analysis only (no AI).
    Uses same optimized logic as with_ai version but without AI recommendations.
    
    Args:
        city_warehouses_dict: Pre-grouped city data with warehouses (NO radius expansion)
        total_requests: Total requests across all warehouses
    """
    
    print("=== Starting data-based analysis (no AI) ===")
    
    # Load all US cities
    us_cities = load_us_cities()
    print(f"Loaded {len(us_cities)} US cities")
    
    # Get request counts by city from Requests table
    print("Fetching request counts by city from Requests table...")
    city_request_counts = await get_request_counts_by_city()
    print(f"Found requests in {len(city_request_counts)} cities")
    
    # Build set of cities that have warehouses (from Airtable)
    cities_with_warehouses = set(city_warehouses_dict.keys())
    print(f"Cities with warehouses: {len(cities_with_warehouses)}")
    
    # ========== COVERAGE GAPS (CODE ONLY) ==========
    # Identify cities with 0 warehouses from all US cities
    print("Calculating coverage gaps (cities with 0 warehouses)...")
    coverage_gaps = []
    
    for city_key, city_info in us_cities.items():
        # Skip if city has warehouses
        if city_key in cities_with_warehouses:
            continue
        
        # Get request count for this city
        request_count = city_request_counts.get(city_key, 0)
        
        # Calculate gap score: prioritize cities with requests
        # If city has requests but no warehouses, it's a high priority gap
        if request_count > 0:
            gap_score = min(request_count / 10.0, 1.0)  # Higher requests = higher score
        else:
            gap_score = 0.1  # Lower priority for cities without requests
        
        coverage_gaps.append(CoverageGap(
            city=city_info['city'],
            state=city_info['state'],
            latitude=city_info.get('latitude', 0.0),
            longitude=city_info.get('longitude', 0.0),
            zipcodes=city_info.get('zipcodes', []),
            warehouseCount=0,  # No warehouses
            gapScore=gap_score,
            requestCount=request_count  # Total requests for this city
        ))
    
    # Rank coverage gaps: cities with requests first, then by request volume
    coverage_gaps.sort(key=lambda x: (x.gapScore == 0.1, -x.gapScore))  # Requests first, then by score
    print(f"Found {len(coverage_gaps)} cities with 0 warehouses")
    
    # ========== HIGH REQUEST AREAS (CODE ONLY) ==========
    # Query Requests table directly, group by city, sort by volume
    print("Calculating high request areas from Requests table...")
    high_request_areas = []
    
    for city_key, request_count in city_request_counts.items():
        if request_count == 0:
            continue
        
        # Get city info
        city_info = us_cities.get(city_key, {})
        if not city_info:
            # If city not in US cities, try to get from warehouse data
            if city_key in city_warehouses_dict:
                data = city_warehouses_dict[city_key]
                city_info = {
                    "city": data["city"],
                    "state": data["state"],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "zipcodes": []
                }
            else:
                continue
        
        # Get warehouse count for this city
        warehouse_count = len(city_warehouses_dict.get(city_key, {}).get("warehouses", []))
        
        # Calculate coverage ratio
        ideal_warehouses = request_count / 3.0  # 1 warehouse per 3 requests
        coverage_ratio = warehouse_count / ideal_warehouses if ideal_warehouses > 0 else 0
        
        high_request_areas.append(HighRequestArea(
            city=city_info['city'],
            state=city_info['state'],
            latitude=city_info.get('latitude', 0.0),
            longitude=city_info.get('longitude', 0.0),
            zipcodes=city_info.get('zipcodes', []),
            requestCount=request_count,
            warehouseCount=warehouse_count,
            coverageRatio=round(coverage_ratio, 2)
        ))
    
    # Sort by request count (highest first)
    high_request_areas.sort(key=lambda x: x.requestCount, reverse=True)
    print(f"Found {len(high_request_areas)} high request areas")
    
    # ========== REQUEST TRENDS (CODE ONLY) ==========
    print("Calculating request trends...")
    request_trends = await get_request_trends(total_requests)
    
    # ========== CALCULATE GOLD COVERAGE RATIOS (CODE ONLY) ==========
    print("Calculating request-to-gold warehouse ratios...")
    gold_coverage_ratios = []
    
    for city_key, data in city_warehouses_dict.items():
        # Count gold warehouses (includes "Gold" and "Potential Gold")
        gold_count = sum(1 for w in data["warehouses"] if w.tier in ["Gold", "Potential Gold"])
        request_count = city_request_counts.get(city_key, 0)
        
        # Only consider cities with requests and warehouses
        if request_count > 0 and len(data["warehouses"]) > 0:
            # Calculate ratio: requests ÷ gold warehouses (higher = worse coverage)
            if gold_count > 0:
                ratio = request_count / gold_count
            else:
                # If no gold warehouses, use a high ratio to prioritize
                ratio = request_count * 10  # Penalize lack of gold warehouses
            
            gold_coverage_ratios.append({
                "city": data["city"],
                "state": data["state"],
                "city_key": city_key,
                "request_count": request_count,
                "gold_count": gold_count,
                "total_warehouses": len(data["warehouses"]),
                "ratio": ratio
            })
    
    # Sort by ratio (highest first) - worst ratios first
    gold_coverage_ratios.sort(key=lambda x: x["ratio"], reverse=True)
    print(f"Found {len(gold_coverage_ratios)} cities with request-to-gold ratios")
    
    # ========== DATA QUALITY ISSUES (CODE ONLY) ==========
    print("Identifying data quality issues...")
    data_quality_issues = []
    
    for city_key, data in city_warehouses_dict.items():
        total_warehouses = len(data["warehouses"])
        gold_count = sum(1 for w in data["warehouses"] if w.tier in ["Gold", "Potential Gold"])
        silver_count = sum(1 for w in data["warehouses"] if w.tier == "Silver")
        bronze_count = sum(1 for w in data["warehouses"] if w.tier == "Bronze")
        standard_tiers = ["Gold", "Potential Gold", "Silver", "Bronze"]
        un_tiered_count = sum(1 for w in data["warehouses"] 
                             if not w.tier or (isinstance(w.tier, str) and w.tier.strip() == "") 
                             or (w.tier not in standard_tiers))
        request_count = city_request_counts.get(city_key, 0)
        
        # Flag if: 10+ warehouses but 0-1 gold warehouses, OR 5+ warehouses with 0 gold and some requests
        if (total_warehouses >= 10 and gold_count <= 1) or (total_warehouses >= 5 and gold_count == 0 and request_count > 0):
            data_quality_issues.append({
                "city": data["city"],
                "state": data["state"],
                "city_key": city_key,
                "total_warehouses": total_warehouses,
                "gold_count": gold_count,
                "silver_count": silver_count,
                "bronze_count": bronze_count,
                "un_tiered_count": un_tiered_count,
                "total_requests": request_count
            })
    
    # Sort by warehouse count (highest first) to prioritize most critical issues
    data_quality_issues.sort(key=lambda x: x["total_warehouses"], reverse=True)
    print(f"Found {len(data_quality_issues)} data quality issues")
    
    # ========== RECOMMENDATIONS (DATA-BASED, NO AI) ==========
    recommendations = []
    
    # Recommendation 1: Expand Warehouse Network in Underserved Areas
    top_gaps_with_requests = [gap for gap in coverage_gaps if gap.gapScore > 0.1][:10]
    if top_gaps_with_requests:
        recommendations.append(Recommendation(
            priority="high",
            action="Expand Warehouse Network in Underserved Areas",
            targetCities=[{"city": gap.city, "state": gap.state} for gap in top_gaps_with_requests],
            reasoning=f"Identified {len(coverage_gaps)} cities with 0 warehouses within this city. Prioritize cities with existing requests."
        ))
    
    # Recommendation 2: Focus on High Request Volume Areas
    if high_request_areas[:10]:
        recommendations.append(Recommendation(
            priority="medium",
            action="Focus on High Request Volume Areas",
            targetCities=[{"city": area.city, "state": area.state} for area in high_request_areas[:10]],
            reasoning=f"Top {len(high_request_areas[:10])} cities with highest request volumes need attention."
        ))
    
    # Recommendation 3: Prioritize Coverage Gaps with Low Gold Coverage
    if gold_coverage_ratios[:10]:
        top_ratio = gold_coverage_ratios[0]
        recommendations.append(Recommendation(
            priority="high",
            action="Prioritize Coverage Gaps with Low Gold Coverage",
            targetCities=[{"city": r["city"], "state": r["state"]} for r in gold_coverage_ratios[:10]],
            reasoning=f"Cities with worst request-to-gold ratios. Higher ratio = worse coverage quality. Example: {top_ratio['request_count']} requests but only {top_ratio['gold_count']} gold warehouses = {top_ratio['ratio']:.1f}:1 ratio."
        ))
    
    # Recommendation 4: Data Quality Issues (last position)
    if data_quality_issues[:10]:
        top_issue = data_quality_issues[0]
        recommendations.append(Recommendation(
            priority="low",
            action="Data Quality Issues",
            targetCities=[{"city": issue["city"], "state": issue["state"]} for issue in data_quality_issues[:10]],
            reasoning=f"Data cleanup needed: {len(data_quality_issues)} cities with high warehouse count but zero/low gold warehouses. {top_issue['total_warehouses']} warehouses in {top_issue['city']}, {top_issue['state']} but only {top_issue['gold_count']} gold. Warehouses need tier evaluation."
        ))
    
    print(f"Data-based analysis results: {len(coverage_gaps)} gaps, {len(high_request_areas)} high-request areas, {len(recommendations)} recommendations")
    
    return AIAnalysisData(
        coverageGaps=coverage_gaps,
        highRequestAreas=high_request_areas,
        requestTrends=request_trends,
        recommendations=recommendations
    )


