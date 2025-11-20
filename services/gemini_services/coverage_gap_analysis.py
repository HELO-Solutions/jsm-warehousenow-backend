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
from services.geolocation.geolocation_service import haversine

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
    """Analyze coverage gaps using AI specifically for coverage gap analysis.
    
    Args:
        city_warehouses_dict: Pre-grouped city data with warehouses (includes radius expansion)
        total_requests: Total requests across all warehouses
        total_unique_warehouses: Total number of unique warehouses (for AI prompt)
    """
    
    # Check if GEMINI_API_KEY is available
    gemini_key = os.getenv("GEMINI_API_KEY")
    print(f"GEMINI_API_KEY available: {bool(gemini_key)}")
    if not gemini_key:
        print("GEMINI_API_KEY not found, using data-based analysis only")
        return await analyze_coverage_gaps_without_ai(city_warehouses_dict, total_requests)
    
    try:
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel("gemini-2.5-flash")
        
        # Prepare warehouse data for AI analysis (sample from grouped data)
        warehouse_summary = []
        sample_count = 0
        for city_key, data in city_warehouses_dict.items():
            for wh in data["warehouses"]:
                if sample_count >= 20:
                    break
                warehouse_summary.append({
                    "name": wh.name,
                    "city": wh.city,
                    "state": wh.state,
                    "zipCode": wh.zipCode,
                    "tier": wh.tier,
                    "requestCount": wh.reqCount,
                    "hazmat": wh.hazmat,
                    "disposal": wh.disposal,
                    "foodGrade": wh.foodGrade
                })
                sample_count += 1
            if sample_count >= 20:
                break
        
        # Create AI prompt for coverage gap analysis
        prompt = f"""
        You are a logistics analyst specializing in warehouse coverage optimization. 
        Analyze the following warehouse network data to identify coverage gaps and optimization opportunities.
        
        Total warehouses: {total_unique_warehouses}
        Total requests across all warehouses: {total_requests}
        
        Warehouse data (first 20 for analysis):
        {warehouse_summary[:20]}
        
        Please analyze and provide insights on:
        
        1. COVERAGE GAPS: Identify geographic areas (zip codes/cities) with high demand but insufficient warehouse coverage
        2. HIGH REQUEST AREAS: Areas with many requests relative to warehouse capacity
        3. REQUEST TRENDS: Analyze patterns in request distribution and growth
        4. RECOMMENDATIONS: Specific actions to improve coverage
        
        Focus on:
        - Geographic distribution patterns
        - Request density vs warehouse density
        - Service gaps in specific regions
        - Strategic expansion opportunities
        
        Provide specific zip codes, cities, and states where gaps exist.
        """
        
        # Get AI response
        ai_response = await model.generate_content_async(prompt)
        ai_text = ai_response.text.strip()
        
        # Load US cities data for coordinates and zipcodes
        us_cities = load_us_cities()
        
        # Use the pre-grouped city data directly (preserves radius expansion)
        city_warehouses = {}
        
        for city_key, data in city_warehouses_dict.items():
            # Extract zipcodes from warehouses in this city group
            zipcodes_set = set()
            for wh in data["warehouses"]:
                if wh.zipCode:
                    zipcodes_set.add(wh.zipCode)
            
            city_warehouses[city_key] = {
                "city": data["city"],
                "state": data["state"],
                "zipCodes": zipcodes_set,
                "warehouses": data["warehouses"],
                "totalRequests": data["totalRequests"]
            }
        
        # Coverage Gaps: Areas with less than 3 warehouses within 50 miles
        coverage_gaps = []
        
        for city_key, data in city_warehouses.items():
            # For each warehouse in this city, count how many warehouses are within 50 miles
            for warehouse in data["warehouses"]:
                nearby_count = 1  # Count itself
                
                # Count ALL warehouses within 50 miles (including those in the same city)
                for other_city_key, other_data in city_warehouses.items():
                    for other_warehouse in other_data["warehouses"]:
                        # Skip the warehouse itself
                        if warehouse.id == other_warehouse.id:
                            continue
                        
                        # Calculate distance using haversine formula
                        if warehouse.lat != 0 and warehouse.lng != 0 and other_warehouse.lat != 0 and other_warehouse.lng != 0:
                            distance = haversine(
                                warehouse.lat, warehouse.lng,
                                other_warehouse.lat, other_warehouse.lng
                            )
                            if distance <= 50:
                                nearby_count += 1
                
                # If less than 3 warehouses within 50 miles, this is a coverage gap
                if nearby_count < 3:
                    # Check if we already added this city
                    existing_gap = next((gap for gap in coverage_gaps if gap.city == data["city"] and gap.state == data["state"]), None)
                    if not existing_gap:
                        # Calculate minimum distance to nearest warehouse (including same city)
                        min_distance = float('inf')
                        for other_city_key, other_data in city_warehouses.items():
                            for other_warehouse in other_data["warehouses"]:
                                # Skip the warehouse itself
                                if warehouse.id == other_warehouse.id:
                                    continue
                                
                                if warehouse.lat != 0 and warehouse.lng != 0 and other_warehouse.lat != 0 and other_warehouse.lng != 0:
                                    distance = haversine(
                                        warehouse.lat, warehouse.lng,
                                        other_warehouse.lat, other_warehouse.lng
                                    )
                                    min_distance = min(min_distance, distance)
                        
                        if min_distance == float('inf'):
                            min_distance = 0.0
                        
                        # Gap score based on request density and warehouse shortage
                        requests = data["totalRequests"]
                        gap_score = min(requests / 10.0, 1.0) * (1.0 - (nearby_count / 3.0))
                        
                        # Get city info from US cities data
                        city_info = us_cities.get(city_key, {})
                        city_lat = city_info.get('latitude', 0.0) if city_info else 0.0
                        city_lng = city_info.get('longitude', 0.0) if city_info else 0.0
                        city_zipcodes = city_info.get('zipcodes', sorted(data["zipCodes"])) if city_info else sorted(data["zipCodes"])
                        
                        coverage_gaps.append(CoverageGap(
                            city=data["city"],
                            state=data["state"],
                            latitude=city_lat,
                            longitude=city_lng,
                            zipcodes=city_zipcodes if isinstance(city_zipcodes, list) else sorted(data["zipCodes"]),
                            warehouseCount=nearby_count,
                            minimumDistance=min_distance,
                            gapScore=max(gap_score, 0.0)
                        ))
                        break  # Only add once per city
        
        # High Request Areas: Cities with the most requests
        high_request_areas = []
        
        for city_key, data in city_warehouses.items():
            if data["totalRequests"] > 0:  # Only areas with requests
                # Calculate coverage ratio
                ideal_warehouses = data["totalRequests"] / 3.0  # 1 warehouse per 3 requests
                coverage_ratio = len(data["warehouses"]) / ideal_warehouses if ideal_warehouses > 0 else 0
                
                # Get city info from US cities data
                city_info = us_cities.get(city_key, {})
                city_lat = city_info.get('latitude', 0.0) if city_info else 0.0
                city_lng = city_info.get('longitude', 0.0) if city_info else 0.0
                city_zipcodes = city_info.get('zipcodes', sorted(data["zipCodes"])) if city_info else sorted(data["zipCodes"])
                
                high_request_areas.append(HighRequestArea(
                    city=data["city"],
                    state=data["state"],
                    latitude=city_lat,
                    longitude=city_lng,
                    zipcodes=city_zipcodes if isinstance(city_zipcodes, list) else sorted(data["zipCodes"]),
                    requestCount=data["totalRequests"],
                    warehouseCount=len(data["warehouses"]),
                    coverageRatio=round(coverage_ratio, 2)
                ))
        
        # Sort high request areas by request count (highest first)
        high_request_areas.sort(key=lambda x: x.requestCount, reverse=True)
        
        # Sort coverage gaps by gap score (highest first)
        coverage_gaps.sort(key=lambda x: x.gapScore, reverse=True)
        
        # Calculate REAL request trends based on actual historical data
        request_trends = await get_request_trends(total_requests)
        
        # Calculate gold warehouse counts per city for correlation insights
        city_gold_counts = {}
        city_metrics = {}
        for city_key, data in city_warehouses.items():
            gold_count = sum(1 for w in data["warehouses"] if w.tier in ["Gold", "Potential Gold"])
            silver_count = sum(1 for w in data["warehouses"] if w.tier == "Silver")
            bronze_count = sum(1 for w in data["warehouses"] if w.tier == "Bronze")
            total_warehouses = len(data["warehouses"])
            
            city_gold_counts[city_key] = gold_count
            city_metrics[city_key] = {
                "gold_count": gold_count,
                "silver_count": silver_count,
                "bronze_count": bronze_count,
                "total_warehouses": total_warehouses,
                "total_requests": data["totalRequests"],
                "city": data["city"],
                "state": data["state"],
                "zipCodes": data["zipCodes"]
            }
        
        # Create REAL recommendations based on actual analysis
        recommendations = []
        if coverage_gaps:
            # Get top 10 coverage gaps for recommendations
            top_gaps = coverage_gaps[:10]
            recommendations.append(Recommendation(
                priority="high",
                action="Expand warehouse network in underserved areas",
                targetCities=[{"city": gap.city, "state": gap.state} for gap in top_gaps],
                reasoning=f"Identified {len(coverage_gaps)} cities with less than 3 warehouses within 50 miles"
            ))
        
        if high_request_areas:
            # Get top 10 high request areas
            top_request_areas = high_request_areas[:10]
            recommendations.append(Recommendation(
                priority="medium",
                action="Focus on areas with highest request volume",
                targetCities=[{"city": area.city, "state": area.state} for area in top_request_areas],
                reasoning=f"Top 10 cities with {sum(area.requestCount for area in top_request_areas)} total requests"
            ))
        
        # 1. DATA QUALITY FLAGS: Identify cities with high warehouse count but zero/low gold warehouses
        data_quality_issues = []
        for city_key, metrics in city_metrics.items():
            total_warehouses = metrics["total_warehouses"]
            gold_count = metrics["gold_count"]
            total_requests = metrics["total_requests"]
            
            # Flag if: 10+ warehouses but 0-1 gold warehouses, OR 5+ warehouses with 0 gold and some requests
            if (total_warehouses >= 10 and gold_count <= 1) or (total_warehouses >= 5 and gold_count == 0 and total_requests > 0):
                data_quality_issues.append({
                    "city": metrics["city"],
                    "state": metrics["state"],
                    "total_warehouses": total_warehouses,
                    "gold_count": gold_count,
                    "total_requests": total_requests
                })
        
        # Sort by warehouse count (highest first) to prioritize most critical issues
        data_quality_issues.sort(key=lambda x: x["total_warehouses"], reverse=True)
        
        if data_quality_issues:
            # Get top 10 data quality issues
            top_quality_issues = data_quality_issues[:10]
            quality_cities = [{"city": issue["city"], "state": issue["state"]} for issue in top_quality_issues]
            
            # Build detailed reasoning with examples
            example_issues = []
            for issue in top_quality_issues[:3]:  # Show top 3 examples
                example_issues.append(
                    f"{issue['city']} {issue['state']}: "
                    f"{issue['total_warehouses']} warehouses, {issue['gold_count']} gold, "
                    f"{issue['total_requests']} requests"
                )
            
            reasoning = f"Data quality issue - {len(data_quality_issues)} cities with high warehouse count but zero/low gold warehouses. "
            reasoning += "Warehouses need tier evaluation. Examples: " + "; ".join(example_issues)
            
            recommendations.append(Recommendation(
                priority="medium",
                action="Data quality issue - warehouses need tier evaluation",
                targetCities=quality_cities,
                reasoning=reasoning
            ))
        
        # 2. HIGH REQUEST + LOW COVERAGE CORRELATION: Highlight cities with high request volume AND low gold warehouse count
        high_request_low_coverage = []
        for city_key, metrics in city_metrics.items():
            total_requests = metrics["total_requests"]
            gold_count = metrics["gold_count"]
            silver_count = metrics["silver_count"]
            
            # Flag if: 10+ requests but 0-2 gold warehouses (high demand, low quality coverage)
            if total_requests >= 10 and gold_count <= 2:
                high_request_low_coverage.append({
                    "city": metrics["city"],
                    "state": metrics["state"],
                    "total_requests": total_requests,
                    "gold_count": gold_count,
                    "silver_count": silver_count,
                    "total_warehouses": metrics["total_warehouses"]
                })
        
        # Sort by request count (highest first)
        high_request_low_coverage.sort(key=lambda x: x["total_requests"], reverse=True)
        
        if high_request_low_coverage:
            # Get top 10 priority coverage gaps
            top_coverage_gaps = high_request_low_coverage[:10]
            coverage_gap_cities = [{"city": gap["city"], "state": gap["state"]} for gap in top_coverage_gaps]
            
            # Build detailed reasoning with examples
            example_gaps = []
            for gap in top_coverage_gaps[:3]:  # Show top 3 examples
                tier_summary = f"{gap['gold_count']} gold"
                if gap['silver_count'] > 0:
                    tier_summary += f" + {gap['silver_count']} silver"
                example_gaps.append(
                    f"{gap['city']} {gap['state']}: "
                    f"{gap['total_requests']} requests, only {tier_summary}"
                )
            
            reasoning = f"Priority coverage gap: {len(high_request_low_coverage)} cities with high request volume AND low gold warehouse count. "
            reasoning += "Examples: " + "; ".join(example_gaps)
            
            recommendations.append(Recommendation(
                priority="high",
                action="Priority coverage gap: High requests with low gold warehouse coverage",
                targetCities=coverage_gap_cities,
                reasoning=reasoning
            ))
        
        # 3. REQUEST VOLUME VS GOLD AVAILABILITY: Show recommended cities based on request-to-gold ratio
        request_to_gold_ratios = []
        for city_key, metrics in city_metrics.items():
            total_requests = metrics["total_requests"]
            gold_count = metrics["gold_count"]
            
            # Only consider cities with requests and warehouses
            if total_requests > 0 and metrics["total_warehouses"] > 0:
                # Calculate request-to-gold ratio (higher = more requests per gold warehouse)
                if gold_count > 0:
                    ratio = total_requests / gold_count
                else:
                    # If no gold warehouses, use a high ratio to prioritize
                    ratio = total_requests * 10  # Penalize lack of gold warehouses
                
                request_to_gold_ratios.append({
                    "city": metrics["city"],
                    "state": metrics["state"],
                    "total_requests": total_requests,
                    "gold_count": gold_count,
                    "ratio": ratio,
                    "total_warehouses": metrics["total_warehouses"]
                })
        
        # Sort by ratio (highest first) - areas where requests significantly outpace gold availability
        request_to_gold_ratios.sort(key=lambda x: x["ratio"], reverse=True)
        
        if request_to_gold_ratios:
            # Get top 10 areas with highest request-to-gold ratio
            top_ratio_areas = request_to_gold_ratios[:10]
            ratio_cities = [{"city": area["city"], "state": area["state"]} for area in top_ratio_areas]
            
            # Calculate recommended additional gold warehouses
            recommended_gold_warehouses = []
            for area in top_ratio_areas[:5]:  # Show top 5 examples
                # Recommend 1 gold warehouse per 5-10 requests (depending on current ratio)
                if area["gold_count"] == 0:
                    recommended = max(1, area["total_requests"] // 5)
                else:
                    # If some gold exists, recommend based on ratio
                    ideal_ratio = 5  # 1 gold per 5 requests
                    current_requests_per_gold = area["total_requests"] / area["gold_count"] if area["gold_count"] > 0 else area["total_requests"]
                    recommended = max(0, int((current_requests_per_gold - ideal_ratio) / ideal_ratio * area["gold_count"]))
                    recommended = max(1, recommended)  # At least 1
                
                recommended_gold_warehouses.append(
                    f"{area['city']} {area['state']}: "
                    f"{area['total_requests']} requests, {area['gold_count']} gold → "
                    f"recommend {recommended} additional gold-tier warehouse(s)"
                )
            
            reasoning = f"Request volume vs gold availability: {len(request_to_gold_ratios)} cities where requests significantly outpace quality warehouse availability. "
            reasoning += "Consider recruiting additional gold-tier warehouses. Examples: " + "; ".join(recommended_gold_warehouses)
            
            recommendations.append(Recommendation(
                priority="high",
                action="Consider recruiting additional gold-tier warehouses in high-request areas",
                targetCities=ratio_cities,
                reasoning=reasoning
            ))
        
        return AIAnalysisData(
            coverageGaps=coverage_gaps,  # Full list, sorted by gap score
            highRequestAreas=high_request_areas,  # Full list, sorted by request count
            requestTrends=request_trends,
            recommendations=recommendations
        )
        
    except Exception as e:
        print(f"Error in AI coverage analysis: {e}")
        # Get fallback trends based on data
        fallback_trends = await get_request_trends(total_requests)
        # Return analysis based on data even if AI fails
        return AIAnalysisData(
            coverageGaps=coverage_gaps if 'coverage_gaps' in locals() else [],
            highRequestAreas=high_request_areas if 'high_request_areas' in locals() else [],
            requestTrends=fallback_trends,
            recommendations=recommendations if 'recommendations' in locals() else []
        )


async def analyze_coverage_gaps_without_ai(city_warehouses_dict: Dict[str, Dict], total_requests: int) -> AIAnalysisData:
    """Analyze coverage gaps using data analysis only (no AI).
    
    Args:
        city_warehouses_dict: Pre-grouped city data with warehouses (includes radius expansion)
        total_requests: Total requests across all warehouses
    """
    
    print(f"Running data-based analysis for {len(city_warehouses_dict)} city groups with {total_requests} total requests")
    
    # Load US cities data for coordinates and zipcodes
    us_cities = load_us_cities()
    
    # Use the pre-grouped city data directly (preserves radius expansion)
    city_warehouses = {}
    
    for city_key, data in city_warehouses_dict.items():
        # Extract zipcodes from warehouses in this city group
        zipcodes_set = set()
        for wh in data["warehouses"]:
            if wh.zipCode:
                zipcodes_set.add(wh.zipCode)
        
        city_warehouses[city_key] = {
            "city": data["city"],
            "state": data["state"],
            "zipCodes": zipcodes_set,
            "warehouses": data["warehouses"],
            "totalRequests": data["totalRequests"]
        }
    
    # Coverage Gaps: Areas with less than 3 warehouses within 50 miles
    coverage_gaps = []
    
    for city_key, data in city_warehouses.items():
        # For each warehouse in this city, count how many warehouses are within 50 miles
        for warehouse in data["warehouses"]:
            nearby_count = 1  # Count itself
            
            # Count ALL warehouses within 50 miles (including those in the same city)
            for other_city_key, other_data in city_warehouses.items():
                for other_warehouse in other_data["warehouses"]:
                    # Skip the warehouse itself
                    if warehouse.id == other_warehouse.id:
                        continue
                    
                    # Calculate distance using haversine formula
                    if warehouse.lat != 0 and warehouse.lng != 0 and other_warehouse.lat != 0 and other_warehouse.lng != 0:
                        distance = haversine(
                            warehouse.lat, warehouse.lng,
                            other_warehouse.lat, other_warehouse.lng
                        )
                        if distance <= 50:
                            nearby_count += 1
            
            # If less than 3 warehouses within 50 miles, this is a coverage gap
            if nearby_count < 3:
                # Check if we already added this city
                existing_gap = next((gap for gap in coverage_gaps if gap.city == data["city"] and gap.state == data["state"]), None)
                if not existing_gap:
                    # Calculate minimum distance to nearest warehouse (including same city)
                    min_distance = float('inf')
                    for other_city_key, other_data in city_warehouses.items():
                        for other_warehouse in other_data["warehouses"]:
                            # Skip the warehouse itself
                            if warehouse.id == other_warehouse.id:
                                continue
                            
                            if warehouse.lat != 0 and warehouse.lng != 0 and other_warehouse.lat != 0 and other_warehouse.lng != 0:
                                distance = haversine(
                                    warehouse.lat, warehouse.lng,
                                    other_warehouse.lat, other_warehouse.lng
                                )
                                min_distance = min(min_distance, distance)
                    
                    if min_distance == float('inf'):
                        min_distance = 0.0
                    
                    # Gap score based on request density and warehouse shortage
                    requests = data["totalRequests"]
                    gap_score = min(requests / 10.0, 1.0) * (1.0 - (nearby_count / 3.0))
                    
                    # Get city info from US cities data
                    city_info = us_cities.get(city_key, {})
                    city_lat = city_info.get('latitude', 0.0) if city_info else 0.0
                    city_lng = city_info.get('longitude', 0.0) if city_info else 0.0
                    city_zipcodes = city_info.get('zipcodes', sorted(data["zipCodes"])) if city_info else sorted(data["zipCodes"])
                    
                    coverage_gaps.append(CoverageGap(
                        city=data["city"],
                        state=data["state"],
                        latitude=city_lat,
                        longitude=city_lng,
                        zipcodes=city_zipcodes if isinstance(city_zipcodes, list) else sorted(data["zipCodes"]),
                        warehouseCount=nearby_count,
                        minimumDistance=min_distance,
                        gapScore=max(gap_score, 0.0)
                    ))
                    break  # Only add once per city
    
    # High Request Areas: Cities with the most requests
    high_request_areas = []
    
    for city_key, data in city_warehouses.items():
        if data["totalRequests"] > 0:  # Only areas with requests
            # Calculate coverage ratio
            ideal_warehouses = data["totalRequests"] / 3.0  # 1 warehouse per 3 requests
            coverage_ratio = len(data["warehouses"]) / ideal_warehouses if ideal_warehouses > 0 else 0
            
            # Get city info from US cities data
            city_info = us_cities.get(city_key, {})
            city_lat = city_info.get('latitude', 0.0) if city_info else 0.0
            city_lng = city_info.get('longitude', 0.0) if city_info else 0.0
            city_zipcodes = city_info.get('zipcodes', sorted(data["zipCodes"])) if city_info else sorted(data["zipCodes"])
            
            high_request_areas.append(HighRequestArea(
                city=data["city"],
                state=data["state"],
                latitude=city_lat,
                longitude=city_lng,
                zipcodes=city_zipcodes if isinstance(city_zipcodes, list) else sorted(data["zipCodes"]),
                requestCount=data["totalRequests"],
                warehouseCount=len(data["warehouses"]),
                coverageRatio=round(coverage_ratio, 2)
            ))
    
    # Sort high request areas by request count (highest first)
    high_request_areas.sort(key=lambda x: x.requestCount, reverse=True)
    
    # Sort coverage gaps by gap score (highest first)
    coverage_gaps.sort(key=lambda x: x.gapScore, reverse=True)
    
    # Calculate REAL request trends based on actual historical data
    request_trends = await get_request_trends(total_requests)
    
    # Calculate gold warehouse counts per city for correlation insights
    city_metrics = {}
    for city_key, data in city_warehouses.items():
        gold_count = sum(1 for w in data["warehouses"] if w.tier in ["Gold", "Potential Gold"])
        silver_count = sum(1 for w in data["warehouses"] if w.tier == "Silver")
        bronze_count = sum(1 for w in data["warehouses"] if w.tier == "Bronze")
        total_warehouses = len(data["warehouses"])
        
        city_metrics[city_key] = {
            "gold_count": gold_count,
            "silver_count": silver_count,
            "bronze_count": bronze_count,
            "total_warehouses": total_warehouses,
            "total_requests": data["totalRequests"],
            "city": data["city"],
            "state": data["state"],
            "zipCodes": data["zipCodes"]
        }
    
    # Create REAL recommendations based on actual analysis
    recommendations = []
    if coverage_gaps:
        # Get top 10 coverage gaps for recommendations
        top_gaps = coverage_gaps[:10]
        recommendations.append(Recommendation(
            priority="high",
            action="Expand warehouse network in underserved areas",
            targetCities=[{"city": gap.city, "state": gap.state} for gap in top_gaps],
            reasoning=f"Identified {len(coverage_gaps)} cities with less than 3 warehouses within 50 miles"
        ))
    
    if high_request_areas:
        # Get top 10 high request areas
        top_request_areas = high_request_areas[:10]
        recommendations.append(Recommendation(
            priority="medium",
            action="Focus on areas with highest request volume",
            targetCities=[{"city": area.city, "state": area.state} for area in top_request_areas],
            reasoning=f"Top 10 cities with {sum(area.requestCount for area in top_request_areas)} total requests"
        ))
    
    # 1. DATA QUALITY FLAGS: Identify cities with high warehouse count but zero/low gold warehouses
    data_quality_issues = []
    for city_key, metrics in city_metrics.items():
        total_warehouses = metrics["total_warehouses"]
        gold_count = metrics["gold_count"]
        total_requests = metrics["total_requests"]
        
        # Flag if: 10+ warehouses but 0-1 gold warehouses, OR 5+ warehouses with 0 gold and some requests
        if (total_warehouses >= 10 and gold_count <= 1) or (total_warehouses >= 5 and gold_count == 0 and total_requests > 0):
            data_quality_issues.append({
                "city": metrics["city"],
                "state": metrics["state"],
                "total_warehouses": total_warehouses,
                "gold_count": gold_count,
                "total_requests": total_requests
            })
    
    # Sort by warehouse count (highest first) to prioritize most critical issues
    data_quality_issues.sort(key=lambda x: x["total_warehouses"], reverse=True)
    
    if data_quality_issues:
        # Get top 10 data quality issues
        top_quality_issues = data_quality_issues[:10]
        quality_cities = [{"city": issue["city"], "state": issue["state"]} for issue in top_quality_issues]
        
        # Build detailed reasoning with examples
        example_issues = []
        for issue in top_quality_issues[:3]:  # Show top 3 examples
            example_issues.append(
                f"{issue['city']} {issue['state']}: "
                f"{issue['total_warehouses']} warehouses, {issue['gold_count']} gold, "
                f"{issue['total_requests']} requests"
            )
        
        reasoning = f"Data quality issue - {len(data_quality_issues)} cities with high warehouse count but zero/low gold warehouses. "
        reasoning += "Warehouses need tier evaluation. Examples: " + "; ".join(example_issues)
        
        recommendations.append(Recommendation(
            priority="medium",
            action="Data quality issue - warehouses need tier evaluation",
            targetCities=quality_cities,
            reasoning=reasoning
        ))
    
    # 2. HIGH REQUEST + LOW COVERAGE CORRELATION: Highlight cities with high request volume AND low gold warehouse count
    high_request_low_coverage = []
    for city_key, metrics in city_metrics.items():
        total_requests = metrics["total_requests"]
        gold_count = metrics["gold_count"]
        silver_count = metrics["silver_count"]
        
        # Flag if: 10+ requests but 0-2 gold warehouses (high demand, low quality coverage)
        if total_requests >= 10 and gold_count <= 2:
            high_request_low_coverage.append({
                "city": metrics["city"],
                "state": metrics["state"],
                "total_requests": total_requests,
                "gold_count": gold_count,
                "silver_count": silver_count,
                "total_warehouses": metrics["total_warehouses"]
            })
    
    # Sort by request count (highest first)
    high_request_low_coverage.sort(key=lambda x: x["total_requests"], reverse=True)
    
    if high_request_low_coverage:
        # Get top 10 priority coverage gaps
        top_coverage_gaps = high_request_low_coverage[:10]
        coverage_gap_cities = [{"city": gap["city"], "state": gap["state"]} for gap in top_coverage_gaps]
        
        # Build detailed reasoning with examples
        example_gaps = []
        for gap in top_coverage_gaps[:3]:  # Show top 3 examples
            tier_summary = f"{gap['gold_count']} gold"
            if gap['silver_count'] > 0:
                tier_summary += f" + {gap['silver_count']} silver"
            example_gaps.append(
                f"{gap['city']} {gap['state']}: "
                f"{gap['total_requests']} requests, only {tier_summary}"
            )
        
        reasoning = f"Priority coverage gap: {len(high_request_low_coverage)} cities with high request volume AND low gold warehouse count. "
        reasoning += "Examples: " + "; ".join(example_gaps)
        
        recommendations.append(Recommendation(
            priority="high",
            action="Priority coverage gap: High requests with low gold warehouse coverage",
            targetCities=coverage_gap_cities,
            reasoning=reasoning
        ))
    
        # 3. REQUEST VOLUME VS GOLD AVAILABILITY: Show recommended cities based on request-to-gold ratio
        request_to_gold_ratios = []
        for city_key, metrics in city_metrics.items():
            total_requests = metrics["total_requests"]
            gold_count = metrics["gold_count"]
            
            # Only consider cities with requests and warehouses
            if total_requests > 0 and metrics["total_warehouses"] > 0:
                # Calculate request-to-gold ratio (higher = more requests per gold warehouse)
                if gold_count > 0:
                    ratio = total_requests / gold_count
                else:
                    # If no gold warehouses, use a high ratio to prioritize
                    ratio = total_requests * 10  # Penalize lack of gold warehouses
                
                request_to_gold_ratios.append({
                    "city": metrics["city"],
                    "state": metrics["state"],
                    "total_requests": total_requests,
                    "gold_count": gold_count,
                    "ratio": ratio,
                    "total_warehouses": metrics["total_warehouses"]
                })
    
    # Sort by ratio (highest first) - areas where requests significantly outpace gold availability
    request_to_gold_ratios.sort(key=lambda x: x["ratio"], reverse=True)
    
    if request_to_gold_ratios:
        # Get top 10 areas with highest request-to-gold ratio
        top_ratio_areas = request_to_gold_ratios[:10]
        ratio_cities = [{"city": area["city"], "state": area["state"]} for area in top_ratio_areas]
        
        # Calculate recommended additional gold warehouses
        recommended_gold_warehouses = []
        for area in top_ratio_areas[:5]:  # Show top 5 examples
            # Recommend 1 gold warehouse per 5-10 requests (depending on current ratio)
            if area["gold_count"] == 0:
                recommended = max(1, area["total_requests"] // 5)
            else:
                # If some gold exists, recommend based on ratio
                ideal_ratio = 5  # 1 gold per 5 requests
                current_requests_per_gold = area["total_requests"] / area["gold_count"] if area["gold_count"] > 0 else area["total_requests"]
                recommended = max(0, int((current_requests_per_gold - ideal_ratio) / ideal_ratio * area["gold_count"]))
                recommended = max(1, recommended)  # At least 1
            
            recommended_gold_warehouses.append(
                f"{area['city']} {area['state']}: "
                f"{area['total_requests']} requests, {area['gold_count']} gold → "
                f"recommend {recommended} additional gold-tier warehouse(s)"
            )
        
        reasoning = f"Request volume vs gold availability: {len(request_to_gold_ratios)} cities where requests significantly outpace quality warehouse availability. "
        reasoning += "Consider recruiting additional gold-tier warehouses. Examples: " + "; ".join(recommended_gold_warehouses)
        
        recommendations.append(Recommendation(
            priority="high",
            action="Consider recruiting additional gold-tier warehouses in high-request areas",
            targetCities=ratio_cities,
            reasoning=reasoning
        ))
    
    print(f"Data-based analysis results: {len(coverage_gaps)} gaps, {len(high_request_areas)} high-request areas, {len(recommendations)} recommendations")
    
    return AIAnalysisData(
        coverageGaps=coverage_gaps,  # Full list, sorted by gap score
        highRequestAreas=high_request_areas,  # Full list, sorted by request count
        requestTrends=request_trends,
        recommendations=recommendations
    )


