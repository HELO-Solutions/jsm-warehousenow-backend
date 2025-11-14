
import math
import asyncio
from dotenv import load_dotenv
import requests
import httpx
import os
import googlemaps

load_dotenv()

MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8  # Radius of earth in miles
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

async def get_driving_distance_and_time_mapbox(origin_coords: tuple, dest_coords: tuple) -> dict:
    """
    Get driving distance (miles) and time (minutes) using Mapbox Directions API.
    origin_coords and dest_coords are tuples: (lat, lon)
    """
    origin_lon, origin_lat = origin_coords[1], origin_coords[0]
    dest_lon, dest_lat = dest_coords[1], dest_coords[0]
    url = f"https://api.mapbox.com/directions/v5/mapbox/driving/{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
    params = {
        "access_token": MAPBOX_TOKEN,
        "overview": "simplified",
        "geometries": "geojson"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data["routes"]:
                return None

            route = data["routes"][0]
            distance_miles = route["distance"] * 0.000621371  # meters → miles
            duration_minutes = route["duration"] / 60  # seconds → minutes

            return {
                "distance_miles": distance_miles,
                "duration_minutes": duration_minutes
            }
        except Exception as e:
            print(f"Error fetching driving data (Mapbox): {e}")
            return None


def get_coordinates_mapbox(zip_code: str):
    if not MAPBOX_TOKEN:
        raise ValueError("MAPBOX_TOKEN is missing. Please set it in your environment variables.")

    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{zip_code}.json"
    params = {
        "access_token": MAPBOX_TOKEN,
        "country": "US",  # Restrict to USA
        "limit": 1         # We only need the first match
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if not data.get("features"):
            return None

        # Mapbox returns coordinates as [lon, lat]
        lon, lat = data["features"][0]["center"]
        return lat, lon

    except Exception as e:
        print(f"Error fetching coordinates from Mapbox: {e}")
        return None

def get_coordinates_google(zip_code: str):
    """
    Get coordinates (lat, lon) for a given ZIP code using Google Maps Geocoding API.
    """
    if not GOOGLE_MAPS_API_KEY:
        raise ValueError("GOOGLE_MAPS_API_KEY is missing. Please set it in your environment variables.")

    try:
        geocode_result = gmaps.geocode(
            address=zip_code,
            components={"country": "US"}  # Restrict search to USA
        )

        if not geocode_result:
            return None

        location = geocode_result[0]['geometry']['location']
        lat, lon = location['lat'], location['lng']
        return lat, lon

    except Exception as e:
        print(f"Error fetching coordinates from Google Maps: {e}")
        return None

async def get_coordinates_google_async(zip_code: str):
    """
    Async version of get_coordinates_google using thread pool.
    """
    if not GOOGLE_MAPS_API_KEY:
        raise ValueError("GOOGLE_MAPS_API_KEY is missing. Please set it in your environment variables.")

    try:
        loop = asyncio.get_event_loop()
        geocode_result = await loop.run_in_executor(
            None, 
            lambda: gmaps.geocode(
                address=zip_code,
                components={"country": "US"}
            )
        )

        if not geocode_result:
            return None

        location = geocode_result[0]['geometry']['location']
        lat, lon = location['lat'], location['lng']
        return lat, lon

    except Exception as e:
        print(f"Error fetching coordinates from Google Maps: {e}")
        return None


async def get_driving_distance_and_time_google(origin_coords: tuple, dest_coords: tuple) -> dict:
    """
    Get driving distance (miles) and time (minutes) using Google Maps Directions API.
    origin_coords and dest_coords are tuples: (lat, lon)
    """
    try:
        loop = asyncio.get_event_loop()
        directions_result = await loop.run_in_executor(
            None,
            lambda: gmaps.directions(
                origin=origin_coords,
                destination=dest_coords,
                mode="driving"
            )
        )

        if not directions_result:
            return None

        leg = directions_result[0]['legs'][0]

        # Distance in meters → miles
        distance_meters = leg['distance']['value']
        distance_miles = distance_meters * 0.000621371

        # Duration in seconds → minutes
        duration_seconds = leg['duration']['value']
        duration_minutes = duration_seconds / 60

        return {
            "distance_miles": distance_miles,
            "duration_minutes": duration_minutes
        }
    except Exception as e:
        print(f"Error fetching driving data (Google Maps): {e}")
        return None

async def update_airtable_coordinates(record_id: str, latitude: float, longitude: float):
    """Update Airtable record with calculated coordinates."""
    try:
        airtable_token = os.getenv("AIRTABLE_TOKEN")
        base_id = os.getenv("BASE_ID")
        table_name = "Warehouses"
        
        url = f"https://api.airtable.com/v0/{base_id}/{table_name}/{record_id}"
        headers = {
            "Authorization": f"Bearer {airtable_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "fields": {
                "Latitude": latitude,
                "Longitude": longitude
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.patch(url, headers=headers, json=payload)
            response.raise_for_status()
            
        print(f"Updated coordinates for record {record_id}: {latitude}, {longitude}")
        return True
        
    except Exception as e:
        print(f"Failed to update coordinates for record {record_id}: {str(e)}")
        return False