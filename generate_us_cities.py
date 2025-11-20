"""
Script to generate a JSON file with all US cities grouped from zipcodes.json.
Groups zipcodes by city+state and calculates average coordinates.
"""

import json
from collections import defaultdict

def generate_us_cities():
    
    print("Loading zipcodes.json...")
    with open('data/zipcodes.json', 'r', encoding='utf-8') as f:
        zipcodes_data = json.load(f)
    
    print(f"Processing {len(zipcodes_data)} zipcode entries...")
    
    cities_dict = {}
    
    for entry in zipcodes_data:
        city = entry.get('city', '').strip()
        state = entry.get('state', '').strip()
        zip_code = entry.get('zip_code')
        lat = entry.get('latitude')
        lng = entry.get('longitude')
        
        if not city or not state:
            continue
        
  
        city_key = f"{city},{state}"
        
        if city_key not in cities_dict:
            cities_dict[city_key] = {
                "city": city,
                "state": state,
                "zipcodes": [],
                "coordinates": []  
            }
        

        if zip_code:
            zip_str = str(zip_code).zfill(5)  
            if zip_str not in cities_dict[city_key]["zipcodes"]:
                cities_dict[city_key]["zipcodes"].append(zip_str)
        
 
        if lat is not None and lng is not None:
            try:
                lat_float = float(lat)
                lng_float = float(lng)
                cities_dict[city_key]["coordinates"].append((lat_float, lng_float))
            except (ValueError, TypeError):
                continue
    
    print(f"Found {len(cities_dict)} unique cities")
    
  
    cities_list = []
    for city_key, data in cities_dict.items():

        if data["coordinates"]:
            avg_lat = sum(coord[0] for coord in data["coordinates"]) / len(data["coordinates"])
            avg_lng = sum(coord[1] for coord in data["coordinates"]) / len(data["coordinates"])
        else:
            avg_lat = 0.0
            avg_lng = 0.0
        
        cities_list.append({
            "city": data["city"],
            "state": data["state"],
            "latitude": round(avg_lat, 6),
            "longitude": round(avg_lng, 6),
            "zipcodes": sorted(data["zipcodes"])  # Sort zipcodes alphabetically
        })
    
    # Sort by state, then city
    cities_list.sort(key=lambda x: (x["state"], x["city"]))
    
    # Create output structure
    output = {
        "total_cities": len(cities_list),
        "cities": cities_list
    }
    
    # Save to file
    output_file = 'data/us_cities.json'
    print(f"Saving to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Successfully generated {output_file}")
    print(f"  Total cities: {len(cities_list)}")
    print(f"  Sample cities: {cities_list[:3]}")
    
    return output

if __name__ == "__main__":
    generate_us_cities()

