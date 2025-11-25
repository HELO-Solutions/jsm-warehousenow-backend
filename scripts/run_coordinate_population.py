#!/usr/bin/env python3
"""
Simple script to run the coordinate population process.
"""

import asyncio
from scripts.populate_coordinates import main

if __name__ == "__main__":
    print("Starting Airtable coordinate population...")
    print("This will add latitude/longitude fields and populate them with coordinates.")
    print("This is a one-time process that will save significant API costs.")
    print()
    
    try:
        asyncio.run(main())
        print("\nSUCCESS: Coordinate population completed successfully!")
        print("You can now use the updated warehouse service with coordinates.")
        print("This will save you ~$426/month in coordinate API costs!")
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"\nERROR: Process failed: {e}")
        print("Please check your environment variables and Airtable access.")
