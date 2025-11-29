

import re
from typing import List
from dotenv import load_dotenv
from fastapi import HTTPException
import httpx
import os

from warehouse.models import RequestData

load_dotenv()
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
ODER_TABLE_NAME = "Requests"

async def fetch_requests_from_airtable():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {}

    records = []
    async with httpx.AsyncClient() as client:
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            resp = await client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
    
    return records


async def fetch_request_by_id_from_airtable(request_id: int) -> List[RequestData]:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {
        "filterByFormula": f"{{Request ID}} = {request_id}",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

    records = data.get("records", [])

    result: RequestData = None
    for record in records:
        fields = record.get("fields", {})
        request_images: List[str] = []
        raw_images = fields.get("BOL & Pictures")

        if isinstance(raw_images, str):
            request_images = re.findall(r"\((https?://[^\)]+)\)", raw_images)
        elif isinstance(raw_images, list):
            for img in raw_images:
                if isinstance(img, dict) and "url" in img:
                    request_images.append(img["url"])

        result = RequestData(
                commodity=fields.get("Commodity"),
                loading_method=fields.get("Loading Style"),
                request_images=request_images
            )

    if not result:
        raise HTTPException(
                status_code=400,
                detail=f"Order with Request ID {request_id} not found"
            )   

    return result