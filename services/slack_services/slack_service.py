import os
from typing import List, Tuple
from fastapi import HTTPException
import requests

from warehouse.models import ChannelData, WarehouseData

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")               
SLACK_CANVAS_CREATE_URL = "https://slack.com/api/canvases.create"

# Column widths for table alignment
COLUMN_WIDTHS = {
    "Warehouse Name": 20,
    "Tier": 6,
    "Contact Name": 15,
    "Email": 25,
    "Phone": 15,
    "Website": 20,
    "Zip Searched": 12,
    "Radius": 6,
    "Assigned": 8,
    "Called?": 8,
    "Emailed?": 8,
    "Notes": 15
}


def pad(value: str, width: int) -> str:
    value = str(value) if value else ""
    if len(value) > width - 1:
        value = value[:width-1]
    return value.ljust(width)

def join_slack_channel(channel_id: str): 

    url = "https://slack.com/api/conversations.join" 
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"} 
    payload = {"channel": channel_id} 
    response = requests.post(url, headers=headers, json=payload) 
    data = response.json() 
    if not data.get("ok"):
     # If already in the channel, Slack may return "method_not_supported"
        if data.get("error") not in ("method_not_supported", "already_in_channel"): 
            raise Exception(f"Failed to join channel: {data}")
    
def build_combined_canvas_markdown(
    warehouses: List[WarehouseData],
    zip_searched: str,
    radius: str,
    has_header: bool
) -> str:
    """Build Markdown table with padded values for Slack Canvas."""
    headers = list(COLUMN_WIDTHS.keys())
    
    # Calculate the maximum width needed for each column
    # Start with the header width and the defined minimum width
    actual_widths = {}
    for h in headers:
        actual_widths[h] = max(COLUMN_WIDTHS[h], len(h) + 1)
    
    # Check all warehouse data to find the maximum width needed
    for w in warehouses:
        f = w.fields
        actual_widths["Warehouse Name"] = max(actual_widths["Warehouse Name"], len(str(f.warehouse_name or "")))
        actual_widths["Tier"] = max(actual_widths["Tier"], len(str(f.tier or "")))
        actual_widths["Contact Name"] = max(actual_widths["Contact Name"], len(str(f.contact_name or "")))
        actual_widths["Email"] = max(actual_widths["Email"], len(str(f.contact_email or "")))
        actual_widths["Phone"] = max(actual_widths["Phone"], len(str(f.office_phone or "")))
        actual_widths["Website"] = max(actual_widths["Website"], len(str(f.website or "")))
        actual_widths["Zip Searched"] = max(actual_widths["Zip Searched"], len(str(zip_searched or "")))
        actual_widths["Radius"] = max(actual_widths["Radius"], len(str(radius or "")))
    
    rows = []
    
    if has_header:
        # Include actual column headers
        header_row = "| " + " | ".join(pad(h, actual_widths[h]) for h in headers) + " |"
        separator_row = "|-" + "-|-".join("-" * actual_widths[h] for h in headers) + "-|"
        rows.extend([header_row, separator_row])
    else:
        # Include empty header row (invisible) but with separator to maintain table structure
        empty_header_row = "| " + " | ".join(pad("", actual_widths[h]) for h in headers) + " |"
        separator_row = "|-" + "-|-".join("-" * actual_widths[h] for h in headers) + "-|"
        rows.extend([empty_header_row, separator_row])

    # Add data rows (always included regardless of has_header)
    for w in warehouses:
        f = w.fields
        row = "| " + " | ".join([
            pad(f.warehouse_name, actual_widths["Warehouse Name"]),
            pad(f.tier, actual_widths["Tier"]),
            pad(f.contact_name, actual_widths["Contact Name"]),
            pad(f.contact_email, actual_widths["Email"]),
            pad(f.office_phone, actual_widths["Phone"]),
            pad(f.website, actual_widths["Website"]),
            pad(zip_searched, actual_widths["Zip Searched"]),
            pad(radius, actual_widths["Radius"]),
            pad("", actual_widths["Assigned"]),
            pad("", actual_widths["Called?"]),
            pad("", actual_widths["Emailed?"]),
            pad("", actual_widths["Notes"]),
        ]) + " |"
        rows.append(row)

    return "\n".join(rows)

def create_slack_canvas(channel_id: str, markdown_content: str, zip_searched: str, radius: str):
    payload = {
        "title": f"Search Results - Zip: {zip_searched}, Radius: {radius} miles\n\n",
        "channel_id": channel_id,
        "document_content": {"type": "markdown", "markdown": markdown_content}
    }

    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.post(SLACK_CANVAS_CREATE_URL, json=payload, headers=headers)
    if not response.ok:
        raise Exception(f"Slack Canvas API error: {response.text}")

    result = response.json()
    if not result.get("ok"):
        raise Exception(f"Slack Canvas API returned error: {result}")

    return result["canvas_id"]


MAX_CHUNK_SIZE = 1800  # Slack-friendly chunk size

def split_markdown(markdown: str, max_size: int = MAX_CHUNK_SIZE):
    chunks = []
    start = 0
    while start < len(markdown):
        end = start + max_size
        chunks.append(markdown[start:end])
        start = end
    return chunks


def append_to_slack_canvas(canvas_file_id: str, new_markdown: str, zip_searched: str, radius: str):
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8"
    }

    title = f"## Search Results - Zip: {zip_searched}, Radius: {radius} miles\n\n"
    full_content =  title + new_markdown

    # Use insert_at_end operation to append without fetching existing content
    payload = {
        "canvas_id": canvas_file_id,
        "changes": [
            {
                "operation": "insert_at_end",
                "document_content": {
                    "type": "markdown",
                    "markdown": full_content,
                }
            }
        ]
    }

    response = requests.post("https://slack.com/api/canvases.edit", headers=headers, json=payload)
    data = response.json()
    if not data.get("ok"):
        raise Exception(f"Slack Canvas update error: {data}")

    return {"ok": True, "message": "Canvas updated successfully."}


def get_channel_data_by_request(request_id: str) -> ChannelData:
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    target = str(request_id)
    channel_types = ["public_channel", "private_channel"]

    for ctype in channel_types:
        cursor = ""
        while True:
            response = requests.get(
                "https://slack.com/api/conversations.list",
                headers=headers,
                params={"limit": 1000, "types": ctype, "cursor": cursor}
            ).json()

            if not response.get("ok"):
                raise Exception(f"Error fetching {ctype}s: {response}")

            for ch in response.get("channels", []):
                channel_name = ch.get("name", "").lower()
                
                # Check if channel name contains the request_id
                if f"{target}" in channel_name or channel_name.startswith(f"{target}"):
                    canvas_id = None
                    file_id = None
                    tabs = ch.get("properties", {}).get("tabs", [])
                    if tabs:
                        canvas_id = tabs[0].get("id")
                        file_id = tabs[0].get("data", {}).get("file_id")
                    
                    return ChannelData(
                        channel_id=ch["id"],
                        channel_name=ch["name"],
                        canvas_id=canvas_id,
                        file_id=file_id
                    )

            cursor = response.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break

    raise HTTPException(
            status_code=400,
            detail=f"No Slack channel found for request_id={request_id}"
        )

def post_message_to_channel(channel_id: str, message: str, canvas_id: str = None):
    """Post a message to a Slack channel, optionally with a canvas link."""
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "channel": channel_id,
        "text": message
    }
    
    if canvas_id:
        payload["blocks"] = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            }
        ]
    
    response = requests.post(url, headers=headers, json=payload)
    data = response.json()
    
    if not data.get("ok"):
        raise Exception(f"Failed to post message to channel: {data}")
    
    return data

async def export_warehouse_results_to_slack(
    warehouses: List[WarehouseData],
    zip_searched: str,
    radius: str,
    request_id: str
):
    channel_data: ChannelData = get_channel_data_by_request(request_id)
    if not channel_data:
        raise HTTPException(
            status_code=400,
            detail=f"No Slack channel found for request_id={request_id}"
        )

    channel_id = channel_data.channel_id
    canvas_id = channel_data.canvas_id
    file_id = channel_data.file_id

    join_slack_channel(channel_id)

    new_table_markdown = build_combined_canvas_markdown(
        warehouses=warehouses,
        zip_searched=zip_searched,
        radius=radius,
        has_header= True #not (canvas_id and file_id)
    )

    if canvas_id and file_id:
        append_to_slack_canvas(
            file_id, 
            new_table_markdown,
            zip_searched,
            radius
            )
        message = f"*New warehouse search results added to Canvas.!*\n• Zip Code: {zip_searched}\n• Radius: {radius} miles\n Please review the records."
        post_message_to_channel(channel_id, message, file_id)
    else:
        canvas_id = create_slack_canvas(
            channel_id,
            new_table_markdown,
            zip_searched,
            radius
        )
        message = f"*New warehouse search results added to Canvas.!*\n• Zip Code: {zip_searched}\n• Radius: {radius} miles\n Please review the updated records."
        post_message_to_channel(channel_id, message, canvas_id)


    return canvas_id
