import os
from typing import List, Tuple
from fastapi import HTTPException
import requests

from warehouse.models import ChannelData, ExportWarehouseData, WarehouseData

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")               
SLACK_CANVAS_CREATE_URL = "https://slack.com/api/canvases.create"

# Column widths for table alignment
COLUMN_WIDTHS = {
    "ID": 20,
    "Tier": 6,
    "Name": 15,
    "Email": 25,
    "Phone": 15,
    "Miles Away": 6,
    "Assigned To": 8,
    "Called?": 8,
    "Notes": 15
}


def pad(value: str, width: int) -> str:
    value = str(value) if value else ""
    if len(value) > width - 1:
        value = value[:width-1]
    return value.ljust(width)

def is_bot_in_channel(channel_id: str) -> bool:
    """Check if bot is already a member of the channel."""
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    response = requests.get(
        "https://slack.com/api/conversations.info",
        headers=headers,
        params={"channel": channel_id}
    ).json()
    
    if response.get("ok"):
        return response.get("channel", {}).get("is_member", False)
    return False

def is_bot_in_channel(channel_id: str) -> bool:
    """Check if bot is already a member of the channel."""
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    response = requests.get(
        "https://slack.com/api/conversations.info",
        headers=headers,
        params={"channel": channel_id}
    ).json()
    
    if response.get("ok"):
        return response.get("channel", {}).get("is_member", False)
    return False

def join_slack_channel(channel_id: str): 
    # First check if we're already in the channel
    if is_bot_in_channel(channel_id):
        return
    
    # Try to join (works for public channels only)
    url = "https://slack.com/api/conversations.join" 
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"} 
    payload = {"channel": channel_id} 
    response = requests.post(url, headers=headers, json=payload) 
    data = response.json() 
    
    if not data.get("ok"):
        error = data.get("error", "")
        acceptable_errors = [
            "method_not_supported", 
            "already_in_channel", 
            "is_archived",
            "method_not_supported_for_channel_type"
        ]
        
        if error == "method_not_supported_for_channel_type":
            # This is a private channel - check if we're a member
            if not is_bot_in_channel(channel_id):
                raise HTTPException(
                    status_code=403,
                    detail=f"Bot is not a member of this private channel. Please invite the bot to the channel using '/invite @BotName'"
                )
        elif error not in acceptable_errors:
            raise Exception(f"Failed to join channel: {data}")
    
def build_combined_canvas_markdown(
    warehouses: List[ExportWarehouseData],
) -> str:
    """Build Markdown table with padded values for Slack Canvas."""
    # Updated column order and widths
    COLUMN_WIDTHS_ORDERED = {
        "ID": 20,
        "Called?": 20,
        "Assigned To": 20,
        "Tier": 20,
        "Name": 20,
        "Phone": 20,
        "Email": 25,
        "Miles Away": 20,
        "Notes": 20
    }
    
    headers = list(COLUMN_WIDTHS_ORDERED.keys())
    
    # Calculate the maximum width needed for each column
    actual_widths = {}
    for h in headers:
        actual_widths[h] = max(COLUMN_WIDTHS_ORDERED[h], len(h) + 1)
    
    # Check all warehouse data to find the maximum width needed
    for w in warehouses:
        f = w.fields
        # Safely concatenate phone numbers, filtering out None values
        phone_numbers = [
            f.office_phone,
            f.cell_phone,
            f.contact_2_cell_phone,
            f.contact_2_office_phone,
            f.contact_3_cell_phone
        ]
        # Filter out None values and join with commas
        phone_number = ", ".join([p for p in phone_numbers if p])
        
        actual_widths["ID"] = max(actual_widths["ID"], len(str(w.warehouse_id or "")))
        actual_widths["Tier"] = max(actual_widths["Tier"], len(str(f.tier or "")))
        actual_widths["Name"] = max(actual_widths["Name"], len(str(f.contact_name or "")))
        actual_widths["Phone"] = max(actual_widths["Phone"], len(phone_number))
        actual_widths["Email"] = max(actual_widths["Email"], len(str(f.contact_email or "")))
        actual_widths["Miles Away"] = max(actual_widths["Miles Away"], len(str(w.distance_miles or "")))
    
    rows = []
    
    header_row = "| " + " | ".join(pad(h, actual_widths[h]) for h in headers) + " |"
    separator_row = "|-" + "-|-".join("-" * actual_widths[h] for h in headers) + "-|"
    rows.extend([header_row, separator_row])

    # Add data rows with correct column order
    for w in warehouses:
        f = w.fields
        phone_numbers = [
            f.office_phone,
            f.cell_phone,
            f.contact_2_cell_phone,
            f.contact_2_office_phone,
            f.contact_3_cell_phone
        ]
        phone_number = ", ".join([p for p in phone_numbers if p])
        
        row = "| " + " | ".join([
            pad(str(w.warehouse_id), actual_widths["ID"]),
            pad("", actual_widths["Called?"]),
            pad("", actual_widths["Assigned To"]),
            pad(f.tier or "", actual_widths["Tier"]),
            pad(f.contact_name or "", actual_widths["Name"]),
            pad(phone_number, actual_widths["Phone"]),
            pad(f.contact_email or "", actual_widths["Email"]),
            pad(str("%.2f" % w.distance_miles) if w.distance_miles else "", actual_widths["Miles Away"]),
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
    target = str(request_id).lower() 
    
    found_private_channel_without_bot = False
    private_channel_name = None
    all_channels = []
    
    # Fetch public channels
    cursor = ""
    while True:
        response = requests.get(
            "https://slack.com/api/conversations.list",
            headers=headers,
            params={
                "limit": 1000, 
                "types": "public_channel",
                "cursor": cursor,
                "exclude_archived": True 
            }
        ).json()

        if response.get("ok"):
            all_channels.extend(response.get("channels", []))
            cursor = response.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
        else:
            print(f"Error fetching public channels: {response}")
            break
    
    # Fetch private channels (only those bot is a member of)
    cursor = ""
    while True:
        response = requests.get(
            "https://slack.com/api/conversations.list",
            headers=headers,
            params={
                "limit": 1000, 
                "types": "private_channel",
                "cursor": cursor,
                "exclude_archived": True 
            }
        ).json()

        if response.get("ok"):
            all_channels.extend(response.get("channels", []))
            cursor = response.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
        else:
            error = response.get("error", "")
            if error in ("missing_scope", "not_authed"):
                print(f"Warning: Missing 'groups:read' scope for private channels. Bot can only see private channels it's a member of.")
            break
    
    # Now search through all collected channels
    for ch in all_channels:
        channel_name = ch.get("name", "").lower()
        is_private = ch.get("is_private", False)
        is_member = ch.get("is_member", False)
        
        # Check if channel name starts with target and hyphen/underscore
        if channel_name.startswith(f"{target}-"):
            # Found a matching channel
            if is_private and not is_member:
                continue 
            
            # Bot is a member or it's public
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

    else:
        raise HTTPException(
            status_code=404,
            detail=f"No Slack channel found for request_id={request_id}', Please invite the Bot if the channel exits and is private."
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
    warehouses: List[ExportWarehouseData],
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
