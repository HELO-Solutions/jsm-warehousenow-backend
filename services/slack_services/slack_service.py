import os
from typing import List, Tuple
import requests

from warehouse.models import ChannelData, WarehouseData


SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")               
SLACK_CANVAS_CREATE_URL = "https://slack.com/api/canvases.create"


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
    radius: str
) -> str:

    header = (
        "| Warehouse Name | Tier | Contact Name | Email | Phone | Website | Zip Searched | Radius | Assigned | Called? | Emailed? | Notes |\n"
        "|----------------|------|--------------|-------|-------|---------|--------------|--------|----------|---------|----------|-------|\n"
    )

    rows = []

    for w in warehouses:
        f = w.fields

        warehouse_name = f.warehouse_name or ""
        tier = f.tier or ""
        contact_name = f.contact_name or ""
        contact_email = f.contact_email or ""
        contact_phone = f.office_phone or ""
        website = f.website or ""

        row = (
            f"| {warehouse_name} "
            f"| {tier} "
            f"| {contact_name} "
            f"| {contact_email} "
            f"| {contact_phone} "
            f"| {website} "
            f"| {zip_searched} "
            f"| {radius} "
            f"|  "  # Assigned
            f"|  "  # Called?
            f"|  "  # Emailed?
            f"|  "  # Notes
            f"|"
        )

        rows.append(row)

    return header + "\n".join(rows)

def create_slack_canvas(channel_id: str, markdown_content: str, title="Warehouse Details"):
    payload = {
        "title": title,
        "channel_id": channel_id,
        "document_content": {
            "type": "markdown",
            "markdown": markdown_content
        }
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

def get_canvas_file_id_and_content(canvas_id: str) -> Tuple[str, str]:
    """Get the actual canvas file_id and content from canvas_id (which might be a tab ID)."""
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    existing_markdown = ""
    file_id = canvas_id
    
    # Try to get canvas info using files.info
    try:
        files_info_url = "https://slack.com/api/files.info"
        files_resp = requests.get(files_info_url, headers=headers, params={"file": canvas_id}).json()
        if files_resp.get("ok"):
            file_info = files_resp.get("file", {})
            file_id = file_info.get("id", canvas_id)
            # Try to get content from file
            if file_info.get("mimetype") == "application/vnd.slack-docs":
                # Canvas file - try to get content
                content = file_info.get("content", "")
                if content:
                    existing_markdown = content
    except Exception:
        pass
    
    # If canvas_id already starts with 'F', assume it's the correct file_id
    if canvas_id.startswith("F"):
        file_id = canvas_id
    
    return file_id, existing_markdown

def append_to_slack_canvas(canvas_file_id: str, new_markdown: str, existing_markdown: str = ""):
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }

    combined_markdown = existing_markdown + ("\n\n---\n\n" if existing_markdown else "") + new_markdown

    payload = {
        "canvas_id": canvas_file_id,
        "changes": [
            {
                "operation": "replace",
                "document_content": {
                    "type": "markdown",
                    "markdown": combined_markdown
                }
            }
        ]
    }

    response = requests.post("https://slack.com/api/canvases.edit", headers=headers, json=payload)
    data = response.json()
    if not data.get("ok"):
        raise Exception(f"Slack Canvas update error: {data}")
    
    return data

    
def get_channel_data_by_name(channel_name: str) -> ChannelData:

    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}"
    }

    target = channel_name.lower()
    channel_types = ["public_channel", "private_channel"]

    for ctype in channel_types:

        cursor = ""
        while True:
            response = requests.get(
                "https://slack.com/api/conversations.list",
                headers=headers,
                params={
                    "limit": 1000,
                    "types": ctype,
                    "cursor": cursor
                }
            ).json()

            if not response.get("ok"):
                raise Exception(f"Error fetching {ctype}s: {response}")

            # search through these channels
            for ch in response.get("channels", []):
                canvas_id = None
                file_id = None
                tabs = ch.get("properties", {}).get("tabs", [])
                if tabs:
                    canvas_id = tabs[0].get("id") 
                    file_id = tabs[0].get("data", {}).get("file_id")
                if ch.get("name", "").lower() == target:
                    channel_data = ChannelData(
                        channel_id=ch["id"],
                        channel_name=ch["name"],
                        canvas_id=canvas_id,
                        file_id=file_id
                    )
                    return channel_data

            # pagination check
            cursor = response.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break  # No more pages

    return None

def export_warehouse_results_to_slack(
    warehouses: List[WarehouseData],
    zip_searched: str,
    radius: str,
    channel_name: str
):
    channel_data: ChannelData = get_channel_data_by_name(channel_name)
    if not channel_data:
        raise Exception(f"Channel not found: {channel_name}")
    
    channel_id = channel_data.channel_id
    canvas_id = channel_data.canvas_id
    file_id = channel_data.file_id

    join_slack_channel(channel_id)

    new_table_markdown = build_combined_canvas_markdown(
        warehouses=warehouses,
        zip_searched=zip_searched,
        radius=radius
    )

    if canvas_id:
        append_to_slack_canvas(file_id, new_table_markdown)
    else:
        canvas_id = create_slack_canvas(channel_id, new_table_markdown, title=f"Warehouse Search Results ({len(warehouses)} results)")

    return canvas_id