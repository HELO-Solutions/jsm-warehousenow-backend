


import base64
import logging
import mimetypes
import re
import uuid

import aiohttp
from warehouse.models import SendBulkEmailData, SendEmailData
import aiosmtplib
from email.message import EmailMessage
from warehouse.models import SendEmailData
import os
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST")    
SMTP_PORT = os.getenv("SMTP_PORT")
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

async def send_bulk_email(send_bulk_emails: SendBulkEmailData):
    results = []
    unique_email_data = remove_duplicate_emails(send_bulk_emails.emails_data)
    for email_data in unique_email_data:
        body = send_bulk_emails.email_body
        image_paths_or_urls = send_bulk_emails.images
        result = await send_email(body, email_data, image_paths_or_urls)
        results.append(result)
    return results

def remove_duplicate_emails(data):
    seen = set()
    unique = []
    for item in data:
        # Normalize email: strip spaces and lower-case
        key = (item.email or "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
    return unique

async def send_email(
    email_body: str,
    email_data: SendEmailData,
    image_paths_or_urls: list[str] = None,
):
    """Send a single email with optional attachments (local, URL, or Base64)."""

    # --- subject ---
    #if not email_data.services:
    #    services_str = "general warehousing services"
    #elif len(email_data.services) == 1:
    #    services_str = email_data.services[0]
    #else:
    #    services_str = ", ".join(email_data.services[:-1]) + f" and {email_data.services[-1]}"
    #subject = email_subject or f"Request for {services_str} near {email_data.adress}"

    # --- body ---
    if not email_body:
        email_body = """\
        <html>
          <body>
            <p>Hi,</p>
            <p>We have a request and would like to know if you can help with this.</p>
            <p>Please provide pricing based on the following info below:</p>
            <p>
              <b>Commodity:</b><br>
              <b>Loading method:</b> palletized / slip sheets / floor loaded
            </p>
            <p>Pictures attached below:</p>
            <p>Best,<br>
            WarehouseNow Team</p>
          </body>
        </html>
        """

    message = EmailMessage()
    message["From"] = f"WarehouseNow <{SMTP_USER}>"
    message["To"] = email_data.email
    message["Subject"] = f"{email_data.email_subject}"

    message.set_content("Please view this email in HTML mode.")
    message.add_alternative(email_body, subtype="html")

    # --- Attachments ---
    if image_paths_or_urls:
        async with aiohttp.ClientSession() as session:
            for item in image_paths_or_urls:
                try:
                    # Handle Base64 data URLs (from frontend uploads)
                    if item.startswith("data:"):
                        # Extract MIME type and data
                        match = re.match(r'data:([^;]+);base64,(.+)', item)
                        if match:
                            mime_type = match.group(1)
                            base64_data = match.group(2)
                            
                            # Decode Base64 data
                            file_data = base64.b64decode(base64_data)
                            
                            # Extract filename and subtype
                            maintype, subtype = mime_type.split("/", 1)
                            filename = f"attachment.{subtype}"
                            
                            logging.info(f"Attaching Base64 image ({mime_type}), {len(file_data)} bytes")
                            
                            message.add_attachment(
                                file_data,
                                maintype=maintype,
                                subtype=subtype,
                                filename=filename,
                            )
                        else:
                            logging.warning(f"Invalid Base64 data URL format: {item[:50]}...")
                    
                    # Handle HTTP/HTTPS URLs
                    elif item.startswith("http://") or item.startswith("https://"):
                        async with session.get(item) as resp:
                            if resp.status != 200:
                                raise ValueError(f"Failed to download {item}, status {resp.status}")
                            file_data = await resp.read()

                            # Detect MIME type
                            mime_type, _ = mimetypes.guess_type(item)
                            if not mime_type:
                                # Try basic magic number detection
                                if file_data.startswith(b"\xff\xd8"):
                                    mime_type = "image/jpeg"
                                elif file_data.startswith(b"\x89PNG"):
                                    mime_type = "image/png"
                                elif file_data.startswith(b"GIF8"):
                                    mime_type = "image/gif"
                                else:
                                    mime_type = "application/octet-stream"
                            maintype, subtype = mime_type.split("/", 1)

                            # Extract filename (fallback to extension)
                            raw_name = os.path.basename(item.split("?")[0])
                            if not raw_name or "." not in raw_name:
                                raw_name = f"attachment.{subtype}"
                            filename = raw_name

                            logging.info(f"Attaching {filename} ({mime_type}), {len(file_data)} bytes")

                            message.add_attachment(
                                file_data,
                                maintype=maintype,
                                subtype=subtype,
                                filename=filename,
                            )
                    
                    # Handle local file paths
                    else:
                        with open(item, "rb") as f:
                            file_data = f.read()
                            mime_type, _ = mimetypes.guess_type(item)
                            if not mime_type:
                                mime_type = "application/octet-stream"
                            maintype, subtype = mime_type.split("/", 1)
                            filename = os.path.basename(item) or f"attachment.{subtype}"

                            logging.info(f"Attaching {filename} ({mime_type}), {len(file_data)} bytes")

                            message.add_attachment(
                                file_data,
                                maintype=maintype,
                                subtype=subtype,
                                filename=filename,
                            )
                            
                except Exception as attach_err:
                    logging.warning(f"Failed to attach {item}: {attach_err}")

    # --- Send ---
    try:
        await aiosmtplib.send(
            message,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            start_tls=True,
            username=SMTP_USER,
            password=SMTP_PASS,
            #mail_from=f"WarehouseNow{uuid.uuid4().hex[:6]}@warehousenow.com"
        )
        return {"status": "success", "to": email_data.email}
    except Exception as e:
        return {"status": "error", "to": email_data.email, "error": str(e)}