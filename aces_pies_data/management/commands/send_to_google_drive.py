import base64
import logging
import os
import re

import requests
from django.core.management import BaseCommand
from googleapiclient.http import MediaFileUpload

from aces_pies_data.management.commands import build_google_service

logger = logging.getLogger('GoogleDriveJob')


class Command(BaseCommand):
    """
    This command is designed to go to gmail service account, find dci emails, download the file from the link in the body, and send to google drive folder
    """
    help = 'Downloads files from email body and sends to google drive'

    def handle(self, *args, **options):
        drive_service = build_google_service('drive', 'v3', ['https://www.googleapis.com/auth/drive'])
        gmail_service = build_google_service('gmail', 'v1', ['https://mail.google.com/'])
        files_folder_result = drive_service.files().list(q="name = 'pending data'").execute()
        files_folder_id = files_folder_result['files'][0]['id']
        for email in get_dci_emails(gmail_service):
            if email['download_url']:
                try:
                    file_name = download_dci_file(email['download_url'])
                    upload_to_google_drive(drive_service, file_name, files_folder_id)
                    archive_email(gmail_service, email['email_id'])
                finally:
                    pass
                    try:
                        os.remove(file_name)
                    except:
                        logger.warning(f"Failed to delete {file_name}")
                        pass


def archive_email(gmail_service, email_id):
    logger.info(f"Archiving email {email_id}")
    gmail_service.users().messages().trash(id=email_id, userId='me').execute()


def upload_to_google_drive(drive_service, file_name, folder_id):
    media = MediaFileUpload(file_name, mimetype='application/zip', resumable=True, chunksize=2048 * 2048)
    try:
        file_metadata = {
            'name': file_name,
            'mimeType': 'application/x-zip-compressed',
            'parents': [folder_id]
        }
        request = drive_service.files().create(body=file_metadata, media_body=media, fields='id')
        response = None
        logger.info(f"Uploading {file_name} to google drive")
        while response is None:
            status, response = request.next_chunk(num_retries=10)
            if status:
                logger.info("Uploaded %d%%." % int(status.progress() * 100))
        logger.info("Upload Complete!")
    finally:
        media._fd.close()


def download_dci_file(url):
    file_name = url.split('/')[-1]
    logger.info(f"Downloading {file_name} from {url}")
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(file_name, 'wb') as f:
        for chunk in r.iter_content(chunk_size=2048 * 2048):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return file_name


def get_dci_emails(gmail_service):
    email_query = "subject:DCi Data Delivery Notification"
    gmail_kwargs = {"userId": 'me', "q": email_query}

    # Is there a better way to use yield here without repeating code?
    gmail_request = gmail_service.users().messages().list(**gmail_kwargs).execute()
    if 'messages' in gmail_request:
        for message in gmail_request['messages']:
            yield get_email_with_content(gmail_service, message)
        next_token = gmail_request.get('nextPageToken', None)
        while next_token:
            gmail_kwargs['pageToken'] = next_token
            gmail_request = gmail_service.users().messages().list(**gmail_kwargs).execute()
            for message in gmail_request['messages']:
                yield get_email_with_content(gmail_service, message)
            next_token = gmail_request.get('nextPageToken', None)


def get_email_with_content(gmail_service, dci_email):
    content = gmail_service.users().messages().get(id=dci_email['id'], userId='me').execute()
    download_link_regex = re.compile("http://www\.etailerdataflow\.com/_Exports/.+?/.+?\.zip")
    if 'parts' in content['payload']:
        body_data = content['payload']['parts'][0]['body']['data']
    else:
        body_data = content['payload']['body']['data']

    body = base64.urlsafe_b64decode(body_data).decode("utf-8")
    download_link_match = download_link_regex.search(body)
    download_url = None
    if download_link_match:
        download_url = download_link_match.group(0)
    return {
        "download_url": download_url,
        "email_id": dci_email['id']
    }
