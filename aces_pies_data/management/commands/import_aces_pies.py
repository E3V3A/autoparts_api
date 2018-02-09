import datetime
import io
import re
import traceback
import zipfile

import pytz
import sys

from googleapiclient.http import MediaIoBaseDownload

from aces_pies_data.management.import_utils import get_file_obj_from_zip, get_csv_lines, parse_file_name
from aces_pies_data.models import ImportTracking, ImportTrackingType
from aces_pies_data.util.aces_pies_parsing import PiesFileParser, AcesFileParser
from aces_pies_data.util.aces_pies_storage import PiesDataStorage, AcesDataStorage, PiesCategoryDataStorage
from . import build_google_service
from django.core.management import BaseCommand
import logging

logger = logging.getLogger('AcesPiesJob')


class Command(BaseCommand):
    """
    This command is responsible for parsing data from the google drive folder
    This is designed to run as a cron job as a full blown celery solution seemed overkill
    """
    help = 'Imports aces pies data from a google drive folder'

    def handle(self, *args, **options):
        max_attempts = 10
        num_tries = 0
        import_complete = False
        logger.info("Parsing aces pies data")
        last_exception = None
        while not import_complete and num_tries < max_attempts:
            num_tries += 1
            try:
                self.do_import()
                import_complete = True
            except Exception as e:
                logger.exception("There was a problem importing, trying again")
                last_exception = e
        if last_exception:
            raise last_exception

    def do_import(self):
        drive_service = build_google_service('drive', 'v3', ['https://www.googleapis.com/auth/drive'])
        files_to_process = self.get_files_to_process(drive_service)
        files_to_parse = files_to_process['files_to_parse']
        files_to_archive = files_to_process['files_to_archive']
        pending_folder_id = files_to_process['pending_folder_id']
        archived_folder_id = files_to_process['archived_folder_id']
        for file_info in files_to_parse:
            brand_short_name = file_info['brand_short_name']
            import_type = file_info['import_type']
            import_action = ImportTracking.objects.get_import_action(brand_short_name, file_info['date'], import_type)
            file_name = file_info['file']['name']
            logger.info(f"Determining if file {file_name} should be parsed")
            with TrackingRecord(import_type, import_action, brand_short_name, file_name):
                if import_action == ImportTracking.DO_IMPORT:
                    def on_complete():
                        archive_file(drive_service, file_info['file']['id'], pending_folder_id, archived_folder_id)

                    logger.info(f"Downloading file {file_name} for {import_type} parsing")
                    file_bytes = self.download_file(drive_service, file_info['file']['id'])
                    with zipfile.ZipFile(file_bytes) as zip_file:
                        file_obj = get_file_obj_from_zip(zip_file, import_type)
                        with zip_file.open(file_obj) as data_file:
                            if import_type == "pies":
                                pies_file_parser = PiesFileParser(data_file, brand_short_name)
                                PiesDataStorage(pies_file_parser.get_brand_data()).store_brand_data(on_complete)
                            elif import_type == "pies_flat":
                                PiesCategoryDataStorage(get_csv_lines(data_file), brand_short_name).store_category_data(on_complete)
                            elif import_type == "aces":
                                aces_file_parser = AcesFileParser(get_csv_lines(data_file), brand_short_name)
                                AcesDataStorage(aces_file_parser).store_brand_fitment(on_complete)
                elif import_action == ImportTracking.DO_ARCHIVE:
                    logger.info(f"Archiving file {file_name}")
                    archive_file(drive_service, file_info['file']['id'], pending_folder_id, archived_folder_id)
                else:
                    logger.info(f"Skipping import of file {file_name}.  Check if there is any part data or category data yet")

        for file_to_archive in files_to_archive:
            file_name = file_to_archive['file']['name']
            brand_short_name = file_to_archive['brand_short_name']
            import_type = file_to_archive['import_type']
            import_action = ImportTracking.DO_ARCHIVE
            with TrackingRecord(import_type, import_action, brand_short_name, file_name):
                logger.info(f"Archiving file {file_name}")
                archive_file(drive_service, file_to_archive['file']['id'], pending_folder_id, archived_folder_id)

    def get_files_to_process(self, drive_service):
        logging.info("Retrieving files to import")
        pending_data_folder_result = drive_service.files().list(q="name = 'pending data'").execute()
        archived_data_folder_result = drive_service.files().list(q="name = 'archived data'").execute()
        pending_data_folder_id = pending_data_folder_result['files'][0]['id']
        archived_data_folder_id = archived_data_folder_result['files'][0]['id']
        query = f"'{pending_data_folder_id}' in parents"
        page_size = 1000
        pending_files_request = drive_service.files().list(q=query, pageSize=page_size).execute()
        pending_files = pending_files_request['files']
        next_token = pending_files_request.get('nextPageToken', None)
        while next_token:
            pending_files_request = drive_service.files().list(pageToken=next_token, pageSize=page_size, q=query).execute()
            pending_files += pending_files_request['files']
            next_token = pending_files_request.get('nextPageToken', None)

        aces_files = dict()
        pies_files = dict()
        pies_flat_files = dict()
        files_to_archive = list()

        # loop through, pick latest dates
        # any dates prior get added to archive
        for file in pending_files:
            parsed_file_name = parse_file_name(file['name'])
            brand_short_name = parsed_file_name['brand_short_name']
            import_type = parsed_file_name['import_type']
            file_date = parsed_file_name['file_date']
            file_lookup = None
            if import_type == "aces":
                file_lookup = aces_files
            elif import_type == "pies":
                file_lookup = pies_files
            elif import_type == "pies_flat":
                file_lookup = pies_flat_files
            if file_lookup is not None:
                file_data = {
                    "import_type": import_type,
                    "brand_short_name": brand_short_name,
                    "file": file,
                    "date": file_date
                }
                if brand_short_name not in file_lookup or file_lookup[brand_short_name]["date"] < file_date:
                    if brand_short_name in file_lookup:
                        files_to_archive.append(file_data)
                    file_lookup[brand_short_name] = file_data
                else:
                    files_to_archive.append(file_data)
        return {
            "files_to_parse": list(pies_flat_files.values()) + list(pies_files.values()) + list(aces_files.values()),
            "files_to_archive": files_to_archive,
            'pending_folder_id': pending_data_folder_id,
            'archived_folder_id': archived_data_folder_id
        }

    def download_file(self, drive_service, file_id):
        request = drive_service.files().get_media(fileId=file_id)
        file_bytes = io.BytesIO()
        downloader = MediaIoBaseDownload(file_bytes, request, chunksize=2048 * 2048)
        done = False
        while done is False:
            status, done = downloader.next_chunk(num_retries=10)
            logger.info(f'Downloaded {file_id} {int(status.progress() * 100)}%')
        return file_bytes


class TrackingRecord(object):
    def __init__(self, tracking_type, import_action, brand_short_name, file_name):
        self.tracking_type = tracking_type
        self.brand_short_name = brand_short_name
        self.import_action = import_action
        self.file_name = file_name

    def __enter__(self):
        tracking_type_record = ImportTrackingType.objects.get_or_create(name=self.tracking_type)[0]
        self.tracking_record = ImportTracking.objects.create(brand_short_name=self.brand_short_name, import_action=self.import_action, tracking_type=tracking_type_record, file_name=self.file_name)

    def __exit__(self, *args):
        if sys.exc_info()[0]:
            self.tracking_record.stack_trace = traceback.format_exc()
        else:
            self.tracking_record.end_date = datetime.datetime.now(pytz.timezone("UTC"))
        self.tracking_record.save()


def archive_file(drive_service, file_id, pending_folder_id, archived_folder_id):
    drive_service.files().update(fileId=file_id, addParents=archived_folder_id, removeParents=pending_folder_id).execute()
