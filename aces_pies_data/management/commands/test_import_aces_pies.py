import os
import re
import zipfile

from django.conf import settings
from django.core.management import BaseCommand
import logging

from aces_pies_data.management.import_utils import get_file_obj_from_zip, get_csv_lines, parse_file_name
from aces_pies_data.util.aces_pies_parsing import PiesFileParser, AcesFileParser
from aces_pies_data.util.aces_pies_storage import PiesDataStorage, PiesCategoryDataStorage, AcesDataStorage

logger = logging.getLogger('AcesPiesJob')


class Command(BaseCommand):
    """
        This command is used for development purposes only
    """
    help = 'Imports aces pies data from local files for development and testing purposes only'

    def __init__(self, stdout=None, stderr=None, no_color=False):
        super().__init__(stdout, stderr, no_color)
        self.aces_pies_folder = os.environ.get("aces_pies_folder")
        if settings.ENVIRONMENT == "prod":
            raise EnvironmentError("This is a test command that cannot be run in a production environment")
        if not self.aces_pies_folder:
            raise EnvironmentError("You must set the aces_pies_folder environment variable")

    def handle(self, *args, **options):
        files_to_parse = self.get_files_to_process()
        for file_info in files_to_parse:
            with open(file_info['file_path'], 'rb') as file:
                with zipfile.ZipFile(file) as zip_file:
                    import_type = file_info['import_type']
                    brand_short_name = file_info['brand_short_name']
                    file_obj = get_file_obj_from_zip(zip_file, import_type)
                    with zip_file.open(file_obj) as data_file:
                        if import_type == "pies":
                            pies_file_parser = PiesFileParser(data_file, brand_short_name)
                            PiesDataStorage(pies_file_parser.get_brand_data()).store_brand_data()
                        elif import_type == "pies_flat":
                            PiesCategoryDataStorage(get_csv_lines(data_file), brand_short_name).store_category_data()
                        elif import_type == "aces":
                            aces_file_parser = AcesFileParser(get_csv_lines(data_file), brand_short_name)
                            AcesDataStorage(aces_file_parser).store_brand_fitment()

    def get_files_to_process(self):
        aces_files = list()
        pies_files = list()
        pies_flat_files = list()
        for file_name in os.listdir(self.aces_pies_folder):
            parsed_file_name = parse_file_name(file_name)
            brand_short_name = parsed_file_name['brand_short_name']
            import_type = parsed_file_name['import_type']
            file_date = parsed_file_name['file_date']
            file_path = self.aces_pies_folder + "\\" + file_name
            data = {
                "brand_short_name": brand_short_name,
                "file_path": file_path,
                "file_date": file_date,
                "import_type": import_type
            }
            file_lookup = None
            if import_type == "aces":
                file_lookup = aces_files
            elif import_type == "pies":
                file_lookup = pies_files
            elif import_type == "pies_flat":
                file_lookup = pies_flat_files
            if file_lookup is not None:
                file_lookup.append(data)
        return pies_flat_files + pies_files + aces_files
