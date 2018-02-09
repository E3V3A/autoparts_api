import datetime
import re

import pytz


def get_file_obj_from_zip(zip_file, import_type):
    import_type_file = {
        "pies_flat": "piesdata67.txt",
        "aces": "n1parts.txt",
        "pies": "pies67.xml"
    }

    for file_obj in zip_file.filelist:
        if import_type_file[import_type] in file_obj.filename.lower():
            return file_obj

    raise ValueError(f"No file found for {import_type}")


def get_csv_lines(csv_file):
    for line in csv_file:
        yield line.decode("utf-8")


def parse_file_name(file_name):
    file_name_regex = re.compile("(.+?)([0-9]{8})_(.+?).zip")
    parsed_file_name = file_name_regex.search(file_name)
    brand_short_name = parsed_file_name.group(1)
    file_date_string = parsed_file_name.group(2)
    file_type = parsed_file_name.group(3)
    file_date = datetime.datetime.strptime(file_date_string, '%Y%m%d').astimezone(pytz.timezone('US/Eastern'))
    import_type = None
    if file_type == "N1":
        import_type = "aces"
    elif file_type == "PIES67":
        import_type = "pies"
    elif file_type == "PIES67Flat":
        import_type = "pies_flat"
    if not import_type:
        raise ValueError(f"No import type found in {file_name}")
    return {
        "brand_short_name": brand_short_name,
        "file_date": file_date,
        "import_type": import_type
    }
