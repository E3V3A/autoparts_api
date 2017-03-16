import csv
import logging
import os
import urllib
import zipfile
from concurrent import futures
from io import StringIO

import re
import requests
from io import BytesIO

from decimal import Decimal

from django.db import transaction
from lxml import html
from requests import HTTPError
from requests import RequestException
from requests.adapters import HTTPAdapter
from supplier.models import Product, Vendor, Category, ProductImage, ProductImageMap

logger = logging.getLogger(__name__)


def open_session(max_workers=15, max_retries=10):
    session = requests.Session()
    adapter_kwargs = dict(pool_connections=max_workers,
                          pool_maxsize=max_workers,
                          max_retries=max_retries)
    session.mount('https://', HTTPAdapter(**adapter_kwargs))
    session.mount('http://', HTTPAdapter(**adapter_kwargs))
    return session


def do_request(request_obj, http_fn, url, **kwargs):
    logger.info("Sending {0} request to {1}".format(http_fn, url))
    if "timeout" not in kwargs:
        kwargs["timeout"] = 60
    response_or_future = getattr(request_obj, http_fn)(url, **kwargs)
    if hasattr(response_or_future, "raise_for_status"):
        response_or_future.raise_for_status()
    logger.info("{0} request to {1} completed".format(http_fn, url))
    return response_or_future


class Turn14DataStorage:
    def __init__(self, data_item):
        self.data_item = data_item
        self.product_data_mapping = {
            'Retail': {
                'model': 'retail_price',
                'serializer': lambda val: self.string_to_decimal(val)
            },
            'Map': {
                'model': 'min_price',
                'serializer': lambda val: self.string_to_decimal(val)
            },
            'Jobber': {
                'model': 'jobber_price',
                'serializer': lambda val: self.string_to_decimal(val)
            },
            'CoreCharge': {
                'model': 'core_charge',
                'serializer': lambda val: self.string_to_decimal(val)
            },
            'DropShip': {
                'model': 'can_drop_ship',
                'serializer': lambda val: self._can_drop_ship(val)
            },
            'DSFee': {
                'model': 'drop_ship_fee',
                'serializer': lambda val: self._get_drop_ship_fee(val)
            },
            'Description': {
                'model': 'description',
                'serializer': None
            },
            'Weight': {
                'model': 'weight_in_lbs',
                'serializer': lambda val: self.string_to_decimal(val)
            },
            'PartNumber': {
                'model': 'vendor_part_num',
                'serializer': None
            },
            'InternalPartNumber': {
                'model': 'internal_part_num',
                'serializer': None
            },
            'overview': {
                'model': 'overview',
                'serializer': None
            },
            'cost': {
                'model': 'cost',
                'serializer': lambda val: self.string_to_decimal(val)
            }
        }

    @transaction.atomic
    def save(self):
        vendor_name = self.data_item["PrimaryVendor"]
        product_args = {
            'vendor': Vendor.objects.get_or_create(name=vendor_name)[0]
        }
        for key, value in self.data_item.items():
            if key in self.product_data_mapping:
                data_mapper = self.product_data_mapping[key]
                product_args[data_mapper['model']] = data_mapper['serializer'](value) if data_mapper['serializer'] is not None else value

        if self.data_item['category']:
            product_args['category'] = Category.objects.get_or_create(name=self.data_item['category'], parent_category=None)[0]
            if self.data_item['sub_category']:
                product_args['category'] = Category.objects.get_or_create(name=self.data_item['sub_category'], parent_category=product_args['category'])[0]
        product_record = Product.objects.update_or_create(internal_part_num=product_args['internal_part_num'], defaults=product_args)[0]
        self._store_remote_images(product_record)

    def _store_remote_images(self, product_record):
        ProductImageMap.objects.filter(product=product_record).delete()
        for image_stack in self.data_item['images']:
            img_url = image_stack['large_img'] if image_stack['large_img'] else image_stack['med_img']
            if img_url:
                product_image = ProductImage.objects.get_or_create(remote_image_file=img_url)[0]
                ProductImageMap.objects.create(product=product_record, image=product_image)

    def _download_and_store_images(self, product_record):
        ProductImageMap.objects.filter(product=product_record).delete()
        max_image_retries = 3
        with open_session() as image_session:
            for image_stack in self.data_item['images']:
                img_url = image_stack['large_img'] if image_stack['large_img'] else image_stack['med_img']
                if img_url:
                    image_retries = 0
                    url_tokens = re.split('/|\\\\', img_url)
                    image_file_name = url_tokens[-1:][0]
                    product_images = ProductImage.objects.filter(image_file__contains=image_file_name)
                    if not product_images:
                        last_http_error = None
                        while image_retries < max_image_retries:
                            image_retries += 1
                            try:
                                image_response = do_request(image_session, "get", img_url)
                                image_retries = max_image_retries
                            except HTTPError as last_http_error:
                                logger.error("Failed to download image from {0}, retrying".format(img_url), exc_info=1)
                        if image_response.status_code != 200:
                            raise last_http_error
                        with BytesIO(image_response.content) as file_stream:
                            product_image = ProductImage()
                            product_image.image_file.save(image_file_name, file_stream, True)
                    else:
                        product_image = product_images[0]
                    ProductImageMap.objects.get_or_create(product=product_record, image=product_image)

    @staticmethod
    def product_exists(internal_part_num):
        if len(Product.objects.filter(internal_part_num=internal_part_num)):
            return True
        return False

    @staticmethod
    def string_to_decimal(value):
        if value:
            return Decimal(value.replace(",", ""))
        return None

    @staticmethod
    def _can_drop_ship(value):
        mapping = {
            'possible': Product.POSSIBLE_DROPSHIP,
            'never': Product.NEVER_DROPSHIP,
            'always': Product.ALWAYS_DROPSHIP,
        }
        return mapping[value] if value else None

    @staticmethod
    def _get_drop_ship_fee(value):
        fee_regex = re.compile("\d+\.\d+")
        fee = fee_regex.search(value)
        if fee:
            return Decimal(fee.group(0))
        return None

class Turn14DataImporter:
    PRODUCT_URL = "https://www.turn14.com/export.php"
    SEARCH_URL = "https://www.turn14.com/search/index.php"
    PART_URL = "https://www.turn14.com/ajax_scripts/vmm.php?action=product"

    def __init__(self, max_workers=15, max_retries=3, max_failed_items=100):
        turn14_user = os.environ.get("turn14_user")
        turn14_password = os.environ.get("turn14_password")
        if not turn14_user:
            raise ValueError("No variable turn14_user found in the environment")
        if not turn14_password:
            raise ValueError("No variable turn14_password found in the environment")

        self.login_data = {
            "username": turn14_user,
            "password": turn14_password
        }
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.max_failed_items = max_failed_items

    def import_and_store_product_data(self, **kwargs):
        csv_results = dict()
        future_results = dict()
        num_retries = kwargs['num_retries'] if 'num_retries' in kwargs else 0

        def store_results():
            try:
                for future in futures.as_completed(future_results):
                    part_num = future_results[future]
                    web_results = future.result()
                    if web_results['is_valid_item']:
                        Turn14DataStorage({**csv_results[part_num], **web_results}).save()
            finally:
                future_results.clear()
                csv_results.clear()

        try:
            with self._open_session() as session:
                product_response = do_request(session, "post", self.PRODUCT_URL, timeout=120, data={"stockExport": "items"})
                if product_response.headers["content-type"] != "application/zip":
                    raise RequestException("The data returned was not in zip format")
                with BytesIO(product_response.content) as file_stream:
                    zip_file = zipfile.ZipFile(file_stream)
                    with zip_file.open(zip_file.filelist[0]) as inventory_csv:
                        csv_file = StringIO(inventory_csv.read().decode("utf-8", errors="ignore"))
                        with futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                            for data_row in csv.DictReader(csv_file):
                                internal_part_num = data_row['InternalPartNumber']
                                if not Turn14DataStorage.product_exists(internal_part_num):
                                    csv_results[internal_part_num] = data_row
                                    future_results[executor.submit(self._get_part_data, internal_part_num, session)] = internal_part_num
                                    if len(future_results) == self.max_workers:
                                        store_results()
                            store_results()
        except:
            if num_retries < self.max_retries:
                logger.error("Retrying parse and store due to error", exc_info=1)
                self.import_and_store_product_data(num_retries=num_retries + 1)
            else:
                raise

    def import_and_store_make_models(self):
        pass

    def _login(self, session):
        logger.info("Sending request to login to turn14")
        login_response = do_request(session, "post", "https://www.turn14.com/user/login", data=self.login_data)
        if not 'X-Php-Sess-User' in login_response.headers:
            raise RequestException("Session was not returned from login request")
        return login_response

    def _open_session(self):
        session = open_session(self.max_workers)
        self._login(session)
        return session

    def _get_part_data(self, part_num, session):
        part_search_url = "{0}?vmmPart={1}".format(self.SEARCH_URL, part_num)
        logger.info("Getting part data for part_num {0} @ {1}".format(part_num, part_search_url))
        part_search_response = do_request(session, "get", part_search_url)
        part_search_html = html.fromstring(part_search_response.content.decode("utf-8", errors="ignore"))
        item_search = part_search_html.xpath('//div[@data-itemcode]')
        part_data = {
            'item_code': 0,
            'is_valid_item': False
        }
        if item_search:
            item_html = item_search[0]
            attributes = item_html.attrib
            item_code = attributes['data-itemcode']
            # Skip group buys
            if item_code.isnumeric() and not 'data-productgroup' in attributes:
                cost_search = item_html.cssselect("p.amount")
                cost = None
                if cost_search:
                    cost = cost_search[0].text.replace("$", "")

                    primary_image_search = item_html.cssselect("img.product-info")
                primary_img_thumb = ""
                if primary_image_search:
                    primary_img_thumb = primary_image_search[0].attrib['src']
                part_url = "{0}&itemCode={1}".format(self.PART_URL, item_code)
                logger.info(
                    "Getting part details for part_num {0}, item_code {1} @ {2}".format(part_num, item_code, part_url))
                part_detail_response = do_request(session, "get", part_url)
                part_detail_html = html.fromstring(part_detail_response.content.decode("utf-8", errors="ignore"))
                fitment_data = self._parse_fitment(part_detail_html)
                part_data['category'] = fitment_data['category']
                part_data['sub_category'] = fitment_data['sub_category']
                part_data['fitment'] = fitment_data['fitment']

                part_data['images'] = self._parse_images(part_detail_html, primary_img_thumb)
                part_data['overview'] = self._parse_overview(part_detail_html)
                part_data['item_code'] = item_code
                part_data['is_valid_item'] = True
                part_data['cost'] = cost
            else:
                logger.info("Skipping part num {0} because it is a group buy".format(part_num))
        return part_data

    def _parse_images(self, part_detail_html, primary_img_thumb):
        images = list()
        img_thumbs = part_detail_html.cssselect("img[data-mediumimage]")
        primary_img_group = None
        for img_thumb in img_thumbs:
            attributes = img_thumb.attrib
            thumb_img = attributes["src"] if "src" in attributes else ""
            img_group = {
                "thumb_img": thumb_img,
                "med_img": attributes["data-mediumimage"] if "data-mediumimage" in attributes else "",
                "large_img": attributes["data-largeimage"] if "data-largeimage" in attributes else ""
            }
            if not primary_img_group and primary_img_thumb and primary_img_thumb == thumb_img:
                primary_img_group = img_group
            else:
                images.append(img_group)
        if primary_img_group:
            images.insert(0, primary_img_group)
        return images

    def _parse_overview(self, part_detail_html):
        overview_el = part_detail_html.cssselect("p.prod-overview")
        if overview_el:
            return overview_el[0].text
        return ""

    def _parse_fitment(self, part_detail_html):
        fitment_sections = part_detail_html.cssselect("li.list-group-item-info")

        anything_regex = "(.+?)(?:&|$)"
        year_regex = re.compile("vmmYear=(\d{4})", re.IGNORECASE)
        make_regex = re.compile("vmmMake=" + anything_regex, re.IGNORECASE)
        model_regex = re.compile("vmmModel=" + anything_regex, re.IGNORECASE)
        sub_model_regex = re.compile("vmmSubmodel=" + anything_regex, re.IGNORECASE)
        engine_regex = re.compile("vmmEngine=" + anything_regex, re.IGNORECASE)
        special_fitment_regex = re.compile("(\d{4}):(.+?)$", re.IGNORECASE | re.MULTILINE)
        category_regex = re.compile("vmmCategory=" + anything_regex)

        fitment_data = {
            'category': '',
            'sub_category': '',
            'fitment': []
        }

        def parse_link(section):
            link = section.cssselect("a")[0]
            attributes = link.attrib
            escaped_url = attributes['href']
            url = urllib.parse.unquote_plus(escaped_url)
            return {
                'link': link,
                'escaped_url': escaped_url,
                'url': url,
                'text': link.text_content().strip()
            }

        if fitment_sections:
            first_section_link = parse_link(fitment_sections[0])
            category_match = category_regex.search(first_section_link['escaped_url'])
            if category_match:
                category = urllib.parse.unquote_plus(category_match.group(1))
                fitment_data['category'] = category
                sub_category_regex = re.compile(category + "::(.+?)$")
                sub_category_match = sub_category_regex.search(first_section_link['text'])
                if sub_category_match:
                    fitment_data['sub_category'] = sub_category_match.group(1)

        for fitment_section in fitment_sections:
            fitment_link = parse_link(fitment_section)
            end_year_match = year_regex.search(fitment_link['url'])
            if end_year_match:
                store_fitment = True
                make_match = make_regex.search(fitment_link['url'])
                model_match = model_regex.search(fitment_link['url'])
                sub_model_match = sub_model_regex.search(fitment_link['url'])
                engine_match = engine_regex.search(fitment_link['url'])

                if not make_match:
                    logger.warning("No make found for {0}".format(fitment_link['escaped_url']))
                    store_fitment = False

                if not model_match:
                    logger.warning("No model found for {0}".format(fitment_link['escaped_url']))
                    store_fitment = False

                if not sub_model_match:
                    logger.warning("No sub model found for {0}".format(fitment_link['escaped_url']))
                    store_fitment = False

                if not engine_match:
                    logger.warning("No engine found for {0}".format(fitment_link['escaped_url']))
                    store_fitment = False

                if store_fitment:
                    make = make_match.group(1)
                    model = model_match.group(1)
                    sub_model = sub_model_match.group(1)
                    engine = engine_match.group(1)

                    fitment_text = fitment_link['text']
                    end_year = end_year_match.group(1)
                    start_year = end_year
                    year_range_regex = re.compile("(\d{4})-" + end_year, re.IGNORECASE)
                    year_range_match = year_range_regex.search(fitment_text)
                    if year_range_match:
                        start_year = year_range_match.group(1)
                    special_fitment = {}
                    special_fitment_section = fitment_section.cssselect("pre.notesText")
                    if special_fitment_section:
                        special_fitment_section = special_fitment_section[0]
                        for special_fitment_match in special_fitment_regex.findall(special_fitment_section.text):
                            special_fitment[special_fitment_match[0]] = special_fitment_match[1].strip()
                    for year in range(int(start_year), int(end_year) + 1):
                        fitment_data['fitment'].append({
                            'year': year,
                            'make': make,
                            'model': model,
                            'sub_model': sub_model,
                            'engine': engine,
                            'special_fitment': special_fitment[str(year)] if str(year) in special_fitment else ''
                        })
        return fitment_data
