import csv
import logging
import os
import re
import urllib
import zipfile
from concurrent import futures
from io import BytesIO, StringIO

import requests
from lxml import html
from requests import RequestException
from requests.adapters import HTTPAdapter

from .turn14_data_storage import Turn14DataStorage

logger = logging.getLogger(__name__)


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

    @staticmethod
    def open_session(max_workers=15, max_retries=10):
        session = requests.Session()
        adapter_kwargs = dict(pool_connections=max_workers,
                              pool_maxsize=max_workers,
                              max_retries=max_retries)
        session.mount('https://', HTTPAdapter(**adapter_kwargs))
        session.mount('http://', HTTPAdapter(**adapter_kwargs))
        return session

    @staticmethod
    def do_request(request_obj, http_fn, url, **kwargs):
        logger.info("Sending {0} request to {1}".format(http_fn, url))
        if "timeout" not in kwargs:
            kwargs["timeout"] = 60
        response_or_future = getattr(request_obj, http_fn)(url, **kwargs)
        if hasattr(response_or_future, "raise_for_status"):
            response_or_future.raise_for_status()
        logger.info("{0} request to {1} completed".format(http_fn, url))
        return response_or_future

    def import_and_store_product_data(self, **kwargs):
        csv_results = dict()
        future_results = dict()
        num_retries = kwargs['num_retries'] if 'num_retries' in kwargs else 0
        refresh_all = kwargs['refresh_all'] if 'refresh_all' in kwargs else False
        data_storage = Turn14DataStorage()

        def store_results():
            try:
                data = list()
                # gather all the future results before hitting db to close the future out
                for future in futures.as_completed(future_results):
                    part_num = future_results[future]
                    data.append({**csv_results[part_num], **future.result()})
                for data_item in data:
                    if data_item['is_valid_item']:
                        data_storage.save(data_item)
            finally:
                future_results.clear()
                csv_results.clear()

        try:
            with self._open_session() as session:
                product_response = self.do_request(session, "post", self.PRODUCT_URL, timeout=120, data={"stockExport": "items"})
                if product_response.headers["content-type"] != "application/zip":
                    raise RequestException("The data returned was not in zip format")
                with BytesIO(product_response.content) as file_stream:
                    zip_file = zipfile.ZipFile(file_stream)
                    with zip_file.open(zip_file.filelist[0]) as inventory_csv:
                        csv_file = StringIO(inventory_csv.read().decode("utf-8", errors="ignore"))
                        with futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                            for data_row in csv.DictReader(csv_file):
                                internal_part_num = data_row['InternalPartNumber']
                                if refresh_all or not Turn14DataStorage.product_exists(internal_part_num):
                                    csv_results[internal_part_num] = data_row
                                    future_results[executor.submit(self._get_part_data, internal_part_num, session)] = internal_part_num
                                    if len(future_results) == self.max_workers:
                                        store_results()
                            store_results()
        except:
            #todo need to keep track and come up with a better restart point
            if num_retries < self.max_retries:
                logger.error("Retrying parse and store due to error", exc_info=1)
                self.import_and_store_product_data(refresh_all=refresh_all, num_retries=num_retries + 1)
            else:
                logger.error("The maximum retry count has been reached")
                raise

    def import_and_store_make_models(self):
        pass

    def _login(self, session):
        logger.info("Sending request to login to turn14")
        login_response = self.do_request(session, "post", "https://www.turn14.com/user/login", data=self.login_data)
        if not 'X-Php-Sess-User' in login_response.headers:
            raise RequestException("Session was not returned from login request")
        return login_response

    def _open_session(self):
        session = self.open_session(self.max_workers)
        self._login(session)
        return session

    def _get_part_data(self, part_num, session):
        part_search_url = "{0}?vmmPart={1}".format(self.SEARCH_URL, part_num)
        logger.info("Getting part data for part_num {0} @ {1}".format(part_num, part_search_url))
        part_search_response = self.do_request(session, "get", part_search_url)
        part_search_html = html.fromstring(part_search_response.content.decode("utf-8", errors="ignore"))
        part_search_data = self._parse_item_data_from_search(part_search_html)
        part_data = dict()
        if part_search_data['is_valid_item']:
            part_data = {**part_search_data, **self._parse_item_data_from_detail(part_search_data['item_code'], part_search_data['primary_img_thumb'], session)}
        else:
            part_data = part_search_data
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

    def _parse_item_data_from_search(self, part_search_html):
        item_search = part_search_html.xpath('//div[@data-itemcode]')
        part_data = {
            'item_code': 0,
            'is_valid_item': False
        }
        if item_search:
            item_html = item_search[0]
            item_code = item_html.attrib['data-itemcode']
            # Skip group buys
            if item_code.isnumeric() and not 'data-productgroup' in item_html.attrib:
                cost_search = item_html.cssselect("*.amount")
                cost = None
                if cost_search:
                    for cost_search_el in cost_search:
                        if "text-muted" not in cost_search_el.attrib['class']:
                            cost = cost_search_el.text.replace("$", "").strip()

                primary_image_search = item_html.cssselect("img.product-info")
                primary_img_thumb = None
                if primary_image_search:
                    primary_img_thumb = primary_image_search[0].attrib['src']

                product_line_search = part_search_html.xpath("//a[contains(@href,'vmmProductLine')]")
                product_line = None
                if product_line_search:
                    product_line = product_line_search[0].text.strip()

                part_data['is_valid_item'] = True
                part_data['item_code'] = item_code.strip()
                part_data['cost'] = cost
                part_data['primary_img_thumb'] = primary_img_thumb
                part_data['product_line'] = product_line
        return part_data

    def _parse_item_data_from_detail(self, item_code, primary_img_thumb, session):
        part_detail_data = dict()
        part_url = "{0}&itemCode={1}".format(self.PART_URL, item_code)
        logger.info("Getting part details for item_code {0} @ {1}".format(item_code, part_url))
        part_detail_response = self.do_request(session, "get", part_url)
        part_detail_html = html.fromstring(part_detail_response.content.decode("utf-8", errors="ignore"))
        overview_search = part_detail_html.cssselect("p.prod-overview")
        overview = ""
        if overview_search:
            overview = overview_search[0].text

        fitment_data = self._parse_fitment(part_detail_html)
        part_detail_data['category'] = fitment_data['category']
        part_detail_data['sub_category'] = fitment_data['sub_category']
        part_detail_data['fitment'] = fitment_data['fitment']

        part_detail_data['images'] = self._parse_images(part_detail_html, primary_img_thumb)
        part_detail_data['overview'] = overview
        return part_detail_data

    def _parse_fitment(self, part_detail_html):
        fitment_sections = part_detail_html.cssselect("li.list-group-item-info")

        anything_regex = "(.+?)(?:&|$)"
        year_regex = re.compile("vmmYear=(\d{4})", re.IGNORECASE)
        make_regex = re.compile("vmmMake=" + anything_regex, re.IGNORECASE)
        model_regex = re.compile("vmmModel=" + anything_regex, re.IGNORECASE)
        sub_model_regex = re.compile("vmmSubmodel=" + anything_regex, re.IGNORECASE)
        engine_regex = re.compile("vmmEngine=" + anything_regex, re.IGNORECASE)
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
                    fitment_notes = None
                    fitment_note_section = fitment_section.cssselect("pre.notesText")
                    if fitment_note_section:
                        fitment_notes = self._parse_fitment_notes(start_year, end_year, fitment_note_section[0])
                    fitment_store = {
                        'make': make,
                        'model': model,
                        'sub_model': sub_model,
                        'engine': engine,
                    }
                    if fitment_notes:
                        for year_range, note in fitment_notes.items():
                            year_tokens = year_range.split("-")
                            fitment_note_start_year = int(year_tokens[0])
                            fitment_note_end_year = int(year_tokens[1])
                            fitment_data['fitment'].append({**fitment_store, **{'start_year': fitment_note_start_year, 'end_year': fitment_note_end_year, 'note': note}})
                    else:
                        fitment_data['fitment'].append({**fitment_store, **{'start_year': int(start_year), 'end_year': int(end_year), 'note': None}})
        fitment_data['fitment'] = self._optimize_fitment_data(fitment_data['fitment'])
        return fitment_data

    def _parse_fitment_notes(self, start_year, end_year, fitment_note_section):
        fitment_note_regex = re.compile("(\d{4}):(.+?)$", re.IGNORECASE | re.MULTILINE)
        fitment_note_start_year = None
        fitment_note_matches = fitment_note_regex.findall(fitment_note_section.text)
        num_matches = len(fitment_note_matches)
        fitment_notes = dict()
        for idx, fitment_note_match in enumerate(fitment_note_matches):
            year = fitment_note_match[0]
            if start_year <= year <= end_year:
                if not fitment_note_start_year:
                    fitment_note_start_year = year
                    fitment_text = fitment_note_match[1].strip()
                next_idx = idx + 1
                next_text = fitment_text
                if next_idx < num_matches:
                    next_text = fitment_note_matches[next_idx][1].strip()
                if fitment_text != next_text or year == end_year:
                    fitment_notes[str(fitment_note_start_year) + "-" + str(year)] = fitment_text
                    fitment_note_start_year = None
                    if year == end_year:
                        break
        return fitment_notes

    def _optimize_fitment_data(self, fitment_data):
        """
        A lot of fitment data from turn14 is duplicated except for the start and end years
        This function will combine any duplicated data into 1 record
        """
        if len(fitment_data) == 1:
            return fitment_data
        consolidated_fitment = dict()

        def insert_in_order(_fitment_to_add):
            _idx_to_add = None
            for _idx, _fitment in enumerate(fitments):
                if _fitment_to_add['start_year'] <= _fitment['start_year']:
                    _idx_to_add = _idx
                    break
            if _idx_to_add:
                fitments.insert(_idx_to_add, _fitment_to_add)
            else:
                fitments.append(_fitment_to_add)

        def consolidate_fitment(fitments):
            if len(fitments) > 1:
                _consolidated_fitments = list()
                _num_fitments = len(fitments)
                for _idx, _fitment in enumerate(fitments):
                    if _idx == 0:
                        _consolidated_fitment = _fitment
                        continue
                    if _consolidated_fitment['start_year'] <= _fitment['start_year'] <= _consolidated_fitment['end_year']:
                        end_year = _consolidated_fitment['end_year']
                        _consolidated_fitment['end_year'] = _fitment['end_year'] if _fitment['end_year'] > end_year else end_year
                    else:
                        _consolidated_fitments.append(_consolidated_fitment)
                        _consolidated_fitment = _fitment
                        if _idx + 1 == _num_fitments:
                            _consolidated_fitments.append(_consolidated_fitment)
                    return _consolidated_fitments
            else:
                return fitments

        for fitment in fitment_data:
            note = fitment['note'] if 'note' in fitment else ""
            key = "%s%s%s%s%s" % (fitment['make'], fitment['model'], fitment['sub_model'], fitment['engine'], note)
            if key not in consolidated_fitment:
                consolidated_fitment[key] = list()
            fitments = consolidated_fitment[key]
            if not len(fitments):
                fitments.append(fitment)
            else:
                insert_in_order(fitment)
        for key, fitments in consolidated_fitment.items():
            consolidated_fitment[key] = consolidate_fitment(fitments)

        optimized_fitments = list()
        for key, fitments in consolidated_fitment.items():
            optimized_fitments += fitments
        return optimized_fitments
