import csv
import string
import xml.etree.ElementTree as XmlTree
import re
from decimal import Decimal
from timeit import default_timer as timer

from aces_pies_data.models import Brand
import logging

from aces_pies_data.util.sql_lite_utils import SqlLiteTempDb

logger = logging.getLogger("AcesPiesParsing")


class PiesFileParser(object):
    xmlns = "http://www.autocare.org"
    xml_namespaces = {"autocare": xmlns}

    def __init__(self, pies_xml_binary, brand_short_name):
        self.pies_xml_binary = pies_xml_binary
        self.brand_short_name = brand_short_name

    def get_brand_data(self):
        """
            File does not come with brand name at the top level and the file name itself doesn't help with figuring out the brand name
            Get the first item and parse out the brand name, then return a generator that parses all the product data
        Returns:
            A dictionary containing brand level info plus a generator for all the product data
        """
        xml_tree = XmlTree.iterparse(self.pies_xml_binary)
        brand_name, marketing_copy, logo_data, first_xml_item = None, None, None, None
        for event, xml_item in xml_tree:
            if xml_item.tag == "{{{0}}}MarketCopy".format(self.xmlns):
                marketing_copy = xml_item.find("autocare:MarketCopyContent", self.xml_namespaces).text
                digital_assets_elem = xml_item.find("autocare:DigitalAssets", self.xml_namespaces)
                for asset_elem in digital_assets_elem:
                    asset_type = asset_elem.find("autocare:AssetType", self.xml_namespaces).text
                    if asset_type == "LGO":
                        logo_data = dict()
                        logo_data['name'] = asset_elem.find("autocare:FileName", self.xml_namespaces).text
                        logo_data['url'] = asset_elem.find("autocare:URI", self.xml_namespaces).text
                        logo_data['file_size_bytes'] = asset_elem.find("autocare:FileSize", self.xml_namespaces).text

            elif xml_item.tag == "{{{0}}}Item".format(self.xmlns):
                first_xml_item = xml_item
                brand_name = first_xml_item.find("autocare:BrandLabel", self.xml_namespaces).text
            if marketing_copy and brand_name:
                break
        if not marketing_copy or not brand_name:
            raise RuntimeError("No marketing copy or brand name found in XML document")
        return {
            'brand': brand_name,
            'brand_short_name': self.brand_short_name,
            'logo': logo_data,
            'marketing_copy': marketing_copy,
            'product_data': self._get_product_data(xml_tree, first_xml_item)
        }

    def _get_product_data(self, xml_tree, first_xml_item=None):
        def get_xml_data(_xml_item):
            _xml_item_data = self._parse_xml_item(_xml_item)
            _xml_item.clear()
            return _xml_item_data

        if first_xml_item:
            yield get_xml_data(first_xml_item)
        for event, xml_item in xml_tree:
            if xml_item.tag == "{{{0}}}Item".format(self.xmlns):
                yield get_xml_data(xml_item)

    def _parse_xml_item(self, xml_item):
        part_number = xml_item.find("autocare:PartNumber", self.xml_namespaces).text
        hazardous_elem = xml_item.find("autocare:HazardousMaterialCode", self.xml_namespaces)
        is_hazardous = hazardous_elem.text.lower() == "y" if hazardous_elem else False
        product_data = {**{
            'part_number': part_number,
            'brand_name': xml_item.find("autocare:BrandLabel", self.xml_namespaces).text,
            'is_hazardous': is_hazardous
        }, **self._parse_xml_descriptions(xml_item, part_number), **self._parse_xml_attributes(xml_item), **self._parse_xml_extended_info(xml_item), **self._parse_xml_pricing(xml_item), **self._parse_xml_digital_assets(xml_item), **self._parse_xml_packaging(xml_item)}

        return product_data

    def _parse_xml_descriptions(self, xml_item, part_number):
        descriptions_elem = xml_item.find("autocare:Descriptions", self.xml_namespaces)
        feature_lookup, features, product_ext, product_des = dict(), list(), None, None
        for description_elem in descriptions_elem:
            code = description_elem.attrib['DescriptionCode']
            if code == 'DES':
                product_des = description_elem.text
            elif code == 'EXT':
                product_ext = description_elem.text
            elif code == 'FAB':
                sequence = description_elem.attrib['Sequence']
                feature_lookup[sequence] = description_elem.text
        for feature_sequence in sorted(feature_lookup.keys()):
            features.append(feature_lookup[feature_sequence])
        product_name = part_number
        if product_ext:
            product_name = product_ext
        elif product_des:
            product_name = product_des
        return {
            'name': product_name,
            'features': features
        }

    def _parse_xml_attributes(self, xml_item):
        attributes_elem = xml_item.find("autocare:ProductAttributes", self.xml_namespaces)
        attributes = list()
        if attributes_elem:
            for attribute_elem in attributes_elem:
                attributes.append({
                    "type": string.capwords(attribute_elem.get("AttributeID")).strip(),
                    "value": attribute_elem.text.strip()
                })
        return {
            "attributes": attributes
        }

    def _parse_xml_extended_info(self, xml_item):
        extended_info_elems = xml_item.find("autocare:ExtendedInformation", self.xml_namespaces)
        not_for_ca, discontinued, obsolete, superseded = False, False, False, False
        superseded_by = None
        if extended_info_elems:
            for extended_info_elem in extended_info_elems:
                code, value = extended_info_elem.get("EXPICode"), extended_info_elem.text
                if code == "EMS" and value == "2":
                    not_for_ca = True
                elif code == "LIF":
                    if value == "7":
                        superseded = True
                    elif value == "8":
                        discontinued = True
                    elif value == "9":
                        obsolete = True
                elif code == "PTS":
                    superseded_by = value
        if superseded_by and not superseded:
            superseded_by = None

        return {
            'is_carb_legal': not not_for_ca,
            'is_discontinued': discontinued,
            'is_obsolete': obsolete,
            'is_superseded': superseded,
            'superseded_by': superseded_by
        }

    def _parse_xml_pricing(self, xml_item):
        pricing_elems = xml_item.find("autocare:Prices", self.xml_namespaces)
        retail_price, map_price = None, None
        if pricing_elems:
            for pricing_elem in pricing_elems:
                price_type = pricing_elem.get("PriceType")
                price_elem = pricing_elem.find("autocare:Price", self.xml_namespaces)
                price = round(Decimal(price_elem.text), 2)
                if price_type == "RMP":
                    map_price = price
                elif price_type == "RET":
                    retail_price = price
        return {
            'map_price': map_price,
            'retail_price': retail_price
        }

    def _parse_xml_packaging(self, xml_item):
        packages_elem = xml_item.findall("autocare:Packages/autocare:Package", self.xml_namespaces)
        packages = list()
        if packages_elem:
            for package_elem in packages_elem:
                package_uom = package_elem.find("autocare:PackageUOM", self.xml_namespaces)
                if package_uom.text == "EA":
                    package_data = dict()
                    quantity_elem = package_elem.find("autocare:QuantityofEaches", self.xml_namespaces)
                    package_data['quantity'] = int(quantity_elem.text)
                    dimensions_elem = package_elem.find("autocare:Dimensions", self.xml_namespaces)
                    weights_elem = package_elem.find("autocare:Weights", self.xml_namespaces)
                    if dimensions_elem:
                        package_data["dimension_unit"] = "in"
                        dimensions_uom = dimensions_elem.get("UOM")  # in or cm
                        for dimension_elem in dimensions_elem:
                            dimension_val = round(self.get_dimension_inches(dimension_elem.text, dimensions_uom), 2)
                            package_data[dimension_elem.tag.replace("{{{0}}}".format(self.xmlns), "").lower()] = dimension_val
                    if weights_elem:
                        package_data["weight_unit"] = "lb"
                        weights_uom = weights_elem.get("UOM")  # pg or gt, gross pounds or gross kilograms
                        for weight_elem in weights_elem:
                            weight_val = round(self.get_weight_pounds(weight_elem.text, weights_uom), 2)
                            package_data[weight_elem.tag.replace("{{{0}}}".format(self.xmlns), "").lower()] = weight_val
                    packages.append(package_data)
        return {
            'packages': packages
        }

    def _parse_xml_digital_assets(self, xml_item):
        assets_elem = xml_item.findall("autocare:DigitalAssets/autocare:DigitalFileInformation", self.xml_namespaces)
        is_image_regex = re.compile("P[0-9]+")
        asset_type_map = {
            'WAR': 'Warranty',
            'OWN': 'Owners Manual',
            'INS': 'Install Instructions',
            'IMG': 'Product Image',
        }
        digital_assets = list()
        display_sequence = 1
        for asset_elem in assets_elem:
            country_elem = asset_elem.find("autocare:Country", self.xml_namespaces)
            if not country_elem or (country_elem and country_elem.text == "US"):
                asset_type = asset_elem.find("autocare:AssetType", self.xml_namespaces).text
                is_image = is_image_regex.match(asset_type)
                if is_image or asset_type in asset_type_map:
                    file_url = asset_elem.find("autocare:URI", self.xml_namespaces).text

                    try:
                        file_size_bytes = int(asset_elem.find("autocare:FileSize", self.xml_namespaces).text)
                    except:
                        file_size_bytes = None

                    file_data = {
                        'url': file_url,
                        'file_size_bytes': file_size_bytes,
                        'display_sequence': 0
                    }
                    if is_image:
                        asset_type_text = asset_type_map["IMG"]
                        if asset_type == "P04":
                            file_data['display_sequence'] = 1
                        else:
                            display_sequence += 1
                            file_data['display_sequence'] = display_sequence
                    else:
                        asset_type_text = asset_type_map[asset_type]
                    file_data['asset_type'] = asset_type_text
                    digital_assets.append(file_data)

        return {
            'digital_assets': digital_assets
        }

    @staticmethod
    def get_dimension_inches(dimension_string_val, unit):
        dimension_val = Decimal(dimension_string_val)
        if unit == "CM":
            return dimension_val * .3937007874  # convert to inches
        return dimension_val

    @staticmethod
    def get_weight_pounds(weight_string_val, unit):
        weight_val = Decimal(weight_string_val)
        if unit == "GT":
            return weight_val * 2.20462  # convert to pounds
        return weight_val


class AcesFileParser(object):
    def __init__(self, aces_flat_file_binary, brand_short_name):
        self.aces_flat_file_binary = aces_flat_file_binary
        self.brand_short_name = brand_short_name
        self.brand_record = Brand.objects.get(short_name=self.brand_short_name)

    def get_fitment_data(self, part_fitment_chunks=10):
        part_num_to_consolidate = None
        parsed_product_fitment = ParsedProductFitment(self.brand_record)
        with SqlLiteTempDb() as sql_cursor:
            self._store_file_in_db(sql_cursor)
            for fitment_row in sql_cursor:
                current_part_num = fitment_row['exppartno']
                if part_num_to_consolidate and part_num_to_consolidate != current_part_num:
                    if len(parsed_product_fitment.part_fitment_storage['storage_objects']) == part_fitment_chunks:
                        parsed_product_fitment._add_years_to_fitment_keys()
                        yield parsed_product_fitment
                        parsed_product_fitment = ParsedProductFitment(self.brand_record)
                part_num_to_consolidate = current_part_num
                parsed_product_fitment.parse_fitment_row(fitment_row)
            if len(parsed_product_fitment.part_fitment_storage['storage_objects']):
                parsed_product_fitment._add_years_to_fitment_keys()
                yield parsed_product_fitment

    def _store_file_in_db(self, sql_cursor):
        """
        The files do not come in sorted.  Store in temp sql_lite DB so we can sort by part number
        Other options were OS level sort, however, if hosting on windows there isn't a great out of the box option.
        Linux does have a great built in sort feature that is reliable and fast.  SQLite strategy is a compromise, but still fast enough.
        """
        logger.info("Storing aces data into SqlliteDB to sort by part number")
        cols = ['catcode', 'year', 'make', 'model', 'submodel', 'engtype', 'liter', 'fuel', 'fueldel', 'asp', 'engvin', 'engdesg', 'dciptdescr', 'expldescr', 'vqdescr', 'fndescr']
        sql_cursor.execute('CREATE TABLE AcesTempStorage (exppartno TEXT NOT NULL, {})'.format(",".join([col + " TEXT NULL" for col in cols])))
        cols.append("exppartno")
        sql_chunks = list()
        reader = csv.DictReader(self.aces_flat_file_binary, delimiter='|', quoting=csv.QUOTE_NONE)
        sql = "INSERT INTO AcesTempStorage ({cols}) VALUES({values})".format(cols=",".join(cols), values=','.join(['?'] * len(cols)))
        sql_cursor.execute("BEGIN")
        num_chunks = 100000
        rows = 0

        def store_chunks(_rows, _sql_chunks):
            row_end = _rows + len(_sql_chunks)
            start = timer()
            sql_cursor.executemany(sql, _sql_chunks)
            logger.info(f"Rows {_rows} - {row_end} stored in SqlliteDB in {timer() - start} seconds")
            return row_end

        for fitment_row in reader:
            values = list()
            for col in cols:
                values.append(fitment_row[col])
            sql_chunks.append(values)
            if len(sql_chunks) == num_chunks:
                rows = store_chunks(rows, sql_chunks)
                sql_chunks = list()
        if len(sql_chunks):
            store_chunks(rows, sql_chunks)
        sql_cursor.execute("COMMIT")
        sql_cursor.execute('CREATE INDEX exppartno ON AcesTempStorage (exppartno)')
        sql_cursor.execute("SELECT * FROM AcesTempStorage ORDER BY exppartno")


class ParsedProductFitment(object):
    def __init__(self, brand_record):
        self.brand_record = brand_record
        self.make_storage = {
            'storage_objects': dict(),
            'makes': set()
        }
        self.model_storage = {
            'storage_objects': dict(),
            'models': set()
        }
        self.sub_model_storage = {
            'storage_objects': dict(),
            'sub_models': set()
        }
        self.engine_storage = {
            'storage_objects': dict(),
            'configurations': set(),
            'engine_codes': set()
        }
        self.vehicle_storage = {
            'storage_objects': dict()
        }
        self.part_fitment_storage = {
            'storage_objects': dict()
        }

    def parse_fitment_row(self, fitment_row):
        year = fitment_row['year']
        if year != 'ALL':
            make_data = self._parse_make_data(fitment_row)
            make, make_key = make_data['make'], make_data['make_key']

            model_data = self._parse_model_data(fitment_row, make)
            model, model_key = model_data['model'], model_data['model_key']

            sub_model_data = self._parse_sub_model_data(fitment_row, make, model, model_key)
            sub_model, sub_model_key = sub_model_data['sub_model'], sub_model_data['sub_model_key']

            engine_key = self._parse_engine_data(fitment_row)
            vehicle_key = self._parse_vehicle_data(fitment_row, make, make_key, model, model_key, sub_model, sub_model_key, engine_key)

            self._parse_vehicle_fitment(fitment_row, vehicle_key)

    def _parse_make_data(self, fitment_row):
        make = fitment_row['make']
        make_key = make
        self.make_storage['makes'].add(make)
        if make_key not in self.make_storage['storage_objects']:
            self.make_storage['storage_objects'][make_key] = {
                'name': make
            }
        return {
            "make": make,
            "make_key": make_key
        }

    def _parse_model_data(self, fitment_row, make):
        model = fitment_row['model']
        self.model_storage['models'].add(model)
        model_key = make + model
        if model_key not in self.model_storage['storage_objects']:
            self.model_storage['storage_objects'][model_key] = {
                'name': model,
                'make': make
            }
        return {
            "model": model,
            "model_key": model_key
        }

    def _parse_sub_model_data(self, fitment_row, make, model, model_key):
        sub_model = fitment_row['submodel']
        sub_model_key = None
        if sub_model:
            self.sub_model_storage['sub_models'].add(sub_model)
            sub_model_key = make + model + sub_model
            if sub_model_key not in self.sub_model_storage['storage_objects']:
                self.sub_model_storage['storage_objects'][sub_model_key] = {
                    'name': sub_model,
                    'model': model_key
                }
        return {
            "sub_model": sub_model,
            "sub_model_key": sub_model_key
        }

    def _parse_engine_data(self, fitment_row):
        engine_configuration = fitment_row['engtype']
        engine_key = None
        if engine_configuration:
            engine_liters = fitment_row['liter'] or None
            if engine_liters:
                engine_liters = Decimal(engine_liters)
            engine_code = fitment_row['engdesg'] or None
            # This only accounts for T and S enumeration for aspiration, I am not sure if there are others
            aspiration = 'N/A'
            if fitment_row['asp'] == 'S':
                aspiration = 'Supercharged'
            elif fitment_row['asp'] == 'T':
                aspiration = 'Turbocharged'
            fuel_type = fitment_row['fuel']
            fuel_delivery = fitment_row['fueldel']
            engine_key = engine_configuration + str(engine_liters or '') + fuel_type + fuel_delivery + (engine_code or '') + aspiration

            self.engine_storage['configurations'].add(engine_configuration)
            if engine_code:
                self.engine_storage['engine_codes'].add(engine_code)
            if engine_key not in self.engine_storage['storage_objects']:
                self.engine_storage['storage_objects'][engine_key] = {
                    'configuration': engine_configuration,
                    'liters': engine_liters,
                    'engine_code': engine_code,
                    'fuel_type': fuel_type,
                    'fuel_delivery': fuel_delivery,
                    'aspiration': aspiration
                }
        return engine_key

    def _parse_vehicle_data(self, fitment_row, make, make_key, model, model_key, sub_model, sub_model_key, engine_key):
        vehicle_key = make + model + (sub_model or '') + (engine_key or '')
        vehicle_year = int(fitment_row['year'])
        if vehicle_key not in self.vehicle_storage['storage_objects']:
            self.vehicle_storage['storage_objects'][vehicle_key] = {
                'make': make_key,
                'model': model_key,
                'sub_model': sub_model_key,
                'engine': engine_key,
                'years': set()
            }
        self.vehicle_storage['storage_objects'][vehicle_key]['years'].add(vehicle_year)
        return vehicle_key

    def _parse_vehicle_fitment(self, fitment_row, vehicle_key):
        part_num = fitment_row['exppartno']
        vehicle_year = int(fitment_row['year'])

        if part_num not in self.part_fitment_storage['storage_objects']:
            self.part_fitment_storage['storage_objects'][part_num] = dict()
        fitment_info_1 = fitment_row['vqdescr']
        fitment_info_2 = fitment_row['fndescr']
        vehicle_fitment_key = vehicle_key + fitment_info_1 + fitment_info_2
        # Filter out engine vin variances.  We may want this data later, but we do not need it now as it takes up storage we will not use
        if vehicle_fitment_key not in self.part_fitment_storage['storage_objects'][part_num]:
            self.part_fitment_storage['storage_objects'][part_num][vehicle_fitment_key] = {
                'product': part_num,
                'vehicle': vehicle_key,
                'years': set(),
                'fitment_info_1': fitment_info_1 or None,
                'fitment_info_2': fitment_info_2 or None
            }
        self.part_fitment_storage['storage_objects'][part_num][vehicle_fitment_key]['years'].add(vehicle_year)

    def _add_years_to_fitment_keys(self):
        part_storage = self.part_fitment_storage['storage_objects']
        test_fitment = dict()

        def add_new_fitment_key(_fitment, _fitment_data, _orig_key, _start_year, _end_year):
            _fitment_data['start_year'] = _start_year
            _fitment_data['end_year'] = _end_year
            _fitment[str(_start_year) + str(_end_year) + _orig_key] = _fitment_data
            test_fitment[str(_start_year) + str(_end_year) + _orig_key] = _fitment_data
            if _orig_key in _fitment:
                del _fitment[_orig_key]

        for part_number, fitment in part_storage.items():
            for vehicle_key in list(fitment.keys()):
                fitment_data = fitment[vehicle_key]
                years = sorted(fitment_data.pop('years'))
                start_year, end_year, prev_year = None, None, None
                num_years = len(years)
                for idx, year in enumerate(years):
                    if prev_year and year - 1 > prev_year:
                        end_year = prev_year
                        add_new_fitment_key(fitment, fitment_data.copy(), vehicle_key, start_year, end_year)
                        start_year, end_year = None, None
                    if start_year is None:
                        start_year = year
                    if not end_year and idx + 1 == num_years:
                        end_year = year
                        add_new_fitment_key(fitment, fitment_data.copy(), vehicle_key, start_year, end_year)
                    prev_year = year
