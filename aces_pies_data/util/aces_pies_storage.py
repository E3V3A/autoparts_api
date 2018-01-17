import csv
from timeit import default_timer as timer
from django.db import transaction

from aces_pies_data.models import DigitalAssetType, DigitalAsset, Brand, Product, Category, ProductFeature, Attribute, AttributeValue, ProductAttribute, ProductDigitalAsset, ProductPackaging, ProductFitment, VehicleMake, VehicleModel, VehicleSubModel, FuelType, \
    FuelDelivery, EngineAspiration, VehicleEngine, Vehicle, VehicleYear, ProductCategoryLookup
from aces_pies_data.util.data_retriever import DataRetriever
import logging
from django.db.models import Q

pies_logger = logging.getLogger("PiesDataStorage")
pies_flat_logger = logging.getLogger("PiesFlatDataStorage")
aces_logger = logging.getLogger("AcesDataStorage")


class PiesDataStorage(object):
    def __init__(self, brand_data):
        self.brand_data = brand_data
        self.brand_records = dict()
        self.category_records = dict()
        self.digital_asset_type_records = dict()

        for brand_record in Brand.objects.all():
            self.brand_records[brand_record.name] = brand_record

    def store_brand_data(self, on_complete=None):
        pies_logger.info("Storing pies product data for brand {}".format(self.brand_data['brand']))
        begin_timer = timer()
        brand_record = self._get_brand_record(self.brand_data)
        products = list()
        num_products_to_store = 50
        for product_data in self.brand_data['product_data']:
            products.append(product_data)
            if len(products) == num_products_to_store:
                self._store_products(products, brand_record)
                products = list()
        if len(products) > 0:
            self._store_products(products, brand_record)
        if on_complete:
            on_complete()
        pies_logger.info('Total time for {0}: {1}'.format(self.brand_data['brand'], timer() - begin_timer))

    @transaction.atomic
    def _store_products(self, products, brand_record):
        products_to_create = {product['part_number']: product for product in products}
        products_to_update = dict()
        part_categories_lookup = dict()
        if not ProductCategoryLookup.objects.filter(brand_short_name=self.brand_data['brand_short_name']).exists():
            raise RuntimeError("Cannot parse pies data if no pies categories have been stored")

        part_categories_queryset = ProductCategoryLookup.objects.filter(part_number__in=products_to_create.keys(), brand_short_name=self.brand_data['brand_short_name']).select_related("category")
        for part_category in part_categories_queryset:
            part_categories_lookup[part_category.part_number] = part_category.category

        existing_product_records = Product.objects.filter(part_number__in=products_to_create.keys(), brand=brand_record).prefetch_related('features').prefetch_related('attributes').prefetch_related('attributes__attribute').prefetch_related('attributes__value').prefetch_related(
            'packages').prefetch_related('digital_assets').prefetch_related('digital_assets__digital_asset').prefetch_related('digital_assets__digital_asset__type').all()
        for existing_product_record in existing_product_records:
            product_data_to_update = products_to_create.pop(existing_product_record.part_number)
            if self._prepare_for_update(product_data_to_update, existing_product_record, part_categories_lookup):
                products_to_update[existing_product_record.part_number] = product_data_to_update
        if len(products_to_create):
            self._bulk_create_products(products_to_create, brand_record, part_categories_lookup)
        if len(products_to_update):
            self._bulk_update_products(products_to_update)

    def _bulk_create_products(self, product_lookup, brand_record, part_categories_lookup):
        products_to_create = list()
        for product_data in product_lookup.values():
            if product_data['part_number'] in part_categories_lookup:
                products_to_create.append(
                    Product(
                        part_number=product_data['part_number'], name=product_data['name'], is_hazardous=product_data['is_hazardous'], is_carb_legal=product_data['is_carb_legal'], is_discontinued=product_data['is_discontinued'],
                        is_obsolete=product_data['is_obsolete'], map_price=product_data['map_price'], retail_price=product_data['retail_price'], brand=brand_record, category=part_categories_lookup.get(product_data['part_number'], None)
                    )
                )
            else:
                pies_logger.warning(f"No category info found for {product_data['part_number']} for brand {self.brand_data['brand']}, skipping")
        if products_to_create:
            Product.objects.bulk_create(products_to_create)
            created_products = Product.objects.filter(part_number__in=product_lookup.keys(), brand=brand_record).all()
            for created_product in created_products:
                product_lookup[created_product.part_number]['product_record'] = created_product
            # filter out any product data that didn't create an actual record.  This can happen if category is missing.
            product_lookup = {part_number: product_data for part_number, product_data in product_lookup.items() if 'product_record' in product_data}
            self._bulk_create_relationships(product_lookup)

    def _bulk_update_products(self, product_lookup):
        for part_number, product_data in product_lookup.items():
            product_data['product_record'].save()
        self._bulk_create_relationships(product_lookup)

    def _bulk_create_relationships(self, product_lookup):
        self._bulk_create_features(product_lookup)
        self._bulk_create_attributes(product_lookup)
        self._bulk_create_digital_assets(product_lookup)
        self._bulk_create_packaging(product_lookup)

    def _prepare_for_update(self, product_data, product_record, part_categories_lookup):
        product_change_manager = ProductChangeManager(product_record)
        product_change_manager.prepare_for_update(product_data, part_categories_lookup)
        product_data['product_record'] = product_record

    def _bulk_create_features(self, product_lookup):
        features_to_create = list()
        for product_data in product_lookup.values():
            features = product_data['features']
            if features:
                for idx, feature in enumerate(features):
                    features_to_create.append(ProductFeature(name=feature, listing_sequence=idx, product=product_data['product_record']))
        if features_to_create:
            ProductFeature.objects.bulk_create(features_to_create)

    def _bulk_create_attributes(self, product_lookup):
        category_records = set()
        attribute_names = set()
        attribute_values = set()
        attribute_objects = dict()
        attribute_value_objects = dict()
        for product_data in product_lookup.values():
            attributes = product_data['attributes']
            if attributes:
                category_record = product_data['product_record'].category
                category_records.add(category_record)
                for attribute in attributes:
                    attribute_names.add(attribute['type'])
                    attribute_values.add(attribute['value'])
                    attribute_key = category_record.name + attribute['type']
                    attribute_objects[attribute_key] = {
                        'name': attribute['type'],
                        'category': category_record
                    }
                    attribute_value_key = attribute_key + attribute['value']
                    attribute_value_objects[attribute_value_key] = {
                        'value': attribute['value'],
                        'attribute': attribute_key
                    }
        if attribute_objects:
            attribute_retriever = DataRetriever(Attribute, Attribute.objects.filter(name__in=attribute_names, category__in=category_records).select_related('category'), ('category__name', 'name',))
            attribute_records = attribute_retriever.bulk_get_or_create(attribute_objects)
            for attribute_value_config in attribute_value_objects.values():
                attribute_value_config['attribute_id'] = attribute_records[attribute_value_config.pop('attribute')]
            attribute_value_retriever = DataRetriever(AttributeValue, AttributeValue.objects.filter(value__in=attribute_values, attribute_id__in=attribute_records.values()).select_related('attribute').select_related('attribute__category'), ('attribute__category__name', 'attribute__name', 'value',))
            attribute_value_records = attribute_value_retriever.bulk_get_or_create(attribute_value_objects)
            product_attributes_to_create = list()
            for product_data in product_lookup.values():
                attributes = product_data['attributes']
                if attributes:
                    product_record = product_data['product_record']
                    category_record = product_record.category
                    category_name = category_record.name
                    for attribute in attributes:
                        attribute_id = attribute_records.get(category_name + attribute['type'])
                        attribute_value_id = attribute_value_records.get(category_name + attribute['type'] + attribute['value'])
                        product_attributes_to_create.append(ProductAttribute(attribute_id=attribute_id, value_id=attribute_value_id, product=product_record))
            if product_attributes_to_create:
                ProductAttribute.objects.bulk_create(product_attributes_to_create)

    def _bulk_create_digital_assets(self, product_lookup):
        product_assets_to_create = list()
        product_asset_objects = dict()
        digital_asset_objects = dict()
        urls = set()

        def _append_digital_asset(_part_number, _asset, _digital_asset_type_record):
            urls.add(_asset['url'])
            digital_asset_key = _asset['url'] + _digital_asset_type_record.name
            digital_asset_objects[digital_asset_key] = {
                'url': _asset['url'],
                'file_size_bytes': _asset['file_size_bytes'],
                'type': _digital_asset_type_record
            }
            if _part_number not in product_asset_objects:
                product_asset_objects[_part_number] = list()
            product_asset_objects[_part_number].append({
                'url': _asset['url'],
                'asset_type': _digital_asset_type_record.name,
                'display_sequence': _asset['display_sequence']
            })

        for product_data in product_lookup.values():
            digital_assets = product_data['digital_assets']
            if digital_assets:
                for digital_asset in digital_assets:
                    digital_asset_type_record = self._get_digital_asset_type_record(digital_asset['asset_type'])
                    _append_digital_asset(product_data['part_number'], digital_asset, digital_asset_type_record)
        if urls:
            digital_assets_retriever = DataRetriever(DigitalAsset, DigitalAsset.objects.filter(url__in=urls), ('url', 'type__name',))
            digital_asset_records = digital_assets_retriever.bulk_get_or_create(digital_asset_objects)
            for part_number, product_assets in product_asset_objects.items():
                product_record = product_lookup[part_number]['product_record']
                for product_asset in product_assets:
                    digital_asset_id = digital_asset_records[product_asset['url'] + product_asset['asset_type']]
                    product_assets_to_create.append(ProductDigitalAsset(digital_asset_id=digital_asset_id, display_sequence=product_asset['display_sequence'], product=product_record))

        if product_assets_to_create:
            ProductDigitalAsset.objects.bulk_create(product_assets_to_create)

    def _bulk_create_packaging(self, product_lookup):
        product_packaging_to_create = list()
        db_col_override = {
            'dimensionalweight': 'dimensional_weight'
        }
        for product_data in product_lookup.values():
            product_packaging = product_data['packages']
            if product_packaging:
                product_record = product_data['product_record']
                for product_package in product_packaging:
                    product_package_record = ProductPackaging(product=product_record, product_quantity=product_package['quantity'])
                    for dimension_type, value in product_package.items():
                        db_col = dimension_type
                        if dimension_type in db_col_override:
                            db_col = db_col_override['dimensionalweight']
                        setattr(product_package_record, db_col, value)
                    product_packaging_to_create.append(product_package_record)
        if product_packaging_to_create:
            ProductPackaging.objects.bulk_create(product_packaging_to_create)

    def _get_brand_record(self, brand_data):
        brand_name = brand_data['brand']
        brand_short_name = brand_data['brand_short_name']
        if brand_name not in self.brand_records:
            logo, marketing_copy = brand_data['logo'], brand_data['marketing_copy']
            digital_asset_record = None
            if logo:
                digital_asset_record = self._create_digital_asset(logo['url'], logo['file_size_bytes'], self._get_digital_asset_type_record('Brand Logo'))
            self.brand_records[brand_name] = Brand.objects.create(name=brand_name, short_name=brand_short_name, logo=digital_asset_record, marketing_copy=marketing_copy)
        return self.brand_records[brand_name]

    def _get_digital_asset_type_record(self, digital_asset_type):
        if digital_asset_type not in self.digital_asset_type_records:
            digital_asset_type_record = DigitalAssetType.objects.get_or_create(name=digital_asset_type)[0]
            self.digital_asset_type_records[digital_asset_type] = digital_asset_type_record
        return self.digital_asset_type_records[digital_asset_type]

    def _create_digital_asset(self, url, file_size_bytes, digital_asset_type_record):
        return DigitalAsset.objects.create(url=url, file_size_bytes=file_size_bytes, type=digital_asset_type_record)


class ProductChangeManager(object):
    def __init__(self, product_record):
        self.product_record = product_record

    def prepare_for_update(self, product_data, part_categories_lookup):
        do_update = False
        related_fields = ('attributes', 'features', 'packages', 'digital_assets',)
        ignore_fields = ('brand_name', 'product_record',)
        product_skip_fields = related_fields + ignore_fields
        for field, value in product_data.items():
            if field not in product_skip_fields and getattr(self.product_record, field) != value:
                setattr(self.product_record, field, value)
                do_update = True
            elif field in related_fields:
                perform_update = getattr(self, '_prepare_update_' + field)(value)
                if perform_update:
                    do_update = True
                if not perform_update:
                    product_data[field] = None  # null out the field so it does not attempt to get inserted on the update
        category = part_categories_lookup.get(self.product_record.part_number, None)
        if category and self.product_record.category_id != category.id:
            raise NotImplementedError("Implementation needed if products change categories")
        return do_update

    def _prepare_update_features(self, features):
        prepare_update = self._are_there_differences(features, self.product_record.features, ('name',))
        if prepare_update:
            self.product_record.features.all().delete()
        return prepare_update

    def _prepare_update_attributes(self, attributes):
        # compare all attributes, store actual attribute objects for potential delete later
        attribute_records = list()
        sorted_attributes = sorted(attributes, key=lambda row: (row['type'], row['value']))
        num_attributes = len(sorted_attributes)
        found_differences = num_attributes != self.product_record.attributes.count()

        for idx, product_attribute in enumerate(self.product_record.attributes.all()):
            if idx < num_attributes - 1:
                sorted_attribute = sorted_attributes[idx]
                attribute_records.append(product_attribute.attribute)
                if product_attribute.attribute.name != sorted_attribute['type'] or product_attribute.value.value != sorted_attribute['value']:
                    found_differences = True
        if found_differences:
            self.product_record.attributes.all().delete()
            for attribute_record in attribute_records:
                if not Product.objects.filter(attributes__attribute=attribute_record).exists():
                    attribute_record.delete()
        return found_differences

    def _prepare_update_packages(self, packages):
        keys_to_compare = ('weight', 'dimensionalweight', 'height', 'length', 'width',)
        db_col_map = {
            'dimensionalweight': 'dimensional_weight'
        }
        prepare_update = self._are_there_differences(packages, self.product_record.packages, keys_to_compare, db_col_map)
        if prepare_update:
            self.product_record.packages.all().delete()
        return prepare_update

    def _prepare_update_digital_assets(self, digital_assets):
        sorted_new_digital_assets = sorted(digital_assets, key=lambda k: [k['display_sequence'], k['asset_type']])
        sorted_db_digital_assets = list()

        for digital_asset_record in self.product_record.digital_assets.all():
            sorted_db_digital_assets.append({
                'asset_type': digital_asset_record.digital_asset.type.name,
                'display_sequence': digital_asset_record.display_sequence,
                'file_size_bytes': digital_asset_record.digital_asset.file_size_bytes,
                'url': digital_asset_record.digital_asset.url
            })
        sorted_db_digital_assets = sorted(sorted_db_digital_assets, key=lambda k: [k['display_sequence'], k['asset_type']])
        found_differences = sorted_new_digital_assets != sorted_db_digital_assets
        if found_differences:
            self.product_record.digital_assets.all().delete()
        return found_differences

    def _are_there_differences(self, data, queryset, keys_to_compare, db_col_map=None):
        if not db_col_map:
            db_col_map = dict()
        found_differences = False
        num_existing_data = queryset.count()
        if num_existing_data != len(data):
            found_differences = True
        else:
            for idx, existing_data_item in enumerate(queryset.all()):
                new_item = data[idx]
                for key in keys_to_compare:
                    db_col = key
                    if key in db_col_map:
                        db_col = db_col_map[key]
                    if isinstance(new_item, dict):
                        val_to_compare = new_item.get(key, None)
                    else:
                        val_to_compare = new_item
                    if getattr(existing_data_item, db_col) != val_to_compare:
                        found_differences = True
                        break
        return found_differences


class AcesDataStorage(object):
    def __init__(self, aces_file_parser):
        self.aces_file_parser = aces_file_parser
        self.fuel_type_lookup = dict()
        self.fuel_delivery_lookup = dict()
        self.aspiration_lookup = dict()

    def store_brand_fitment(self, on_complete=None):
        aces_logger.info(f'Storing aces fitment for brand {self.aces_file_parser.brand_record.name}')
        begin_timer = timer()
        for fitment_data in self.aces_file_parser.get_fitment_data(30):
            # Only start storing data if there any makes were parsed, some part numbers fit ALL and will not have associated makes/models/etc
            if fitment_data.make_storage['makes']:
                self._clean_and_store_data(fitment_data)
        if on_complete:
            on_complete()
        aces_logger.info(f'Total time for {self.aces_file_parser.brand_record.name}: {timer() - begin_timer}')

    @transaction.atomic
    def _clean_and_store_data(self, fitment_data):
        part_fitment_storage = self._clean_fitment_data(fitment_data)
        if part_fitment_storage['storage_objects']:
            aces_logger.info('Storing fitment for parts {}'.format(",".join(list(fitment_data.part_fitment_storage['storage_objects'].keys()))))
            self._store_data(fitment_data)

    def _store_data(self, fitment_data):
        make_records = self._get_make_records(fitment_data)
        model_records = self._get_model_records(fitment_data, make_records)
        sub_model_records = self._get_sub_model_records(fitment_data, model_records)
        engine_records = self._get_engine_records(fitment_data)
        vehicle_records = self._get_vehicle_records(fitment_data, make_records, model_records, sub_model_records, engine_records)
        make_records.clear()
        model_records.clear()
        if sub_model_records:
            sub_model_records.clear()

        if engine_records:
            engine_records.clear()

        self._store_fitment(fitment_data, vehicle_records)

    def _clean_fitment_data(self, fitment_data):
        """
        This method cleans the fitment data.
        1. If the fitment_data input is the same as database, do not store
        2. If the fitment_data and database differ, delete existing records from the database and re-insert new records
        3. If the fitment_data is storing a part that does not exist, remove it

        """
        existing_fitment_lookup = dict()
        part_fitment_storage = fitment_data.part_fitment_storage
        existing_product_lookup = Product.objects.filter(brand=fitment_data.brand_record, part_number__in=part_fitment_storage['storage_objects'].keys()).values_list("part_number", flat=True)
        part_fitment_storage['storage_objects'] = {key: value for key, value in part_fitment_storage['storage_objects'].items() if key in existing_product_lookup}
        existing_fitment_records = ProductFitment.objects.filter(product__brand=fitment_data.brand_record, product__part_number__in=part_fitment_storage['storage_objects'].keys())
        existing_fitment_records = existing_fitment_records.select_related("product", "vehicle", "vehicle__make", "vehicle__model", "vehicle__sub_model", "vehicle__engine", "vehicle__engine__fuel_delivery", "vehicle__engine__fuel_type", "vehicle__engine__aspiration")
        vehicle_key_parts = [
            "vehicle__make__name", "vehicle__model__name", "vehicle__sub_model__name", "vehicle__engine__configuration", "vehicle__engine__liters", "vehicle__engine__fuel_type__name", "vehicle__engine__fuel_delivery__name", "vehicle__engine__engine_code", "vehicle__engine__aspiration__name"
        ]
        vehicle_fitment_key_parts = vehicle_key_parts + ["fitment_info_1", "fitment_info_2"]
        existing_fitment_records = existing_fitment_records.values(*(["id", "product__part_number", "start_year", "end_year"] + vehicle_fitment_key_parts))
        existing_fitment_record_lookup = dict()
        for existing_fitment_record in existing_fitment_records:
            vehicle_fitment_key = DataRetriever.get_record_key(existing_fitment_record, vehicle_fitment_key_parts)
            vehicle_key = DataRetriever.get_record_key(existing_fitment_record, vehicle_key_parts)
            part_number = existing_fitment_record['product__part_number']
            if part_number not in existing_fitment_lookup:
                existing_fitment_lookup[part_number] = dict()
            existing_fitment_record_lookup[part_number] = existing_fitment_record
            existing_fitment_lookup[part_number][vehicle_fitment_key] = {
                'product': part_number,
                'vehicle': vehicle_key,
                'start_year': existing_fitment_record['start_year'],
                'end_year': existing_fitment_record['end_year'],
                'fitment_info_1': existing_fitment_record['fitment_info_1'],
                'fitment_info_2': existing_fitment_record['fitment_info_2']
            }
        product_fitment_to_delete = list()
        if existing_fitment_lookup:
            for part_number in list(part_fitment_storage['storage_objects'].keys()):
                if part_number in existing_fitment_lookup:
                    new_part_fitment_storage = part_fitment_storage['storage_objects'][part_number]
                    existing_part_fitment_storage = existing_fitment_lookup[part_number]
                    if new_part_fitment_storage == existing_part_fitment_storage:
                        del part_fitment_storage['storage_objects'][part_number]
                    else:
                        product_fitment_to_delete.append(existing_fitment_record_lookup[part_number]['id'])
        if product_fitment_to_delete:
            ProductFitment.objects.filter(id__in=product_fitment_to_delete).delete()
        return part_fitment_storage

    def _get_make_records(self, fitment_data):
        make_retriever = DataRetriever(VehicleMake, VehicleMake.objects.filter(name__in=fitment_data.make_storage['makes']), ('name',))
        return make_retriever.bulk_get_or_create(fitment_data.make_storage['storage_objects'])

    def _get_model_records(self, fitment_data, make_records):
        model_storage = fitment_data.model_storage['storage_objects']
        for model_key, model_object in model_storage.items():
            model_object["make_id"] = make_records[model_object.pop("make")]

        model_retriever = DataRetriever(VehicleModel, VehicleModel.objects.filter(name__in=fitment_data.model_storage['models'], make_id__in=make_records.values()).select_related("make"), ("make__name", "name",))
        return model_retriever.bulk_get_or_create(model_storage)

    def _get_sub_model_records(self, fitment_data, model_records):
        sub_model_storage = fitment_data.sub_model_storage['storage_objects']
        if sub_model_storage:
            for sub_model_key, sub_model_object in sub_model_storage.items():
                sub_model_object['model_id'] = model_records[sub_model_object.pop('model')]
            sub_model_retriever = DataRetriever(VehicleSubModel, VehicleSubModel.objects.filter(name__in=fitment_data.sub_model_storage['sub_models'], model_id__in=model_records.values()).select_related("model__make"), ("model__make__name", "model__name", "name",))
            return sub_model_retriever.bulk_get_or_create(sub_model_storage)
        return None

    def _get_engine_records(self, fitment_data):
        engine_storage = fitment_data.engine_storage['storage_objects']
        if engine_storage:
            for engine_key, engine_object in engine_storage.items():
                engine_object['fuel_type'] = self._get_fuel_type(engine_object['fuel_type'])
                engine_object['fuel_delivery'] = self._get_fuel_delivery(engine_object['fuel_delivery'])
                engine_object['aspiration'] = self._get_aspiration(engine_object['aspiration'])
            engine_retriever = DataRetriever(VehicleEngine, VehicleEngine.objects.filter(configuration__in=fitment_data.engine_storage['configurations']).filter(Q(engine_code__isnull=True) | Q(engine_code__in=fitment_data.engine_storage['engine_codes'])).select_related(),
                                             ("configuration", "liters", "fuel_type__name", "fuel_delivery__name", "engine_code", "aspiration__name"))
            return engine_retriever.bulk_get_or_create(engine_storage)
        return None

    def _get_vehicle_records(self, fitment_data, make_records, model_records, sub_model_records, engine_records):
        vehicle_storage = fitment_data.vehicle_storage['storage_objects']
        vehicle_year_storage = dict()
        for vehicle_key, vehicle_object in vehicle_storage.items():
            vehicle_object['make_id'] = make_records[vehicle_object.pop('make')]
            vehicle_object['model_id'] = model_records[vehicle_object.pop('model')]
            if vehicle_object['sub_model'] and sub_model_records:
                vehicle_object['sub_model_id'] = sub_model_records[vehicle_object.pop('sub_model')]
            if vehicle_object['engine'] and engine_records:
                vehicle_object['engine_id'] = engine_records[vehicle_object.pop('engine')]
            vehicle_start_year = vehicle_object.pop('start_year')
            vehicle_end_year = vehicle_object.pop('end_year')
            vehicle_year_key = str(vehicle_start_year) + str(vehicle_end_year) + vehicle_key
            vehicle_year_storage[vehicle_year_key] = {
                'vehicle': vehicle_key,
                'start_year': vehicle_start_year,
                'end_year': vehicle_end_year
            }
        vehicle_retriever = DataRetriever(
            Vehicle,
            Vehicle.objects.filter(model_id__in=model_records.values()).select_related("make", "model", "sub_model", "engine", "engine__fuel_delivery", "engine__fuel_type", "engine__aspiration"),
            ("make__name", "model__name", "sub_model__name", "engine__configuration", "engine__liters", "engine__fuel_type__name", "engine__fuel_delivery__name", "engine__engine_code", "engine__aspiration__name")
        )
        vehicle_records = vehicle_retriever.bulk_get_or_create(vehicle_storage)
        for vehicle_year_object in vehicle_year_storage.values():
            vehicle_year_object['vehicle_id'] = vehicle_records[vehicle_year_object.pop('vehicle')]

        vehicle_year_retriever = DataRetriever(
            VehicleYear,
            VehicleYear.objects.filter(vehicle_id__in=vehicle_records.values()).select_related("vehicle", "vehicle__make", "vehicle__model", "vehicle__sub_model", "vehicle__engine", "vehicle__engine__fuel_delivery", "vehicle__engine__fuel_type", "vehicle__engine__aspiration"),
            (
                "start_year", "end_year", "vehicle__make__name", "vehicle__model__name", "vehicle__sub_model__name", "vehicle__engine__configuration", "vehicle__engine__liters", "vehicle__engine__fuel_type__name", "vehicle__engine__fuel_delivery__name", "vehicle__engine__engine_code",
                "vehicle__engine__aspiration__name")
        )
        vehicle_year_retriever.bulk_get_or_create(vehicle_year_storage)
        return vehicle_records

    def _store_fitment(self, fitment_data, vehicle_records):
        fitment_storage_objects = fitment_data.part_fitment_storage['storage_objects']
        product_retriever = DataRetriever(Product, Product.objects.filter(brand=fitment_data.brand_record, part_number__in=fitment_storage_objects.keys()), ("part_number",))
        product_fitment_objects = list()
        for part_number, storage_objects in fitment_storage_objects.items():
            for storage_object in storage_objects.values():
                storage_object['product_id'] = product_retriever.get_instance(storage_object.pop('product'))
                storage_object['vehicle_id'] = vehicle_records[storage_object.pop('vehicle')]
                product_fitment_objects.append(ProductFitment(**storage_object))
        if product_fitment_objects:
            ProductFitment.objects.bulk_create(product_fitment_objects)

    def _get_fuel_type(self, fuel_type):
        fuel_type_record = self.fuel_type_lookup.get(fuel_type, None)
        if not fuel_type_record:
            fuel_type_record = FuelType.objects.get_or_create(name=fuel_type)[0]
            self.fuel_type_lookup[fuel_type] = fuel_type_record
        return fuel_type_record

    def _get_fuel_delivery(self, fuel_delivery):
        fuel_delivery_record = self.fuel_delivery_lookup.get(fuel_delivery, None)
        if not fuel_delivery_record:
            fuel_delivery_record = FuelDelivery.objects.get_or_create(name=fuel_delivery)[0]
            self.fuel_delivery_lookup[fuel_delivery] = fuel_delivery_record
        return fuel_delivery_record

    def _get_aspiration(self, aspiration):
        aspiration_record = self.aspiration_lookup.get(aspiration, None)
        if not aspiration_record:
            aspiration_record = EngineAspiration.objects.get_or_create(name=aspiration)[0]
            self.aspiration_lookup[aspiration] = aspiration_record
        return aspiration_record


class PiesCategoryDataStorage(object):
    def __init__(self, pies_flat_binary, brand_short_name):
        self.pies_flat_binary = pies_flat_binary
        self.brand_short_name = brand_short_name

    def store_category_data(self, on_complete=None):
        reader = csv.DictReader(self.pies_flat_binary, delimiter='|', quoting=csv.QUOTE_NONE)
        categories = set()
        parts_categories = dict()
        part_numbers = set()
        num_chunks = 100
        pies_flat_logger.info(f"Storing category lookup for brand {self.brand_short_name}")
        for row in reader:
            category = row['partterminologyname']
            part_number = row['PartNumber']
            part_category_key = self.brand_short_name + category + part_number
            categories.add(category)
            parts_categories[part_category_key] = {
                "brand_short_name": self.brand_short_name,
                "part_number": part_number,
                "category": category
            }
            part_numbers.add(part_number)
            if len(parts_categories) == num_chunks:
                self._store_chunks(categories, parts_categories, part_numbers)
                parts_categories = dict()
                categories = set()
                part_numbers = set()
        if len(parts_categories):
            self._store_chunks(categories, parts_categories, part_numbers)
        if on_complete:
            on_complete()

    @transaction.atomic
    def _store_chunks(self, categories, parts_categories, part_numbers):
        category_records = dict()
        existing_categories = Category.objects.filter(name__in=categories)
        for existing_category in existing_categories:
            category_records[existing_category.name] = existing_category

        for category in categories:
            if category not in category_records:
                category_records[category] = Category.objects.create(name=category)

        for part_category_data in parts_categories.values():
            part_category_data['category'] = category_records[part_category_data.pop("category")]

        part_category_retriever = DataRetriever(ProductCategoryLookup, ProductCategoryLookup.objects.filter(part_number__in=part_numbers).select_related("category"), ("brand_short_name", "category__name", "part_number",))
        part_category_retriever.bulk_get_or_create(parts_categories)
