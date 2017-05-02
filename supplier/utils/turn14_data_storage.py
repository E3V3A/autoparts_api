import re
from decimal import Decimal

import time

import logging
from django.db import transaction

from supplier.models import Vendor, VendorProductLine, Category, Product, ProductCategory, ProductImage, ProductFitment, VehicleYear, VehicleMake, VehicleModel, VehicleEngine, VehicleSubModel, Vehicle
from bulk_update.helper import bulk_update

logger = logging.getLogger(__name__)


class Turn14DataStorage:
    def __init__(self):
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
            'long_description': {
                'model': 'long_description',
                'serializer': None
            },
            'name': {
                'model': 'name',
                'serializer': None
            },
            'is_carb_legal': {
                'model': 'is_carb_legal',
                'serializer': None
            },
            'cost': {
                'model': 'cost',
                'serializer': lambda val: self.string_to_decimal(val)
            },
            'primary_img_thumb': {
                'model': 'remote_image_thumb',
                'serializer': None
            }
        }

    @transaction.atomic
    def update_stock(self, parts_to_update):
        products = Product.objects.filter(internal_part_num__in=parts_to_update.keys()).all()
        for product in products:
            product.stock = parts_to_update[product.internal_part_num]
        bulk_update(products, update_fields=['stock'])

    @transaction.atomic
    def optimized_save(self, data):
        vendor_objects, vendor_names = list(), list()
        product_line_objects, product_line_names = list(), list()
        category_objects, category_names = list(), list()
        sub_category_objects, sub_category_names = list(), list()
        product_data_lookup = dict()
        for data_item in data:
            if data_item['is_valid_item']:
                internal_part_num, vendor, product_line = data_item["InternalPartNumber"], data_item["PrimaryVendor"], data_item["product_line"]
                category, sub_category = data_item['category'], data_item['sub_category']
                product_data_lookup[internal_part_num] = {
                    "category": category,
                    "sub_category": sub_category
                }
                if vendor not in vendor_names:
                    vendor_objects.append({"name": vendor})
                    vendor_names.append(vendor)
                if product_line and product_line not in product_line_names:
                    product_line_objects.append({"name": product_line, "vendor": vendor})
                    product_line_names.append(product_line)
                if category and category not in category_names:
                    category_names.append(category)
                    category_objects.append({"name": category, "parent_category": None})
                if sub_category and sub_category not in sub_category_names:
                    sub_category_names.append(sub_category)
                    sub_category_objects.append({"name": sub_category, "parent_category": category})

        vendor_retriever = DataRetriever(Vendor, Vendor.objects.filter(name__in=vendor_names))
        vendor_records = vendor_retriever.bulk_get_or_create(vendor_objects, ("name",))
        for product_line_object in product_line_objects:
            product_line_object["vendor"] = vendor_records[product_line_object["vendor"]]
        product_line_retriever = DataRetriever(VendorProductLine, VendorProductLine.objects.filter(name__in=product_line_names).select_related("vendor"))
        product_line_records = product_line_retriever.bulk_get_or_create(product_line_objects, ("name", "vendor__name"))

        category_retriever = DataRetriever(Category, Category.objects.filter(name__in=category_names, parent_category=None))
        category_records = category_retriever.bulk_get_or_create(category_objects, ("name",))
        for sub_category_object in sub_category_objects:
            sub_category_object["parent_category"] = category_records[sub_category_object["parent_category"]]
        sub_category_retriever = DataRetriever(Category, Category.objects.filter(name__in=sub_category_names, parent_category__name__isnull=False).select_related("parent_category"))
        sub_category_records = sub_category_retriever.bulk_get_or_create(sub_category_objects, ("name", "parent_category__name"))

        product_lookup = dict()
        existing_product_records = Product.objects.filter(internal_part_num__in=product_data_lookup.keys()).all()
        for existing_product in existing_product_records:
            product_lookup[existing_product.internal_part_num] = existing_product
        products_to_create = list()
        internal_part_nums_to_create = list()
        for data_item in data:
            if data_item['is_valid_item']:
                internal_part_num = data_item["InternalPartNumber"]
                if internal_part_num not in product_lookup:
                    product_data = product_data_lookup[internal_part_num]
                    product_data['images'] = data_item['images']
                    product_data['fitment'] = data_item['fitment']
                    internal_part_nums_to_create.append(internal_part_num)
                    vendor, product_line = data_item["PrimaryVendor"], data_item["product_line"]
                    product_args = {
                        'internal_part_num': internal_part_num,
                        'vendor': vendor_records[vendor]
                    }
                    if product_line:
                        product_args['vendor_product_line'] = product_line_records[product_line + vendor]
                    for key, value in data_item.items():
                        if key in self.product_data_mapping:
                            data_mapper = self.product_data_mapping[key]
                            product_args[data_mapper['model']] = data_mapper['serializer'](value) if data_mapper['serializer'] is not None else value
                    products_to_create.append(Product(**product_args))
        if products_to_create:
            Product.objects.bulk_create(products_to_create)
            created_products = Product.objects.filter(internal_part_num__in=internal_part_nums_to_create).all()
            product_categories_to_create = list()
            product_images_to_create = list()
            for created_product in created_products:
                product_data = product_data_lookup[created_product.internal_part_num]
                category, sub_category = product_data["category"], product_data["sub_category"]
                if category:
                    category_record = category_records[category]
                    product_categories_to_create.append(ProductCategory(product=created_product, category=category_record))
                    if sub_category:
                        sub_category_record = sub_category_records[sub_category + category]
                        product_categories_to_create.append(ProductCategory(product=created_product, category=sub_category_record))
                product_images_to_create += self._get_images_to_store(created_product, product_data['images'])
                self._store_product_fitment(created_product, product_data['fitment'])
            if product_categories_to_create:
                ProductCategory.objects.bulk_create(product_categories_to_create)
            if product_images_to_create:
                ProductImage.objects.bulk_create(product_images_to_create)

    @transaction.atomic
    def save(self, data_item):
        # TODO can speed this up by doing bulk creates or updates
        vendor = Vendor.objects.get_or_create(name=data_item["PrimaryVendor"])[0]
        product_args = {
            'vendor': vendor
        }
        product_line = None
        if data_item['product_line']:
            product_line = VendorProductLine.objects.get_or_create(vendor=vendor, name=data_item['product_line'])[0]
        product_args['vendor_product_line'] = product_line
        for key, value in data_item.items():
            if key in self.product_data_mapping:
                data_mapper = self.product_data_mapping[key]
                product_args[data_mapper['model']] = data_mapper['serializer'](value) if data_mapper['serializer'] is not None else value
        category_records = []
        if data_item['category']:
            category_records.append(Category.objects.get_or_create(name=data_item['category'], parent_category=None)[0])
            if data_item['sub_category']:
                category_records.append(Category.objects.get_or_create(name=data_item['sub_category'], parent_category=category_records[0])[0])
        product_record = Product.objects.update_or_create(internal_part_num=product_args['internal_part_num'], defaults=product_args)[0]
        for category_record in category_records:
            ProductCategory.objects.get_or_create(product=product_record, category=category_record)
        self._store_remote_images(product_record, data_item['images'])
        self._store_product_fitment(product_record, data_item['fitment'])

    def _get_images_to_store(self, product_record, images):
        images_to_create = list()
        for image_stack in images:
            img_url = image_stack['large_img'] if image_stack['large_img'] else image_stack['med_img']
            if img_url:
                images_to_create.append(ProductImage(product=product_record, is_primary=image_stack['is_primary'], remote_image_file=img_url))
        return images_to_create

    def _store_remote_images(self, product_record, images):
        ProductImage.objects.filter(product=product_record).delete()
        create_objs = list()
        for image_stack in images:
            img_url = image_stack['large_img'] if image_stack['large_img'] else image_stack['med_img']
            if img_url:
                create_objs.append(ProductImage(product=product_record, is_primary=image_stack['is_primary'], remote_image_file=img_url))
        if create_objs:
            ProductImage.objects.bulk_create(create_objs)

    @staticmethod
    def _fitment_exists(fitment_item, existing_fitment_models):
        for fitment_model in existing_fitment_models:
            v = fitment_model.vehicle
            if (
                                                v.make.name == fitment_item['make']
                                        and v.model.name == fitment_item['model']
                                    and v.sub_model.name == fitment_item['sub_model']
                                and v.engine.name == fitment_item['engine']
                            and fitment_model.start_year == fitment_item['start_year']
                        and fitment_model.end_year == fitment_item['end_year']
                    and fitment_model.note == fitment_item['note']
            ):
                return True
        return False

    @staticmethod
    def _vehicle_has_year(vehicle_id, year, vehicle_year_models):
        for vehicle_year in vehicle_year_models:
            if vehicle_year.vehicle_id == vehicle_id and vehicle_year.year == year:
                return True
        return False

    def _get_vehicle(self, make, model, sub_model, engine, vehicle_models):
        vehicle = None
        for v in vehicle_models:
            if v.make.name == make and v.model.name == model and v.sub_model.name == sub_model and v.engine.name == engine:
                vehicle = v
        if not vehicle:
            vehicle = self.store_or_get_vehicle(make, model, sub_model, engine)
        key = "%s-%s-%s-%s" % (make, model, sub_model, engine)
        return {
            'key': key,
            'vehicle': vehicle
        }

    def _store_product_fitment(self, product_record, fitment):
        existing_fitment_models = ProductFitment.objects.filter(product=product_record).select_related("vehicle").all()
        if not fitment and existing_fitment_models:
            existing_fitment_models.delete()
        elif fitment:
            makes = list()
            models = list()
            vehicle_years = dict()
            fitment_mismatch = False

            for fitment_item in fitment:
                if not fitment_mismatch and existing_fitment_models:
                    if not self._fitment_exists(fitment_item, existing_fitment_models):
                        fitment_mismatch = True
                key = "%s-%s-%s-%s" % (fitment_item['make'], fitment_item['model'], fitment_item['sub_model'], fitment_item['engine'])
                if key not in vehicle_years:
                    vehicle_years[key] = list()
                for year in range(fitment_item['start_year'], fitment_item['end_year'] + 1):
                    if year not in vehicle_years[key]:
                        vehicle_years[key].append(year)
                if not fitment_item['model'] in models:
                    models.append(fitment_item['model'])
                if not fitment_item['make'] in makes:
                    makes.append(fitment_item['make'])
            do_create = False
            if existing_fitment_models and fitment_mismatch:
                existing_fitment_models.delete()
                do_create = True
            elif not existing_fitment_models:
                do_create = True

            if do_create:
                vehicle_models = Vehicle.objects.filter(make__name__in=makes, model__name__in=models).select_related("make").select_related("model").select_related("sub_model").select_related("engine").all()
                vehicles_used = dict()

                fitment_create_objs = list()
                for fitment_item in fitment:
                    note = fitment_item.pop('note')
                    start_year = fitment_item.pop('start_year')
                    end_year = fitment_item.pop('end_year')
                    vehicle_obj = self._get_vehicle(**{**fitment_item, **{'vehicle_models': vehicle_models}})
                    vehicle = vehicle_obj['vehicle']
                    vehicles_used[vehicle.pk] = vehicle_obj
                    fitment_create_objs.append(ProductFitment(product=product_record, vehicle=vehicle, start_year=start_year, end_year=end_year, note=note))
                if fitment_create_objs:
                    year_create_objs = list()
                    vehicle_year_models = VehicleYear.objects.filter(vehicle_id__in=vehicles_used.keys()).all()

                    for vehicle_id, vehicle_data in vehicles_used.items():
                        key = vehicle_data['key']
                        vehicle = vehicle_data['vehicle']
                        years = vehicle_years[key]
                        for year in years:
                            if not self._vehicle_has_year(vehicle_id, year, vehicle_year_models):
                                year_create_objs.append(VehicleYear(year=year, vehicle=vehicle))
                    ProductFitment.objects.bulk_create(fitment_create_objs)
                    if year_create_objs:
                        VehicleYear.objects.bulk_create(year_create_objs)

    def store_or_get_vehicle(self, make, model, sub_model, engine):
        make_record = VehicleMake.objects.get_or_create(name=make)[0]
        model_record = VehicleModel.objects.get_or_create(name=model, make=make_record)[0]
        sub_model_record = VehicleSubModel.objects.get_or_create(name=sub_model, model=model_record)[0]
        engine_record = VehicleEngine.objects.get_or_create(name=engine)[0]
        return Vehicle.objects.get_or_create(make=make_record, model=model_record, sub_model=sub_model_record, engine=engine_record)[0]

    @staticmethod
    def product_exists(internal_part_num):
        return Product.objects.filter(internal_part_num=internal_part_num).exists()

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


class DataRetriever(object):
    def __init__(self, model_cls, query_set):
        self.model_cls = model_cls
        self.query_set = query_set

    def get_instance(self, data):
        for model in self.query_set:
            model_compare = dict()
            for key, value in data.items():
                model_compare[key] = getattr(model, key)
            if data == model_compare:
                return model
        return None

    def bulk_get_or_create(self, data_list, keys):
        items_to_create = list()
        for data_item in data_list:
            model_instance = self.get_instance(data_item)
            if not model_instance:
                items_to_create.append(self.model_cls(**data_item))
        if items_to_create:
            self.model_cls.objects.bulk_create(items_to_create)
            self.query_set = self.query_set.all()
        lookup = dict()
        for model_instance in self.query_set:
            lookup_key = ""
            for key in keys:
                next_attr = model_instance
                # Key could be a relationship, go down the tree
                key_tokens = key.split("__")
                for key_token in key_tokens:
                    next_attr = getattr(next_attr, key_token)
                lookup_key += next_attr
            lookup[lookup_key] = model_instance
        return lookup
