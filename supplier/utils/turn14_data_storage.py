import re
from decimal import Decimal

import time
from django.db import transaction

from supplier.models import Vendor, VendorProductLine, Category, Product, ProductCategory, ProductImage, ProductFitment, VehicleYear, VehicleMake, VehicleModel, VehicleEngine, VehicleSubModel, Vehicle


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
            'overview': {
                'model': 'overview',
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
    def save(self, data_item):
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

    def _store_remote_images(self, product_record, images):
        ProductImage.objects.filter(product=product_record).delete()
        create_objs = list()
        for image_stack in images:
            img_url = image_stack['large_img'] if image_stack['large_img'] else image_stack['med_img']
            if img_url:
                create_objs.append(ProductImage(product=product_record, remote_image_file=img_url))
        if create_objs:
            ProductImage.objects.bulk_create(create_objs)

    def _store_product_fitment(self, product_record, fitment):
        ProductFitment.objects.filter(product=product_record).delete()
        create_objs = list()
        for fitment_item in fitment:
            special_fitment = fitment_item.pop('special_fitment')
            vehicle = self.store_or_get_vehicle(**fitment_item)
            create_objs.append(ProductFitment(product=product_record, vehicle=vehicle, special_fitment=special_fitment))
        if create_objs:
            ProductFitment.objects.bulk_create(create_objs)

    def store_or_get_vehicle(self, year, make, model, sub_model, engine, **kwargs):
        year_record = VehicleYear.objects.get_or_create(year=year)[0]
        make_record = VehicleMake.objects.get_or_create(name=make)[0]
        model_record = VehicleModel.objects.get_or_create(name=model, make=make_record)[0]
        sub_model_record = VehicleSubModel.objects.get_or_create(name=sub_model, model=model_record)[0]
        engine_record = VehicleEngine.objects.get_or_create(name=engine)[0]
        return Vehicle.objects.get_or_create(year=year_record, make=make_record, model=model_record, sub_model=sub_model_record, engine=engine_record)[0]

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
