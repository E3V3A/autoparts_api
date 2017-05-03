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
    def optimized_save(self, product_data):
        related_records = self._bulk_create_product_relationships(product_data)
        vendor_records, product_line_records = related_records['vendor_records'], related_records['product_line_records']
        category_records, sub_category_records = related_records['category_records'], related_records['sub_category_records']

        product_records = dict()
        existing_product_records = Product.objects.filter(internal_part_num__in=product_data.keys()).all()
        for existing_product in existing_product_records:
            product_records[existing_product.internal_part_num] = existing_product
        products_to_create = dict()
        products_to_update = dict()
        for internal_part_num, product_data_item in product_data.items():
            if product_data_item['is_valid_item']:
                # filter duplicates out
                if internal_part_num not in products_to_create and internal_part_num not in products_to_update:
                    product_args = dict()
                    for key, value in product_data_item.items():
                        if key in self.product_data_mapping:
                            data_mapper = self.product_data_mapping[key]
                            product_args[data_mapper['model']] = data_mapper['serializer'](value) if data_mapper['serializer'] is not None else value
                    # new product
                    if internal_part_num not in product_records:
                        vendor, product_line = product_data_item["PrimaryVendor"], product_data_item["product_line"]
                        product_args = {
                            'internal_part_num': internal_part_num,
                            'vendor': vendor_records[vendor]
                        }
                        if product_line:
                            product_args['vendor_product_line'] = product_line_records[vendor + product_line]
                        products_to_create[internal_part_num] = Product(**product_args)
                    # update the product
                    else:
                        products_to_update[internal_part_num] = Product(**product_args)
        if products_to_create:
            Product.objects.bulk_create(products_to_create.values())
            created_products = Product.objects.filter(internal_part_num__in=products_to_create.keys()).all()
            product_categories_to_create = list()
            for created_product in created_products:
                product_data_item = product_data[created_product.internal_part_num]
                category, sub_category = product_data_item["category"], product_data_item["sub_category"]
                if category:
                    category_record = category_records[category]
                    product_categories_to_create.append(ProductCategory(product=created_product, category=category_record))
                    if sub_category:
                        sub_category_record = sub_category_records[category + sub_category]
                        product_categories_to_create.append(ProductCategory(product=created_product, category=sub_category_record))
            self._optimized_store_remote_images(created_products, product_data)
            self._optimized_bulk_store_product_fitment(created_products, product_data)

            if product_categories_to_create:
                ProductCategory.objects.bulk_create(product_categories_to_create)
        if products_to_update:
            product_update_fields = ("name", "description", "long_description", "cost", "retail_price", "jobber_price", "min_price", "core_charge", "can_drop_ship", "drop_ship_fee",)
            bulk_update(products_to_update.values(), update_fields=product_update_fields)

    def _bulk_create_product_relationships(self, product_data):
        vendor_objects, vendor_names = list(), list()
        product_line_objects, product_line_names = dict(), list()
        category_objects, category_names = list(), list()
        sub_category_objects, sub_category_names = dict(), list()
        for internal_part_num, product_data_item in product_data.items():
            if product_data_item['is_valid_item']:
                vendor, product_line = product_data_item["PrimaryVendor"], product_data_item["product_line"]
                category, sub_category = product_data_item['category'], product_data_item['sub_category']
                if vendor not in vendor_names:
                    vendor_objects.append({"name": vendor})
                    vendor_names.append(vendor)
                if product_line:
                    product_line_key = vendor + product_line
                    if product_line not in product_line_names:
                        product_line_names.append(product_line)
                    if product_line_key not in product_line_objects:
                        product_line_objects[product_line_key] = {"name": product_line, "vendor": vendor}
                if category:
                    if category not in category_names:
                        category_names.append(category)
                        category_objects.append({"name": category, "parent_category": None})
                    if sub_category:
                        sub_category_key = category + sub_category
                        if sub_category not in sub_category_names:
                            sub_category_names.append(sub_category)
                        if sub_category_key not in sub_category_objects:
                            sub_category_objects[sub_category_key] = {"name": sub_category, "parent_category": category}

        vendor_retriever = DataRetriever(Vendor, Vendor.objects.filter(name__in=vendor_names))
        vendor_records = vendor_retriever.bulk_get_or_create(vendor_objects, ("name",))
        for product_line_object in product_line_objects.values():
            product_line_object["vendor"] = vendor_records[product_line_object["vendor"]]
        product_line_retriever = DataRetriever(VendorProductLine, VendorProductLine.objects.filter(name__in=product_line_names).select_related("vendor"))
        product_line_records = product_line_retriever.bulk_get_or_create(product_line_objects.values(), ("vendor__name", "name",))

        category_retriever = DataRetriever(Category, Category.objects.filter(name__in=category_names, parent_category=None))
        category_records = category_retriever.bulk_get_or_create(category_objects, ("name",))
        for sub_category_object in sub_category_objects.values():
            sub_category_object["parent_category"] = category_records[sub_category_object["parent_category"]]
        sub_category_retriever = DataRetriever(Category, Category.objects.filter(name__in=sub_category_names, parent_category__name__isnull=False).select_related("parent_category"))
        sub_category_records = sub_category_retriever.bulk_get_or_create(sub_category_objects.values(), ("parent_category__name", "name",))

        return {
            "vendor_records": vendor_records,
            "product_line_records": product_line_records,
            "category_records": category_records,
            "sub_category_records": sub_category_records
        }

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

    def _optimized_store_remote_images(self, product_records, product_data):
        product_images_to_create = list()
        for product_record in product_records:
            images = product_data[product_record.internal_part_num]['images']
            for image_stack in images:
                img_url = image_stack['large_img'] if image_stack['large_img'] else image_stack['med_img']
                if img_url:
                    product_images_to_create.append(ProductImage(product=product_record, is_primary=image_stack['is_primary'], remote_image_file=img_url))
        if product_images_to_create:
            ProductImage.objects.bulk_create(product_images_to_create)

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

    def _optimized_get_vehicle(self, make, model, sub_model, engine, vehicle_lookup):
        vehicle = None
        vehicle_key = "%s%s%s%s" % (make, model, sub_model, engine)
        if vehicle_key in vehicle_lookup:
            vehicle = vehicle_lookup[vehicle_key]
        if not vehicle:
            vehicle = self.store_or_get_vehicle(make, model, sub_model, engine)
        return vehicle

    def _optimized_bulk_store_product_fitment(self, product_records, product_data):
        related_records = self._bulk_create_fitment_relationship(product_data)
        vehicle_records = related_records["vehicle_records"]
        fitment_create_objs = list()
        vehicle_years = dict()
        for product_record in product_records:
            product_data_item = product_data[product_record.internal_part_num]
            fitment = product_data_item['fitment']
            for fitment_item in fitment:
                make, model, sub_model, engine = fitment_item['make'], fitment_item['model'], fitment_item['sub_model'], fitment_item['engine']
                vehicle_key = "%s%s%s%s" % (make, model, sub_model, engine)
                note = fitment_item.pop('note')
                start_year = fitment_item.pop('start_year')
                end_year = fitment_item.pop('end_year')
                vehicle = vehicle_records[vehicle_key]
                fitment_create_objs.append(ProductFitment(product=product_record, vehicle=vehicle, start_year=start_year, end_year=end_year, note=note))
                if vehicle.pk not in vehicle_years:
                    vehicle_years[vehicle.pk] = {
                        'years': list(),
                        'vehicle': vehicle
                    }
                for year in range(start_year, end_year + 1):
                    if year not in vehicle_years[vehicle.pk]['years']:
                        vehicle_years[vehicle.pk]['years'].append(year)

        if fitment_create_objs:
            ProductFitment.objects.bulk_create(fitment_create_objs)
            year_create_objs = list()
            vehicle_year_records = VehicleYear.objects.filter(vehicle_id__in=vehicle_years.keys()).all()
            vehicle_year_existing = list()
            for vehicle_year_record in vehicle_year_records:
                vehicle_year_key = str(vehicle_year_record.vehicle_id) + str(vehicle_year_record.year)
                vehicle_year_existing.append(vehicle_year_key)
            for vehicle_id, vehicle_data in vehicle_years.items():
                vehicle, years = vehicle_data['vehicle'], vehicle_data['years']
                for year in years:
                    vehicle_year_key = str(vehicle_id) + str(year)
                    if vehicle_year_key not in vehicle_year_existing:
                        year_create_objs.append(VehicleYear(year=year, vehicle=vehicle))
            if year_create_objs:
                VehicleYear.objects.bulk_create(year_create_objs)

    def _bulk_create_fitment_relationship(self, product_data):
        make_objects, make_names = list(), list()
        model_objects, model_names = dict(), list()
        sub_model_objects, sub_model_names = dict(), list()
        engine_objects, engine_names = list(), list()
        vehicle_objects = dict()
        for internal_part_num, product_data_item in product_data.items():
            fitment = product_data_item['fitment']
            for fitment_item in fitment:
                make, model, sub_model, engine = fitment_item['make'], fitment_item['model'], fitment_item['sub_model'], fitment_item['engine']
                if make not in make_names:
                    make_objects.append({"name": make})
                    make_names.append(make)

                if engine not in engine_names:
                    engine_objects.append({"name": engine})
                    engine_names.append(engine)

                if model not in model_names:
                    model_names.append(model)
                model_key = make + model
                if model_key not in model_objects:
                    model_objects[model_key] = {"name": model, "make": make}
                if sub_model not in sub_model_names:
                    sub_model_names.append(sub_model)
                sub_model_key = make + model + sub_model
                if sub_model_key not in sub_model_objects:
                    sub_model_objects[sub_model_key] = {"name": sub_model, "model": model, "make": make}
                vehicle_key = "%s%s%s%s" % (make, model, sub_model, engine)
                if vehicle_key not in vehicle_objects:
                    vehicle_objects[vehicle_key] = {
                        "make": make,
                        "model": model,
                        "sub_model": sub_model,
                        "engine": engine
                    }
        make_retriever = DataRetriever(VehicleMake, VehicleMake.objects.filter(name__in=make_names))
        make_records = make_retriever.bulk_get_or_create(make_objects, ("name",))

        engine_retriever = DataRetriever(VehicleEngine, VehicleEngine.objects.filter(name__in=engine_names))
        engine_records = engine_retriever.bulk_get_or_create(engine_objects, ("name",))

        for model_key, model_object in model_objects.items():
            model_object["make"] = make_records[model_object["make"]]

        model_retriever = DataRetriever(VehicleModel, VehicleModel.objects.filter(name__in=model_names).select_related("make"))
        model_records = model_retriever.bulk_get_or_create(model_objects.values(), ("make__name", "name",))

        for sub_model_key, sub_model_object in sub_model_objects.items():
            model_lookup_key = "%s%s" % (sub_model_object["make"], sub_model_object["model"])
            sub_model_object["model"] = model_records[model_lookup_key]
            sub_model_object.pop("make")

        sub_model_retriever = DataRetriever(VehicleSubModel, VehicleSubModel.objects.filter(name__in=sub_model_names).select_related("model__make"))
        sub_model_records = sub_model_retriever.bulk_get_or_create(sub_model_objects.values(), ("model__make__name", "model__name", "name",))

        for vehicle_key, vehicle_object in vehicle_objects.items():
            make, model, sub_model, engine = vehicle_object["make"], vehicle_object["model"], vehicle_object["sub_model"], vehicle_object["engine"]
            vehicle_object["make"] = make_records[make]
            vehicle_object["model"] = model_records[make + model]
            vehicle_object["sub_model"] = sub_model_records[make + model + sub_model]
            vehicle_object["engine"] = engine_records[engine]

        vehicle_retriever = DataRetriever(Vehicle, Vehicle.objects.filter(make__name__in=make_names, model__name__in=model_names).select_related("make").select_related("model").select_related("sub_model").select_related("engine"))
        vehicle_records = vehicle_retriever.bulk_get_or_create(vehicle_objects.values(), ("make__name", "model__name", "sub_model__name", "engine__name",))

        return {
            "make_records": make_records,
            "model_records": model_records,
            "sub_model_records": sub_model_records,
            "engine_records": engine_records,
            "vehicle_records": vehicle_records
        }

    def _optimized_store_product_fitment(self, product_record, fitment, existing_fitment_records):
        if not fitment and existing_fitment_records:
            existing_fitment_records.delete()
        elif fitment:
            makes = list()
            models = list()
            vehicle_years = dict()
            fitment_mismatch = False

            for fitment_item in fitment:
                if not fitment_mismatch and existing_fitment_records:
                    if not self._fitment_exists(fitment_item, existing_fitment_records):
                        fitment_mismatch = True
                key = "%s%s%s%s" % (fitment_item['make'], fitment_item['model'], fitment_item['sub_model'], fitment_item['engine'])
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
            if existing_fitment_records and fitment_mismatch:
                existing_fitment_records.delete()
                do_create = True
            elif not existing_fitment_records:
                do_create = True
            vehicle_records = Vehicle.objects.filter(make__name__in=makes, model__name__in=models).select_related("make").select_related("model").select_related("sub_model").select_related("engine").all()
            vehicle_lookup = dict()
            for vehicle_record in vehicle_records:
                make, model, sub_model, engine = vehicle_record.make.name, vehicle_record.model.name, vehicle_record.sub_model.name, vehicle_record.engine.name
                vehicle_lookup[make + model + sub_model + engine] = vehicle_record
            if do_create:
                make_objects, make_names = list(), list()
                model_objects, model_names = dict(), list()
                sub_model_objects, sub_model_names = dict(), list()
                engine_objects, engine_names = list(), list()
                vehicle_objects = dict()
                num_existing_vehicles = 0
                for fitment_item in fitment:
                    make, model, sub_model, engine = fitment_item['make'], fitment_item['model'], fitment_item['sub_model'], fitment_item['engine']
                    vehicle_key = "%s%s%s%s" % (make, model, sub_model, engine)
                    if vehicle_key in vehicle_lookup:
                        num_existing_vehicles += 1
                    if make not in make_names:
                        make_objects.append({"name": make})
                        make_names.append(make)

                    if engine not in engine_names:
                        engine_objects.append({"name": engine})
                        engine_names.append(engine)

                    if model not in model_names:
                        model_names.append(model)
                    model_key = make + model
                    if model_key not in model_objects:
                        model_objects[model_key] = {"name": model, "make": make}
                    if sub_model not in sub_model_names:
                        sub_model_names.append(sub_model)
                    sub_model_key = make + model + sub_model
                    if sub_model_key not in sub_model_objects:
                        sub_model_objects[sub_model_key] = {"name": sub_model, "model": model, "make": make}
                    vehicle_key = "%s%s%s%s" % (make, model, sub_model, engine)
                    if vehicle_key not in vehicle_objects:
                        vehicle_objects[vehicle_key] = {
                            "make": make,
                            "model": model,
                            "sub_model": sub_model,
                            "engine": engine
                        }
                num_vehicles_to_create = len(fitment) - num_existing_vehicles
                print("To create, ", num_vehicles_to_create)
                # use_bulk_create = num_vehicles_to_create > 3
                use_bulk_create = False
                if use_bulk_create:
                    make_retriever = DataRetriever(VehicleMake, VehicleMake.objects.filter(name__in=make_names))
                    make_records = make_retriever.bulk_get_or_create(make_objects, ("name",))

                    engine_retriever = DataRetriever(VehicleEngine, VehicleEngine.objects.filter(name__in=engine_names))
                    engine_records = engine_retriever.bulk_get_or_create(engine_objects, ("name",))

                    for model_key, model_object in model_objects.items():
                        model_object["make"] = make_records[model_object["make"]]

                    model_retriever = DataRetriever(VehicleModel, VehicleModel.objects.filter(name__in=model_names).select_related("make"))
                    model_records = model_retriever.bulk_get_or_create(model_objects.values(), ("make__name", "name",))

                    for sub_model_key, sub_model_object in sub_model_objects.items():
                        model_lookup_key = "%s%s" % (sub_model_object["make"], sub_model_object["model"])
                        sub_model_object["model"] = model_records[model_lookup_key]
                        sub_model_object.pop("make")

                    sub_model_retriever = DataRetriever(VehicleSubModel, VehicleSubModel.objects.filter(name__in=sub_model_names).select_related("model__make"))
                    sub_model_records = sub_model_retriever.bulk_get_or_create(sub_model_objects.values(), ("model__make__name", "model__name", "name",))

                    for vehicle_key, vehicle_object in vehicle_objects.items():
                        make, model, sub_model, engine = vehicle_object["make"], vehicle_object["model"], vehicle_object["sub_model"], vehicle_object["engine"]
                        vehicle_object["make"] = make_records[make]
                        vehicle_object["model"] = model_records[make + model]
                        vehicle_object["sub_model"] = sub_model_records[make + model + sub_model]
                        vehicle_object["engine"] = engine_records[engine]

                    vehicle_retriever = DataRetriever(Vehicle, Vehicle.objects.filter(make__name__in=makes, model__name__in=models).select_related("make").select_related("model").select_related("sub_model").select_related("engine"))
                    vehicle_records = vehicle_retriever.bulk_get_or_create(vehicle_objects.values(), ("make__name", "model__name", "sub_model__name", "engine__name",))
                vehicles_used = dict()

                fitment_create_objs = list()
                for fitment_item in fitment:
                    make, model, sub_model, engine = fitment_item['make'], fitment_item['model'], fitment_item['sub_model'], fitment_item['engine']
                    vehicle_key = "%s%s%s%s" % (make, model, sub_model, engine)
                    note = fitment_item.pop('note')
                    start_year = fitment_item.pop('start_year')
                    end_year = fitment_item.pop('end_year')
                    if use_bulk_create:
                        vehicle_record = vehicle_records[vehicle_key]
                    else:
                        vehicle_record = self._optimized_get_vehicle(make, model, sub_model, engine, vehicle_records)
                    vehicles_used[vehicle_record.pk] = {
                        "key": vehicle_key,
                        "vehicle": vehicle_record
                    }
                    fitment_create_objs.append(ProductFitment(product=product_record, vehicle=vehicle_record, start_year=start_year, end_year=end_year, note=note))
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
