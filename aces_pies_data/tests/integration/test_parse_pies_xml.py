import os

import pytest

from aces_pies_data.models import Product, Brand
from aces_pies_data.util.aces_pies_parsing import PiesFileParser
from aces_pies_data.util.aces_pies_storage import PiesDataStorage


@pytest.mark.django_db
def test_pies_data_storage_matches_parsed_data_with_production_file(brand_data, brand_record):
    """
    This test ensures a real life pies file is parsed and stored correctly.
    The DataStorage class uses bulk inserts to speed things up.
    Bulk inserts and relationships are much more difficult to manage than sequential inserts, thus the reason this test is here.
    """

    products_to_compare = dict()
    num_products_to_bulk_compare = 20

    for product_data in brand_data['product_data']:
        products_to_compare[product_data['part_number']] = product_data
        if len(products_to_compare) == num_products_to_bulk_compare:
            compare_products_to_db(products_to_compare, brand_record)
            products_to_compare = dict()
    if len(products_to_compare) == num_products_to_bulk_compare:
        compare_products_to_db(products_to_compare, brand_record)


@pytest.mark.django_db
def test_pies_product_update(updated_test_brand_data, test_brand_record):
    """
     This test ensures a product is correctly updated when imported data differs from stored data
    """
    PiesDataStorage(updated_test_brand_data).store_brand_data()
    compare_products_to_db(updated_test_brand_data, test_brand_record)


@pytest.fixture
def initial_test_brand_data():
    brand_name = 'Test Brand'
    initial_test_data = {
        'brand': brand_name,
        'logo': {
            'file_size_bytes': '102',
            'name': 'test_logo.jpg',
            'url': 'http://fake.logo.com'
        },
        'marketing_copy': 'Test brand marketing copy',
        'product_data': [
            {
                'part_number': 'test_1',
                'name': 'Test item',
                'brand_name': brand_name,
                'is_carb_legal': True,
                'is_discontinued': False,
                'is_hazardous': False,
                'is_obsolete': False,
                'map_price': 12.25,
                'retail_price': 17.24,
                'attributes': [
                    {
                        'type': 'Test Attribute 1',
                        'value': 'Test Attribute Value 1'
                    },
                    {
                        'type': 'Test Attribute 2',
                        'value': 'Test Attribute Value 2'
                    }
                ],
                'digital_assets': [
                    {
                        'asset_type': 'Product Image',
                        'display_sequence': 1,
                        'file_size_bytes': 387,
                        'url': 'http://fake.image.com/test_product_image_main.jpg'
                    },
                ],
                'features': [
                    'test feature 1',
                    'test feature 2',
                    'test feature 3',
                ],
                'packages': [
                    {
                        'dimension_unit': 'in',
                        'dimensionalweight': 1.50,
                        'height': 3.00,
                        'length': 6.00,
                        'quantity': 1,
                        'weight': 1.50,
                        'weight_unit': 'lb',
                        'width': 5.00
                    }
                ]
            }
        ]
    }
    PiesDataStorage(initial_test_data).store_brand_data()
    return initial_test_data


@pytest.fixture
def updated_test_brand_data(initial_test_brand_data):
    updated_data = initial_test_brand_data.copy()
    product_to_update = updated_data['product_data'][0]
    product_to_update['retail_price'] = product_to_update['retail_price'] + 1.76
    product_to_update['digital_assets'][0]['file_size_bytes'] = product_to_update['digital_assets'][0]['file_size_bytes'] + 5
    product_to_update['attributes'][0]['value'] = product_to_update['attributes'][0]['value'] + " updated"
    product_to_update['features'].append("New Feature!!!")
    product_to_update['packages'][0]['weight'] = product_to_update['packages'][0]['weight'] + 1.25
    return updated_data


@pytest.fixture
def production_brand_data():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = dir_path + "/test_files/production_PIES67.xml"
    with open(file_path) as production_pies_file:
        pies_file_parser = PiesFileParser(production_pies_file)
        PiesDataStorage(pies_file_parser.get_brand_data()).store_brand_data()
    production_pies_file = open(file_path)
    pies_file_parser = PiesFileParser(production_pies_file)
    yield pies_file_parser.get_brand_data()
    production_pies_file.close()


@pytest.fixture
def production_brand_record(brand_data):
    brand_name = brand_data['brand']
    return Brand.objects.get(name=brand_name)


@pytest.fixture
def test_brand_record(initial_test_brand_data):
    brand_name = initial_test_brand_data['brand']
    return Brand.objects.get(name=brand_name)


def compare_products_to_db(products, brand_record):
    existing_product_records = Product.objects.filter(part_number__in=products.keys(), brand=brand_record).prefetch_related('features').prefetch_related('attributes').prefetch_related('attributes__attribute').prefetch_related('attributes__value').prefetch_related(
        'packages').prefetch_related('digital_assets').prefetch_related('digital_assets__digital_asset').prefetch_related('digital_assets__digital_asset__type').all()
    for existing_product_record in existing_product_records:
        compare_product_data(products[existing_product_record.part_number], existing_product_record)
        compare_attributes(products[existing_product_record.part_number]['attributes'], existing_product_record)
        compare_features(products[existing_product_record.part_number]['features'], existing_product_record)
        compare_packages(products[existing_product_record.part_number]['packages'], existing_product_record)
        compare_digital_assets(products[existing_product_record.part_number]['digital_assets'], existing_product_record)


def compare_product_data(product_data, product_record):
    assert product_data['name'] == product_record.name
    assert product_data['is_carb_legal'] == product_record.is_carb_legal
    assert product_data['is_discontinued'] == product_record.is_discontinued
    assert product_data['is_obsolete'] == product_record.is_obsolete
    assert product_data['map_price'] == product_record.map_price
    assert product_data['retail_price'] == product_record.retail_price


def compare_attributes(attributes, product_record):
    assert len(attributes) == product_record.attributes.count()
    sorted_attributes = sorted(attributes, key=lambda row: (row['type'], row['value']))
    for idx, existing_attribute in enumerate(product_record.attributes.all()):
        attribute_to_compare = sorted_attributes[idx]
        assert attribute_to_compare['type'] == existing_attribute.attribute.name
        assert attribute_to_compare['value'] == existing_attribute.value.value


def compare_features(features, product_record):
    assert len(features) == product_record.features.count()
    for idx, existing_feature in enumerate(product_record.features.all()):
        feature_to_compare = features[idx]
        assert feature_to_compare == existing_feature.name
        assert idx == existing_feature.listing_sequence


def compare_packages(packages, product_record):
    assert len(packages) == product_record.packages.count()
    for idx, existing_package in enumerate(product_record.packages.all()):
        package_to_compare = packages[idx]
        assert package_to_compare.get('weight', None) == existing_package.weight
        assert package_to_compare.get('dimensionalweight', None) == existing_package.dimensional_weight
        assert package_to_compare.get('height', None) == existing_package.height
        assert package_to_compare.get('length', None) == existing_package.length
        assert package_to_compare.get('width', None) == existing_package.width


def compare_digital_assets(digital_assets, product_record):
    assert len(digital_assets) == product_record.digital_assets.count()
    for existing_digital_asset in product_record.digital_assets.all():
        digital_asset_to_compare = next(filter(lambda digital_asset: digital_asset['url'] == existing_digital_asset.digital_asset.url and digital_asset['asset_type'] == existing_digital_asset.digital_asset.type.name, digital_assets))
        assert digital_asset_to_compare['display_sequence'] == existing_digital_asset.display_sequence
        assert digital_asset_to_compare['file_size_bytes'] == existing_digital_asset.digital_asset.file_size_bytes
