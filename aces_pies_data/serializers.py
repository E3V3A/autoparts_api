from django.db.models import Count
from rest_framework import serializers
from rest_framework.relations import HyperlinkedIdentityField

from .models import Category, Brand, Product, DigitalAsset, ProductAttribute, ProductFeature, ProductPackaging, ProductDigitalAsset, Attribute, AttributeValue, ProductFitment, Vehicle


class DigitalAssetSerializer(serializers.ModelSerializer):
    class Meta:
        model = DigitalAsset
        fields = ('id', 'url', 'file_size_bytes',)


class BrandSerializer(serializers.ModelSerializer):
    logo = DigitalAssetSerializer(read_only=True)

    class Meta:
        model = Brand
        fields = ('id', 'name', 'logo', 'marketing_copy',)


class ProductBrandSerializer(serializers.HyperlinkedModelSerializer):
    url = serializers.HyperlinkedIdentityField(view_name="brands-detail")

    class Meta:
        model = Brand
        fields = ('id', 'name', 'url',)


class AttributeSerializer(serializers.ModelSerializer):
    values = serializers.SerializerMethodField()

    def get_values(self, attribute_model):
        values_list = list()
        for value in attribute_model.values.all():
            values_list.append(value.value)
        return values_list

    class Meta:
        model = Attribute
        fields = ('name', 'values',)


class CategorySerializer(serializers.HyperlinkedModelSerializer):
    attributes = serializers.HyperlinkedRelatedField(view_name='attributes-detail', many=True, read_only=True)

    class Meta:
        model = Category
        fields = ('id', 'name', 'parent', 'attributes',)


class ProductCategorySerializer(serializers.HyperlinkedModelSerializer):
    url = serializers.HyperlinkedIdentityField(view_name="categories-detail")

    class Meta:
        model = Category
        fields = ('id', 'name', 'url',)


class VehicleSerializer(serializers.ModelSerializer):
    make = serializers.SerializerMethodField()

    def get_make(self, vehicle):
        return vehicle.make.name

    class Meta:
        model = Vehicle
        fields = ('make',)


class ProductFitmentSerializer(serializers.ModelSerializer):
    make = serializers.CharField(source='vehicle.make.name')
    model = serializers.CharField(source='vehicle.model.name')
    sub_model = serializers.CharField(source='vehicle.sub_model.name')
    engine = serializers.SerializerMethodField()
    years = serializers.SerializerMethodField()
    fitment_info_1 = serializers.CharField()
    fitment_info_2 = serializers.CharField()

    def get_engine(self, fitment_record):
        engine = fitment_record.vehicle.engine
        if engine:
            return {
                "configuration": engine.configuration,
                "liters": engine.liters,
                "engine_code": engine.engine_code,
                "aspiration": engine.aspiration.name,
                "fuel_type": engine.fuel_type.name,
                "fuel_delivery": engine.fuel_delivery.name
            }
        return None

    def get_years(self, fitment_record):
        start_year = fitment_record.start_year
        end_year = fitment_record.end_year

        if start_year == end_year:
            years = str(start_year)
        else:
            years = str(start_year) + " - " + str(end_year)
        return years

    class Meta:
        model = ProductFitment
        fields = ('years', 'make', 'model', 'sub_model', 'engine', 'fitment_info_1', 'fitment_info_2',)


class ProductDigitalAssetSerializer(serializers.ModelSerializer):
    url = serializers.CharField(source='digital_asset.url')
    type = serializers.CharField(source='digital_asset.type.name')

    class Meta:
        model = ProductDigitalAsset
        fields = ('display_sequence', 'url', 'type',)


class ProductPackagingSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProductPackaging
        fields = ('product_quantity', 'weight', 'height', 'length', 'width')


class ProductAttributeSerializer(serializers.ModelSerializer):
    name = serializers.CharField(source='attribute.name')
    value = serializers.CharField(source='value.value')

    class Meta:
        model = ProductAttribute
        fields = ('name', 'value',)


class ProductSerializer(serializers.HyperlinkedModelSerializer):
    brand = ProductBrandSerializer(read_only=True)
    category = ProductCategorySerializer(read_only=True)
    attributes = ProductAttributeSerializer(many=True, read_only=True)
    features = serializers.SerializerMethodField()
    packages = ProductPackagingSerializer(many=True, read_only=True)
    digital_assets = ProductDigitalAssetSerializer(many=True, read_only=True)
    fitment_listing = serializers.HyperlinkedIdentityField(view_name='product-fitment')
    fitment_count = serializers.SerializerMethodField()
    images = serializers.SerializerMethodField()

    def get_fitment_count(self, product_model):
        context = self.context
        """
        Custom count query to get fitment below.  This was orginally put as an annotation on the original viewset query set,
        however, it was discovered that the additional group bys for brand and category significantly slowed down the query
        Grouping only by product vastly increased the speed
        """
        if 'fitment_count' not in context:
            if not getattr(self.instance, '__iter__', False):
                product_records = [self.instance]
            else:
                product_records = self.instance
            product_ids = [product.pk for product in product_records]
            product_with_fitment = Product.objects.filter(pk__in=product_ids).annotate(fitment_count=Count('fitment')).all()
            context['fitment_count'] = dict()
            for product in product_with_fitment:
                context['fitment_count'][product.pk] = product.fitment_count

        return context['fitment_count'][product_model.pk]

    def get_features(self, product_model):
        feature_list = list()
        for feature in product_model.features.all():
            feature_list.append(feature.name)
        return feature_list

    def get_images(self, product_model):
        digital_assets = list()
        for product_digital_asset in product_model.digital_assets.all():
            digital_asset = product_digital_asset.digital_asset
            if digital_asset.type.name == 'Product Image':
                digital_assets.append({
                    "url": digital_asset.url,
                    "display_sequence": product_digital_asset.display_sequence
                })

        images = [digital_asset['url'] for digital_asset in sorted(digital_assets, key=lambda k: k['display_sequence'])]
        return images

    class Meta:
        model = Product
        fields = ('id', 'part_number', 'name', 'brand', 'category', 'is_hazardous', 'is_carb_legal', 'is_discontinued', 'is_obsolete', 'map_price', 'retail_price', 'attributes', 'features', 'packages', 'digital_assets', 'fitment_count', 'fitment_listing', 'images',)
