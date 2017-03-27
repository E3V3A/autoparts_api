from rest_framework import serializers

from supplier.models import Product, Vendor, Category, ProductImage, VendorProductLine


class VendorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Vendor
        fields = ('id', 'name',)


class VendorProductLineSerializer(serializers.ModelSerializer):
    class Meta:
        model = VendorProductLine
        fields = ('id', 'name',)


class CategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = Category
        fields = ('id', 'name', 'parent_category',)


class ProductCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = Category
        fields = ('id', 'name',)


class ProductImageSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()

    def get_url(self, image):
        """
        Below is for serving local files or files from our own cdn
        request = self.context.get('request')
        return request.build_absolute_uri(image.image_file.url)
        """
        return image.remote_image_file

    class Meta:
        model = ProductImage
        fields = ('url',)


class ProductSerializer(serializers.ModelSerializer):
    vendor = VendorSerializer(read_only=True)
    vendor_product_line = VendorProductLineSerializer(read_only=True)
    sub_category = serializers.SerializerMethodField()
    category = serializers.SerializerMethodField()
    can_drop_ship = serializers.SerializerMethodField()
    images = ProductImageSerializer(read_only=True, many=True)

    def get_sub_category(self, product):
        return ProductCategorySerializer(instance=product.productcategory_set.filter(category__parent_category__isnull=False).first().category).data

    def get_category(self, product):
        return ProductCategorySerializer(instance=product.productcategory_set.filter(category__parent_category__isnull=True).first().category).data

    def get_can_drop_ship(self, product):
        return product.get_can_drop_ship_display()

    class Meta:
        model = Product
        fields = (
            'id', 'internal_part_num', 'vendor_part_num', 'description', 'overview', 'cost', 'retail_price', 'jobber_price', 'min_price', 'core_charge', 'can_drop_ship', 'drop_ship_fee', 'vendor', 'vendor_product_line', 'category', 'sub_category',
            'images', 'remote_image_thumb',)
