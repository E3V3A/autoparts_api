from django.contrib import admin

# Register your models here.
from supplier.models import ProductImage, ProductImageMap

admin.site.register(ProductImage)
admin.site.register(ProductImageMap)