from django.contrib import admin

# Register your models here.
from supplier.models import ProductImage, ProductCategory

admin.site.register(ProductImage)
admin.site.register(ProductCategory)