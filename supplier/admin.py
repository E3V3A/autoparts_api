from django.contrib import admin

# Register your models here.
from supplier.models import ProductImage, ProductCategory, Vehicle, ProductFitment

admin.site.register(ProductImage)
admin.site.register(ProductCategory)
admin.site.register(Vehicle)
admin.site.register(ProductFitment)
