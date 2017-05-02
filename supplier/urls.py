from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from . import views

app_name = 'supplier'

router = DefaultRouter()
router.register(r'vendors', views.VendorViewSet)
router.register(r'products', views.ProductViewSet, base_name="products")
router.register(r'categories', views.CategoryViewSet, base_name='categories')

urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^import-products/$', views.import_products),
    url(r'^import-stock/$', views.import_stock),
]
