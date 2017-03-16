from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from . import views
app_name = 'supplier'

router = DefaultRouter()
router.register(r'vendors', views.VendorViewSet)
router.register(r'products', views.ProductViewSet)
router.register(r'categories', views.CategoryViewSet)

urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^import/$', views.import_products),
]