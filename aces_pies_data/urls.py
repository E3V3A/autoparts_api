from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from aces_pies_data.views import ProductFitmentListView
from . import views

router = DefaultRouter()
router.register(r'brands', views.BrandViewSet, base_name="brands")
router.register(r'products', views.ProductViewSet, base_name="products")
router.register(r'categories', views.CategoryViewSet, base_name='categories')
router.register(r'attributes', views.AttributeViewSet, base_name='attributes')

urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^product-fitment/(?P<pk>[0-9]+)/$', ProductFitmentListView.as_view(), name='product-fitment')
]
