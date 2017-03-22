import logging
from django.http import HttpResponse
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import viewsets
from rest_framework.filters import OrderingFilter

from supplier.filters import DefaultPagination, ProductListFilter, VendorListFilters, CategoryListFilters
from supplier.models import Product, Category, Vendor
from supplier.serializers import ProductSerializer, CategorySerializer, VendorSerializer
from supplier.utils.turn14_data_importer import Turn14DataImporter

logger = logging.getLogger(__name__)


class ProductViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Product.objects.all()
    serializer_class = ProductSerializer
    pagination_class = DefaultPagination
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = ProductListFilter
    ordering_fields = ('vendor_part_num', 'description', 'retail_price', 'jobber_price', 'min_price', 'core_charge', 'can_drop_ship', 'drop_ship_fee', 'vendor', 'category', 'sub_category',)

    # TODO: figure out how to order on the custom category/sub_category fields
    # possible solution http://stackoverflow.com/questions/24987446/django-rest-framework-queryset-doesnt-order


class CategoryViewSet(viewsets.ReadOnlyModelViewSet):
    # permission_classes = (permissions.IsAuthenticated,)
    queryset = Category.objects.all()
    serializer_class = CategorySerializer
    pagination_class = DefaultPagination
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = CategoryListFilters
    ordering_fields = ('name',)


class VendorViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Vendor.objects.all()
    serializer_class = VendorSerializer
    pagination_class = DefaultPagination

    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = VendorListFilters
    ordering_fields = ('name',)


def import_products(request):
    Turn14DataImporter().import_and_store_product_data(refresh_all=True)
    return HttpResponse("Doing work!")
