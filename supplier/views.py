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


class FieldLimiterMixin(object):
    @staticmethod
    def filter_fields(fields, request):
        include_fields = request.query_params.get("fields")
        exclude_fields = request.query_params.get("-fields")
        if include_fields:
            included_fields = include_fields.split(",")
            filtered_fields = [field for field in included_fields if field in fields]

        elif exclude_fields:
            excluded_fields = exclude_fields.split(",")
            filtered_fields = [field for field in fields if field not in excluded_fields]
        else:
            filtered_fields = fields

        return filtered_fields

    def get_serializer(self, *args, **kwargs):
        serializer_class = self.get_serializer_class()

        def serializer_init(self, *args, **kwargs):
            super(serializer_class, self).__init__(*args, **kwargs)
            included_fields = FieldLimiterMixin.filter_fields(self.fields, self.context['request'])
            field_keys = list(self.fields.keys())
            for field_key in field_keys:
                if field_key not in included_fields:
                    self.fields.pop(field_key)

        kwargs['context'] = self.get_serializer_context()
        serializer_class.__init__ = serializer_init
        serializer_instance = serializer_class(*args, **kwargs)
        return serializer_instance

    def get_queryset(self):
        def apply_related_fns(related_map, fn_to_apply, query_set, fields):
            for related_tuple in related_map:
                fields_mapped = [field for field in related_tuple[1] if field in fields]
                if fields_mapped:
                    query_set = getattr(query_set, fn_to_apply)(*related_tuple[0])
            return query_set

        serializer_class = self.get_serializer_class()
        included_fields = self.filter_fields(serializer_class.Meta.fields, self.request)
        final_query_set = serializer_class.Meta.model.objects.all()
        if self.select_related_map:
            final_query_set = apply_related_fns(self.select_related_map, "select_related", final_query_set, included_fields)

        if self.prefetch_related_map:
            final_query_set = apply_related_fns(self.prefetch_related_map, "prefetch_related", final_query_set, included_fields)
        return final_query_set


class ProductViewSet(FieldLimiterMixin, viewsets.ReadOnlyModelViewSet):
    serializer_class = ProductSerializer
    pagination_class = DefaultPagination
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = ProductListFilter
    ordering_fields = ('vendor_part_num', 'description', 'retail_price', 'jobber_price', 'min_price', 'core_charge', 'can_drop_ship', 'drop_ship_fee', 'vendor', 'category', 'sub_category',)
    ordering = ('vendor_part_num',)
    select_related_map = (
        (("vendor",), ("vendor",)),
        (("vendor_product_line",), ("vendor_product_line",))
    )
    fit_obj = "productfitment_set__vehicle__"
    prefetch_related_map = (
        (("productcategory_set__category",), ("category", "sub_category",)),
        (("images",), ("images",)),
        ((fit_obj + "make", fit_obj + "model", fit_obj + "sub_model", fit_obj + "engine",), ("fitment",))
    )

class CategoryViewSet(viewsets.ReadOnlyModelViewSet):
    # permission_classes = (permissions.IsAuthenticated,)
    queryset = Category.objects.all()
    serializer_class = CategorySerializer
    pagination_class = DefaultPagination
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = CategoryListFilters
    ordering_fields = ('name',)
    ordering = ('name',)


class VendorViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Vendor.objects.all()
    serializer_class = VendorSerializer
    pagination_class = DefaultPagination

    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = VendorListFilters
    ordering_fields = ('name',)
    ordering = ('name',)


def import_products(request):
    Turn14DataImporter().import_and_store_product_data(refresh_all=False)
    return HttpResponse("Doing work!")
