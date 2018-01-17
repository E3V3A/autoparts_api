import logging
import os
import re
import zipfile

from django.db.models import Count, F
from django.http import HttpResponse
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import viewsets, generics
from rest_framework.decorators import detail_route
from rest_framework.filters import OrderingFilter
from rest_framework.response import Response
from rest_framework.views import APIView

from aces_pies_data.util.aces_pies_parsing import PiesFileParser
from aces_pies_data.util.aces_pies_storage import PiesDataStorage
from .filters import BrandListFilters, CategoryListFilters, ProductListFilter
from .models import Category, Brand, Product, Attribute, ProductFitment
from .serializers import CategorySerializer, BrandSerializer, ProductSerializer, AttributeSerializer, ProductFitmentSerializer

logger = logging.getLogger(__name__)


class FieldLimiterMixin(object):
    """
    Prefetch_related_map and select_related_map is a list of tuples
    0 index represents a list of relationships to fetch
    1 index represets a list of fields those relationships apply to
    Blacklist fields by passing the -fields= querystring.
    Whitelist fields by passing +fields=
    """
    prefetch_related_map = None
    select_related_map = None

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
        if not self.queryset:
            final_query_set = serializer_class.Meta.model.objects.all()
        else:
            final_query_set = self.queryset
        if self.select_related_map:
            final_query_set = apply_related_fns(self.select_related_map, "select_related", final_query_set, included_fields)

        if self.prefetch_related_map:
            final_query_set = apply_related_fns(self.prefetch_related_map, "prefetch_related", final_query_set, included_fields)
        return final_query_set


class AttributeViewSet(FieldLimiterMixin, viewsets.ReadOnlyModelViewSet):
    queryset = Attribute.objects.all()
    serializer_class = AttributeSerializer
    # filter_backends = (DjangoFilterBackend, OrderingFilter)
    # filter_class = CategoryListFilters
    ordering_fields = ('name',)
    ordering = ('name',)


class CategoryViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Category.objects.all()
    serializer_class = CategorySerializer
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = CategoryListFilters
    ordering_fields = ('name',)
    ordering = ('name',)


class BrandViewSet(FieldLimiterMixin, viewsets.ReadOnlyModelViewSet):
    queryset = Brand.objects.all()
    serializer_class = BrandSerializer
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = BrandListFilters
    ordering_fields = ('name',)
    ordering = ('name',)
    select_related_map = (
        (("logo",), ("logo",)),
    )


class ProductViewSet(FieldLimiterMixin, viewsets.ReadOnlyModelViewSet):
    queryset = Product.objects.all()
    serializer_class = ProductSerializer
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = ProductListFilter
    ordering_fields = ('part_number', 'name', 'brand', 'category',)
    ordering = ('part_number',)
    select_related_map = (
        (("brand",), ("brand",)),
        (("category",), ("category",))
    )

    prefetch_related_map = (
        (("attributes__attribute", "attributes__value"), ("attributes",)),
        (("features",), ("features",)),
        (("packages",), ("packages",)),
        (("digital_assets__digital_asset__type",), ("digital_assets",)),
    )


class ProductFitmentListView(generics.ListAPIView):
    queryset = ProductFitment.objects.select_related("vehicle", "vehicle__make", "vehicle__model", "vehicle__sub_model", "vehicle__engine", "vehicle__engine__fuel_delivery", "vehicle__engine__fuel_type", "vehicle__engine__aspiration", ).all()
    queryset = queryset.annotate(make=F('vehicle__make__name')).annotate(model=F('vehicle__model__name'))
    ordering_fields = ('start_year', "make", "model")
    ordering = ('start_year',)

    serializer_class = ProductFitmentSerializer
    filter_backends = (DjangoFilterBackend, OrderingFilter)

    def get_queryset(self):
        return self.queryset.filter(product_id=int(self.kwargs['pk']))