import logging

import django_filters
from rest_framework.pagination import PageNumberPagination

from supplier.models import Product, Vendor, Category

logger = logging.getLogger(__name__)


def get_numeric_filter_list(raw_filter_list):
    filter_list = list()
    for val in raw_filter_list:
        try:
            filter_list.append(int(val))
        except:
            logger.warning("Passed filter value {0} not numeric".format(val))
    return filter_list


class ProductListFilter(django_filters.rest_framework.FilterSet):
    min_cost = django_filters.NumberFilter(name="cost", lookup_expr='gte')
    max_cost = django_filters.NumberFilter(name="cost", lookup_expr='lte')
    max_core_charge = django_filters.NumberFilter(name='core_charge', lookup_expr='lte')
    no_core_charge = django_filters.BooleanFilter(name='core_charge', lookup_expr='isnull')
    vendor_id = django_filters.BaseInFilter(method="vendor_filter")
    can_drop_ship = django_filters.BaseInFilter(method='can_drop_ship_filter')
    category_id = django_filters.BaseInFilter(method='category_filter')
    description = django_filters.CharFilter(method="description_filter")
    has_images = django_filters.BooleanFilter(method="has_images_filter")
    min_stock = django_filters.NumberFilter(name="stock", lookup_expr='gte')
    max_stock = django_filters.NumberFilter(name="stock", lookup_expr='lte')
    min_profit = django_filters.NumberFilter(name="profit", lookup_expr='gte')
    max_profit = django_filters.NumberFilter(name="profit", lookup_expr='lte')
    has_min_price = django_filters.BooleanFilter(method="has_min_price_filter")

    # If no image thumb, assume no images at all for faster filtering
    def has_images_filter(self, queryset, name, value):
        return queryset.filter(**{
            "remote_image_thumb__isnull": not value,
        })

    def has_min_price_filter(self, queryset, name, value):
        return queryset.filter(**{
            "min_price__isnull": not value,
        })

    def can_drop_ship_filter(self, queryset, name, value):
        filter_list = list()
        for val in value:
            filter_list.append(Product.get_drop_ship_val(val))
        return queryset.filter(**{
            "can_drop_ship__in": filter_list
        })

    def description_filter(self, queryset, name, value):
        return queryset.filter(**{
            "description__contains": value
        })

    def vendor_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "vendor__id__in": filter_list
        })

    def category_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "productcategory__category__id__in": filter_list
        }).distinct()

    class Meta:
        model = Product
        fields = ('internal_part_num', 'vendor_part_num', 'is_carb_legal',)


class SimpleIdListFilter(django_filters.rest_framework.FilterSet):
    id = django_filters.BaseInFilter(method='id_filter')

    def id_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "id__in": filter_list
        })


class VendorListFilters(SimpleIdListFilter):
    category_id = django_filters.BaseInFilter(method="category_filter")

    def category_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "product__productcategory__category__pk__in": filter_list
        }).distinct()

    class Meta:
        model = Vendor
        fields = ('id',)


class CategoryListFilters(SimpleIdListFilter):
    top_level_only = django_filters.BooleanFilter(method="top_level_only_filter")
    vendor_id = django_filters.BaseInFilter(method="vendor_filter")

    def vendor_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "productcategory__product__vendor__pk__in": filter_list
        }).distinct()

    def top_level_only_filter(self, queryset, name, value):
        return queryset.filter(**{
            "parent_category__isnull": value,
        })

    class Meta:
        model = Category
        fields = ('id',)


class DefaultPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = "page_size"
