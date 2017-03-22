import django_filters
import logging

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

    def has_images_filter(self, queryset, name, value):
        return queryset.filter(**{
            "images__isnull": not value,
        }).distinct()

    def can_drop_ship_filter(self, queryset, name, value):
        filter_list = list()
        for val in value:
            filter_list.append(Product.get_drop_ship_val(val))
        return queryset.filter(**{
            "can_drop_ship__in": filter_list,
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
            "category__parent_category__id__in": filter_list
        }) | queryset.filter(**{
            "category__id__in": filter_list,
            "category__parent_category__isnull": True
        })

    class Meta:
        model = Product
        fields = ('vendor_part_num',)


class SimpleIdListFilter(django_filters.rest_framework.FilterSet):
    id = django_filters.BaseInFilter(method='id_filter')

    def id_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "id__in": filter_list
        })


class VendorListFilters(SimpleIdListFilter):
    class Meta:
        model = Vendor
        fields = ('id',)


class CategoryListFilters(SimpleIdListFilter):
    top_level_only = django_filters.BooleanFilter(method="top_level_only_filter")

    def top_level_only_filter(self, queryset, name, value):
        return queryset.filter(**{
            "parent_category__isnull": value,
        }).distinct()

    class Meta:
        model = Category
        fields = ('id',)


class DefaultPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = "page_size"
