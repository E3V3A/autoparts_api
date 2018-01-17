import logging

import django_filters
from django.db.models import Q

from .models import Product, Brand, Category

logger = logging.getLogger(__name__)


def get_numeric_filter_list(raw_filter_list):
    filter_list = list()
    for val in raw_filter_list:
        try:
            filter_list.append(int(val))
        except:
            logger.warning("Passed filter value {0} not numeric".format(val))
    return filter_list


def parse_in_filter_string(filter_string):
    strings = list()
    numbers = list()

    def is_number(_value):
        try:
            int(_value)
            return True
        except ValueError:
            return False

    for value in filter_string:
        if is_number(value):
            numbers.append(value)
        else:
            strings.append(value)
    return {
        "numbers": numbers,
        "strings": strings
    }


def get_ids_for_in_filter(filter_string, queryset, string_field, number_field):
    filter_vals = parse_in_filter_string(filter_string)
    strings = filter_vals['strings']
    numbers = filter_vals['numbers']

    id_queryset_set = queryset
    if strings and numbers:
        id_queryset_set = id_queryset_set.filter(Q(**{string_field + "__in": strings}) | Q(**{number_field + "__in": numbers}))
    elif strings:
        id_queryset_set = id_queryset_set.filter(**{string_field + "__in": strings})
    elif numbers:
        id_queryset_set = id_queryset_set.filter(**{number_field + "__in": numbers})

    return id_queryset_set.values_list('id', flat=True).distinct()


class ProductListFilter(django_filters.rest_framework.FilterSet):
    brand_id = django_filters.BaseInFilter(method="brand_filter")
    category_id = django_filters.BaseInFilter(method="category_filter")
    has_map_price = django_filters.BooleanFilter(method="has_map_price_filter", label="Has Map Price")
    min_map = django_filters.NumberFilter(name="map_price", lookup_expr='gte')
    max_map = django_filters.NumberFilter(name="map_price", lookup_expr='lte')
    is_universal_fitment = django_filters.BooleanFilter(method="is_universal_fitment_filter", label="Universal Fitment")
    fits_make = django_filters.BaseInFilter(method="fits_make_filter", label="Fits Make(s)")
    fits_model = django_filters.BaseInFilter(method="fits_model_filter", label="Fits Model(s)")
    years = django_filters.BaseInFilter(method="years_filter", label="Fits Years")

    def brand_filter(self, queryset, name, value):
        ids_for_filter = get_ids_for_in_filter(value, Product.objects, "brand__name", "brand_id")
        return queryset.filter(**{
            "id__in": ids_for_filter
        })

    def fits_make_filter(self, queryset, name, value):
        ids_for_filter = get_ids_for_in_filter(value, Product.objects, "fitment__vehicle__make__name", "fitment__vehicle__make__id")
        return queryset.filter(**{
            "id__in": ids_for_filter
        })

    def fits_model_filter(self, queryset, name, value):
        ids_for_filter = get_ids_for_in_filter(value, Product.objects, "fitment__vehicle__model__name", "fitment__vehicle__model__id")
        return queryset.filter(**{
            "id__in": ids_for_filter
        })

    def years_filter(self, queryset, name, value):
        id_query_set = Product.objects
        all_q_construct = None
        for year in value:
            q_construct = Q(fitment__start_year__gte=year, fitment__end_year__lte=year)
            if all_q_construct is None:
                all_q_construct = q_construct
            else:
                all_q_construct = all_q_construct | q_construct
        ids_for_filter = id_query_set.filter(all_q_construct).values_list('id', flat=True).distinct()
        return queryset.filter(**{
            "id__in": ids_for_filter
        })

    def category_filter(self, queryset, name, value):
        ids_for_filter = get_ids_for_in_filter(value, Product.objects, "category__name", "category_id")
        return queryset.filter(**{
            "id__in": ids_for_filter
        })

    def has_map_price_filter(self, queryset, name, value):
        return queryset.filter(**{
            "map_price__isnull": not value,
        })

    def is_universal_fitment_filter(self, queryset, name, value):
        if value:
            return queryset.filter(**{
                "fitment_count": 0,
            })
        else:
            return queryset.filter(**{
                "fitment_count__gte": 1,
            })

    class Meta:
        model = Product
        fields = ('part_number', 'is_hazardous', 'is_carb_legal', 'is_discontinued', 'is_obsolete',)


class SimpleIdListFilter(django_filters.rest_framework.FilterSet):
    id = django_filters.BaseInFilter(method='id_filter')

    def id_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "id__in": filter_list
        })


class BrandListFilters(SimpleIdListFilter):
    category_id = django_filters.BaseInFilter(method="category_filter")

    def category_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "product__category__pk__in": filter_list
        }).distinct()

    class Meta:
        model = Brand
        fields = ('id',)


class CategoryListFilters(SimpleIdListFilter):
    top_level_only = django_filters.BooleanFilter(method="top_level_only_filter")
    brand_id = django_filters.BaseInFilter(method="brand_filter")

    def brand_filter(self, queryset, name, value):
        filter_list = get_numeric_filter_list(value)
        return queryset.filter(**{
            "product__brand__pk__in": filter_list
        }).distinct()

    def top_level_only_filter(self, queryset, name, value):
        return queryset.filter(**{
            "parent__isnull": value,
        })

    class Meta:
        model = Category
        fields = ('id',)
