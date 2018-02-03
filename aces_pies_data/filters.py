import logging

import django_filters

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


class ProductListFilter(django_filters.rest_framework.FilterSet):
    brand_id = django_filters.BaseInFilter(method="brand_filter")
    category_id = django_filters.BaseInFilter(method="category_filter")
    has_map_price = django_filters.BooleanFilter(method="has_map_price_filter", label="Has Map Price")
    min_map = django_filters.NumberFilter(name="map_price", lookup_expr='gte')
    max_map = django_filters.NumberFilter(name="map_price", lookup_expr='lte')
    is_universal_fitment = django_filters.BooleanFilter(method="is_universal_fitment_filter", label="Universal Fitment")
    fitment = django_filters.CharFilter(method="fitment_filter", label="Fitment, example: 2005 Chevrolet Corvette.  Multi year, 2008-2013 Chevrolet Corvette")
    has_images = django_filters.BooleanFilter(method="has_images_filter", label="Has images")

    def fitment_filter(self, queryset, name, val):
        """
        Below filters use rawsql because built in django exists filter is quite slow as it adds unnecessary group bys
        The performance on the filter goes from 100+ seconds to under 1 second
        Culprit was entire SQL turns into a subquery with odd group bys
        """
        fitment_query = val.split()
        year, make, model = fitment_query[0:3]
        engine, engine_condition_sql, engine_condition_join = None, '', ''
        if len(fitment_query) == 4:
            engine = fitment_query[3]
        year_range = year.split("-")
        if len(year_range) > 1:
            years = range(int(year_range[0]), int(year_range[1]) + 1)
        else:
            years = [int(year)]
        years_conditions = list()
        for year in years:
            years_conditions.append(f"({year} BETWEEN aces_pies_data_productfitment.start_year and aces_pies_data_productfitment.end_year)")
        years_condition_sql = " OR ".join(years_conditions)
        if engine:
            engine_condition_join = "INNER JOIN aces_pies_data_vehicleengine ON aces_pies_data_vehicle.engine_id = aces_pies_data_vehicleengine.id"
            engine_condition_sql = f"AND aces_pies_data_vehicleengine.configuration = '{engine}'"
        queryset = queryset.extra(where=[f"""
            EXISTS (
                SELECT 1 FROM aces_pies_data_productfitment 
                INNER JOIN aces_pies_data_vehicle ON aces_pies_data_vehicle.id = aces_pies_data_productfitment.vehicle_id
                INNER JOIN aces_pies_data_vehiclemake ON aces_pies_data_vehiclemake.id = aces_pies_data_vehicle.make_id
                INNER JOIN aces_pies_data_vehiclemodel ON aces_pies_data_vehiclemodel.id = aces_pies_data_vehicle.model_id
                {engine_condition_join}
                WHERE aces_pies_data_productfitment.product_id = aces_pies_data_product.id
                AND aces_pies_data_vehiclemake.name = '{make}'
                AND aces_pies_data_vehiclemodel.name = '{model}'
                AND ({years_condition_sql})
                {engine_condition_sql}
            )
        """])
        return queryset

    def has_images_filter(self, queryset, name, val):
        not_exists = "" if val else "NOT"
        queryset = queryset.extra(where=[f"""
            {not_exists} EXISTS (
                SELECT 1 FROM aces_pies_data_productdigitalasset pda
                INNER JOIN aces_pies_data_digitalasset da on da.id = pda.digital_asset_id
                INNER JOIN aces_pies_data_digitalassettype dat on da.type_id = dat.id
                WHERE pda.product_id = aces_pies_data_product.id
                AND dat.name = 'Product Image'
            )
        """])
        return queryset

    def brand_filter(self, queryset, name, value):
        return queryset.filter(**{
            "brand__name__in": value
        })

    def category_filter(self, queryset, name, value):
        return queryset.filter(**{
            "category__name__in": value
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
