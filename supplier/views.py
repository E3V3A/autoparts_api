import django_filters
import logging
from django.http import HttpResponse
from rest_framework import generics
from rest_framework import viewsets
from rest_framework.pagination import PageNumberPagination
from rest_framework.filters import OrderingFilter
from django_filters.rest_framework import DjangoFilterBackend, FilterSet
from supplier.models import Product, Category, ProductImageMap, Vendor
from supplier.serializers import ProductSerializer, CategorySerializer, VendorSerializer
from supplier.utils.turn14_data_importer import Turn14DataImporter

logger = logging.getLogger(__name__)


class ProductListFilter(django_filters.rest_framework.FilterSet):
    min_cost = django_filters.NumberFilter(name="cost", lookup_expr='gte')
    max_cost = django_filters.NumberFilter(name="cost", lookup_expr='lte')
    max_core_charge = django_filters.NumberFilter(name='core_charge', lookup_expr='lte')
    no_core_charge = django_filters.BooleanFilter(name='core_charge', lookup_expr='isnull')
    vendor = django_filters.BaseInFilter(name="vendor__name", method="vendor_filter")
    can_drop_ship = django_filters.CharFilter(name='can_drop_ship', method='can_drop_ship_filter')
    category = django_filters.BaseInFilter(method='category_filter')
    description = django_filters.CharFilter(name="description", method="description_filter")
    has_images = django_filters.BooleanFilter(method="has_images_filter")

    def has_images_filter(self,queryset, name, value):
        return queryset.filter(**{
            "product_images__isnull": not value,
        }).distinct()

    def get_numeric_filter_list(self, raw_filter_list):
        filter_list = list()
        for val in raw_filter_list:
            try:
                filter_list.append(int(val))
            except:
                logger.warning("Passed filter value {0} not numeric".format(val))
        return filter_list

    def can_drop_ship_filter(self, queryset, name, value):
        try:
            number_val = int(value)
        except ValueError:
            number_val = Product.get_drop_ship_val(value)
        return queryset.filter(**{
            name: number_val,
        })

    def description_filter(self, queryset, name, value):
        return queryset.filter(**{
            "description__contains": value
        })

    def vendor_filter(self, queryset, name, value):
        filter_list = self.get_numeric_filter_list(value)
        return queryset.filter(**{
            "vendor__id__in": filter_list
        })

    def category_filter(self, queryset, name, value):
        filter_list = self.get_numeric_filter_list(value)
        return queryset.filter(**{
            "category__parent_category__id__in": filter_list
        }) | queryset.filter(**{
            "category__id__in": filter_list,
            "category__parent_category__isnull": True
        })

    class Meta:
        model = Product
        fields = ('vendor_part_num',)


class DefaultPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = "page_size"

class ProductViewSet(viewsets.ReadOnlyModelViewSet):
    # permission_classes = (permissions.IsAuthenticated,)
    queryset = Product.objects.all()
    serializer_class = ProductSerializer
    pagination_class = DefaultPagination
    filter_backends = (DjangoFilterBackend, OrderingFilter)
    filter_class = ProductListFilter
    ordering_fields = ('vendor_part_num', 'description', 'retail_price', 'jobber_price', 'min_price', 'core_charge', 'can_drop_ship', 'drop_ship_fee', 'vendor', 'category', 'sub_category',)

    #TODO: figure out how to order on the custom category/sub_category fields
    #possible solution http://stackoverflow.com/questions/24987446/django-rest-framework-queryset-doesnt-order

class CategoryViewSet(viewsets.ReadOnlyModelViewSet):
    # permission_classes = (permissions.IsAuthenticated,)
    queryset = Category.objects.all()
    serializer_class = CategorySerializer
    pagination_class = DefaultPagination
    # filter_backends = (django_filters.rest_framework.DjangoFilterBackend,)
    # filter_class = ProductListFilter
    # filter_fields = ('vendor_part_num',)


class VendorViewSet(viewsets.ReadOnlyModelViewSet):
    # permission_classes = (permissions.IsAuthenticated,)
    queryset = Vendor.objects.all()
    serializer_class = VendorSerializer
    pagination_class = DefaultPagination

    # filter_backends = (django_filters.rest_framework.DjangoFilterBackend,)
    # filter_class = ProductListFilter
    # filter_fields = ('vendor_part_num',)


def import_products(request):
    Turn14DataImporter().import_and_store_product_data()
    return HttpResponse("Doing work!")


"""
Code below is for HATEOAS

class ProductHighlight(generics.GenericAPIView):
    queryset = Product.objects.all()
    renderer_classes = (renderers.StaticHTMLRenderer,)

    def get(self, request, *args, **kwargs):
        snippet = self.get_object()
        return Response(snippet.highlighted)

@api_view(['GET'])
def api_root(request, format=None):
    return Response({
        'products': reverse('supplier:product-list', request=request, format=format)
    })

"""
