from django.db import models
from mptt.fields import TreeForeignKey
from mptt.models import MPTTModel


class Base(models.Model):
    created_on = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_on = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        abstract = True
        ordering = ('created_on',)


class DigitalAssetType(Base):
    name = models.CharField(max_length=100)


class DigitalAsset(Base):
    type = models.ForeignKey(DigitalAssetType, on_delete=models.CASCADE)
    url = models.CharField(max_length=200, unique=True)
    file_size_bytes = models.PositiveIntegerField(null=True)

    class Meta:
        unique_together = ("url", "type",)


class Brand(Base):
    name = models.CharField(max_length=100, unique=True, db_index=True)
    # TODO when parsing from sema, check the file name prefix and see how it matches up against DCI's standard
    short_name = models.CharField(max_length=10, unique=True, db_index=True)
    logo = models.ForeignKey(DigitalAsset, null=True, on_delete=models.CASCADE)
    marketing_copy = models.TextField(null=True)


class Category(MPTTModel):
    name = models.CharField(max_length=100, unique=True, db_index=True)
    parent = TreeForeignKey('self', null=True, blank=True, related_name='children', db_index=True, on_delete=models.PROTECT)


class Product(Base):
    part_number = models.CharField(max_length=50, db_index=True)
    name = models.CharField(max_length=300, db_index=True)
    brand = models.ForeignKey(Brand, on_delete=models.CASCADE)
    is_hazardous = models.BooleanField(default=False, db_index=True)
    is_carb_legal = models.BooleanField(default=True, db_index=True)
    is_discontinued = models.BooleanField(default=False, db_index=True)
    is_superseded = models.BooleanField(default=False, db_index=True)
    superseded_by = models.CharField(null=True, max_length=50, db_index=True)
    is_obsolete = models.BooleanField(default=False, db_index=True)
    map_price = models.DecimalField(max_digits=7, decimal_places=2, null=True, db_index=True)
    retail_price = models.DecimalField(max_digits=7, decimal_places=2, null=True, db_index=True)
    category = models.ForeignKey(Category, on_delete=models.PROTECT)

    class Meta:
        unique_together = ("part_number", "brand",)


class ProductFeature(Base):
    name = models.CharField(max_length=300)
    listing_sequence = models.PositiveSmallIntegerField()
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="features")

    class Meta:
        unique_together = ("name", "product",)
        ordering = ('listing_sequence',)


class Attribute(Base):
    name = models.CharField(max_length=100, db_index=True)
    category = models.ForeignKey(Category, on_delete=models.CASCADE, related_name="attributes")

    class Meta:
        unique_together = ("name", "category",)
        ordering = ("name",)


class AttributeValue(Base):
    attribute = models.ForeignKey(Attribute, on_delete=models.CASCADE, related_name="values")
    value = models.CharField(max_length=300, db_index=True)

    class Meta:
        unique_together = ("attribute", "value",)
        ordering = ("value",)


class ProductAttribute(Base):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="attributes")
    attribute = models.ForeignKey(Attribute, on_delete=models.CASCADE)
    value = models.ForeignKey(AttributeValue, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("product", "attribute", "value",)
        ordering = ("attribute__name", "value__value",)


class ProductPackaging(Base):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="packages")
    product_quantity = models.PositiveSmallIntegerField()
    weight = models.DecimalField(max_digits=7, decimal_places=2, null=True)
    dimensional_weight = models.DecimalField(max_digits=7, decimal_places=2, null=True)
    height = models.DecimalField(max_digits=7, decimal_places=2, null=True)
    length = models.DecimalField(max_digits=7, decimal_places=2, null=True)
    width = models.DecimalField(max_digits=7, decimal_places=2, null=True)


class ProductDigitalAsset(Base):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="digital_assets")
    digital_asset = models.ForeignKey(DigitalAsset, on_delete=models.CASCADE)
    display_sequence = models.PositiveSmallIntegerField()

    class Meta:
        unique_together = ("product", "digital_asset",)
        ordering = ('display_sequence',)


class ImportTrackingType(Base):
    name = models.CharField(max_length=50, db_index=True)


class ImportTrackingManager(models.Manager):
    def get_import_action(self, brand_short_name, date_time, import_type):
        last_completed_import = self.filter(brand_short_name=brand_short_name, tracking_type__name=import_type, end_date__isnull=False).order_by("-start_date").first()
        if last_completed_import and last_completed_import.start_date >= date_time:
            return ImportTracking.DO_ARCHIVE
        else:
            # Order of importing must be pies_flat, pies, then aces
            if import_type == "pies_flat" or (import_type == "pies" and ProductCategoryLookup.objects.filter(brand_short_name=brand_short_name).exists()) or (import_type == "aces" and Brand.objects.filter(short_name=brand_short_name).exists()):
                return ImportTracking.DO_IMPORT
            return ImportTracking.NO_ACTION


class ImportTracking(Base):
    DO_IMPORT = 1
    DO_ARCHIVE = 2
    NO_ACTION = 3

    action_choices = (
        (DO_IMPORT, 'Import'),
        (DO_ARCHIVE, 'Archive'),
        (NO_ACTION, 'No Action'),
    )

    tracking_type = models.ForeignKey(ImportTrackingType, on_delete=models.PROTECT, related_name="tracking_records")
    import_action = models.IntegerField(choices=action_choices, default=1, db_index=True)
    file_name = models.CharField(max_length=100)
    stack_trace = models.TextField(null=True)
    brand_short_name = models.CharField(max_length=10, db_index=True)
    start_date = models.DateTimeField(auto_now_add=True, db_index=True)
    end_date = models.DateTimeField(db_index=True, null=True)
    objects = ImportTrackingManager()


class VehicleMake(Base):
    name = models.CharField(max_length=100, db_index=True, unique=True)


class VehicleModel(Base):
    name = models.CharField(max_length=100, db_index=True)
    make = models.ForeignKey(VehicleMake, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("name", "make",)


class VehicleSubModel(Base):
    name = models.CharField(max_length=100, db_index=True)
    model = models.ForeignKey(VehicleModel, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("name", "model",)


class FuelType(Base):
    name = models.CharField(max_length=20)


class FuelDelivery(Base):
    name = models.CharField(max_length=10)


class EngineAspiration(Base):
    name = models.CharField(max_length=12)


class VehicleEngine(Base):
    configuration = models.CharField(max_length=3, db_index=True)
    liters = models.DecimalField(max_digits=3, decimal_places=1, null=True, db_index=True)
    engine_code = models.CharField(max_length=100, db_index=True, null=True)
    aspiration = models.ForeignKey(EngineAspiration, on_delete=models.PROTECT, related_name="engines")
    fuel_type = models.ForeignKey(FuelType, on_delete=models.PROTECT, related_name="engines")
    fuel_delivery = models.ForeignKey(FuelDelivery, on_delete=models.PROTECT, related_name="engines")

    class Meta:
        unique_together = ("configuration", "liters", "fuel_type", "fuel_delivery", "aspiration", "engine_code",)


class Vehicle(Base):
    make = models.ForeignKey(VehicleMake, on_delete=models.CASCADE)
    model = models.ForeignKey(VehicleModel, on_delete=models.CASCADE)
    sub_model = models.ForeignKey(VehicleSubModel, on_delete=models.CASCADE, null=True)
    engine = models.ForeignKey(VehicleEngine, on_delete=models.CASCADE, null=True)

    class Meta:
        unique_together = ("make", "model", "sub_model", "engine",)


"""
Years are denormalized now to save space
Original design was to put 1 year per row for each table, but this took up too much space for little gain
Now the data is denormalized and each table stores a range of years instead
This still allows the ability to easily determine if a part fits a given car or not and there isn't much downside to doing it this way
"""


class VehicleYear(Base):
    vehicle = models.ForeignKey(Vehicle, on_delete=models.CASCADE)
    year = models.PositiveIntegerField(db_index=True)

    class Meta:
        unique_together = ("vehicle", "year",)


class ProductFitment(Base):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="fitment")
    vehicle = models.ForeignKey(Vehicle, on_delete=models.CASCADE)
    start_year = models.PositiveSmallIntegerField(db_index=True)
    end_year = models.PositiveSmallIntegerField(db_index=True)
    fitment_info_1 = models.CharField(max_length=1000, null=True)
    fitment_info_2 = models.CharField(max_length=1000, null=True)

    class Meta:
        unique_together = ("product", "vehicle", "start_year", "end_year", "fitment_info_1", "fitment_info_2",)


class ProductCategoryLookup(Base):
    """
    Since we use the full XML file to parse Pies data, we cannot get the category until we get an account with autocare.org, which gives access to the category database.
    The XML file only has the ID field.
    DCI also sends over the flat file, which contains less information, but does contain the category field.  We won't be able to get the parent categories, but we will be able
    to get the lowest level category, which is the part type.  I.E Spark plug.

    The problem is there is no real way to sync the XML and flat files, they could be sent at different times with different parts.  This table is designed to hold the part_number->category info.
    When the XML File is parsed, it will look at this table to get the category.  If it cannot get it,  the field will remain NULL.
    """

    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    brand_short_name = models.CharField(max_length=10, db_index=True)
    part_number = models.CharField(max_length=50, db_index=True)

    class Meta:
        unique_together = ("brand_short_name", "category", "part_number",)
