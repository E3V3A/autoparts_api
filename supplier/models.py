from django.core.files.storage import FileSystemStorage
from django.db import models


class Base(models.Model):
    created_on = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_on = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        abstract = True
        ordering = ('created_on',)


class Vendor(Base):
    name = models.CharField(max_length=50, unique=True, db_index=True)


class VendorProductLine(Base):
    name = models.CharField(max_length=150, db_index=True)
    vendor = models.ForeignKey(Vendor, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("name", "vendor",)


class Category(Base):
    name = models.CharField(max_length=50, db_index=True)
    parent_category = models.ForeignKey('self', on_delete=models.CASCADE, null=True, related_name='parent')

    class Meta:
        unique_together = ("name", "parent_category",)


class Product(Base):
    ALWAYS_DROPSHIP = 1
    NEVER_DROPSHIP = 0
    POSSIBLE_DROPSHIP = 2
    DROPSHIP_CHOICES = (
        (ALWAYS_DROPSHIP, 'Always'),
        (NEVER_DROPSHIP, 'Never'),
        (POSSIBLE_DROPSHIP, 'Possible'),
    )

    internal_part_num = models.CharField(max_length=30, unique=True, db_index=True)
    vendor_part_num = models.CharField(max_length=30, db_index=True)
    internal_item_code = models.CharField(max_length=30, null=True, db_index=True)
    description = models.CharField(max_length=300, db_index=True)
    overview = models.TextField(null=True)
    cost = models.DecimalField(max_digits=7, decimal_places=2, null=True, db_index=True)
    retail_price = models.DecimalField(max_digits=7, decimal_places=2, null=True, db_index=True)
    jobber_price = models.DecimalField(max_digits=7, decimal_places=2, null=True, db_index=True)
    min_price = models.DecimalField(max_digits=7, decimal_places=2, null=True, db_index=True)
    core_charge = models.DecimalField(max_digits=7, decimal_places=2, null=True, db_index=True)
    weight_in_lbs = models.DecimalField(max_digits=6, decimal_places=2, null=True, db_index=True)
    can_drop_ship = models.IntegerField(choices=DROPSHIP_CHOICES, default=NEVER_DROPSHIP, db_index=True)
    drop_ship_fee = models.DecimalField(max_digits=5, decimal_places=2, null=True, db_index=True)
    vendor = models.ForeignKey(Vendor, on_delete=models.CASCADE)
    vendor_product_line = models.ForeignKey(VendorProductLine, on_delete=models.CASCADE, null=True)
    remote_image_thumb = models.CharField(max_length=150, null=True)

    @staticmethod
    def get_drop_ship_val(drop_ship_text):
        for dropship_choice in Product.DROPSHIP_CHOICES:
            if dropship_choice[1].lower() == drop_ship_text.lower():
                return dropship_choice[0]
        return None


class ProductCategory(Base):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)

    class Meta:
        unique_together = ('product', 'category',)


"""
If grabbing and storing the images on our own server, use below ImageStorage class paired with an ImageField.  This will handle any duplicates and cleaning up of deleted files
For now, we are just storing the remote CDN url directly in a charfield.  But it may be a good idea in the future to hold all that data ourselves, since we do not have control over their CDN.
"""


class ImageStorage(FileSystemStorage):
    def _save(self, name, content):
        if self.exists(name):
            self.delete(name)
        return super(ImageStorage, self)._save(name, content)

    def get_available_name(self, name, max_length=None):
        return name


class ProductImage(Base):
    # image_file = models.ImageField(upload_to="products/", max_length=150, storage=ImageStorage(), db_index=True)
    remote_image_file = models.CharField(max_length=150, db_index=True)
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="images")
    """
    below for handling duplicates

    def save(self, *args, **kwargs):
        ProductImage.objects.filter(image_file=self.image_file).delete()
        super(ProductImage, self).save(*args, **kwargs)
    """


class VehicleMake(Base):
    name = models.CharField(max_length=50, db_index=True, unique=True)


class VehicleModel(Base):
    name = models.CharField(max_length=50, db_index=True)
    make = models.ForeignKey(VehicleMake, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("name", "make",)


class VehicleSubModel(Base):
    name = models.CharField(max_length=50, db_index=True)
    model = models.ForeignKey(VehicleModel, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("name", "model",)


class VehicleEngine(Base):
    name = models.CharField(max_length=50, db_index=True, unique=True)


class VehicleSubModelEngine(Base):
    sub_model = models.ForeignKey(VehicleSubModel, on_delete=models.CASCADE)
    engine = models.ForeignKey(VehicleEngine, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("sub_model", "engine",)


class Vehicle(Base):
    make = models.ForeignKey(VehicleMake, on_delete=models.CASCADE)
    model = models.ForeignKey(VehicleModel, on_delete=models.CASCADE)
    sub_model = models.ForeignKey(VehicleSubModel, on_delete=models.CASCADE)
    engine = models.ForeignKey(VehicleEngine, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("make", "model", "sub_model", "engine",)


class VehicleYear(Base):
    vehicle = models.ForeignKey(Vehicle, on_delete=models.CASCADE)
    year = models.PositiveIntegerField(db_index=True)

    class Meta:
        unique_together = ("year", "vehicle",)


class ProductFitment(Base):
    start_year = models.PositiveIntegerField(db_index=True)
    end_year = models.PositiveIntegerField(db_index=True)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    vehicle = models.ForeignKey(Vehicle, on_delete=models.CASCADE)
    note = models.CharField(max_length=300, null=True, db_index=True)

    class Meta:
        unique_together = ("start_year", "end_year", "product", "vehicle", "note",)
