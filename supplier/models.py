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
    vendor = models.ForeignKey(Vendor, on_delete=models.CASCADE, db_index=True)

    class Meta:
        unique_together = ("name", "vendor",)


class Category(Base):
    name = models.CharField(max_length=50, db_index=True)
    parent_category = models.ForeignKey('self', on_delete=models.CASCADE, null=True, related_name='parent', db_index=True)


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
    vendor = models.ForeignKey(Vendor, on_delete=models.CASCADE, db_index=True)
    vendor_product_line = models.ForeignKey(VendorProductLine, on_delete=models.CASCADE, db_index=True, null=True)
    category = models.ForeignKey(Category, on_delete=models.CASCADE, null=True, db_index=True)

    @staticmethod
    def get_drop_ship_val(drop_ship_text):
        for dropship_choice in Product.DROPSHIP_CHOICES:
            if dropship_choice[1].lower() == drop_ship_text.lower():
                return dropship_choice[0]
        return None


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

    """
    below for handling duplicates

    def save(self, *args, **kwargs):
        ProductImage.objects.filter(image_file=self.image_file).delete()
        super(ProductImage, self).save(*args, **kwargs)
    """


class ProductImageMap(Base):
    image = models.ForeignKey(ProductImage, on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.CASCADE, db_index=True, related_name="product_images")
