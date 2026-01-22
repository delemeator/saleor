from django.core.management.base import BaseCommand
from django.db import transaction

from saleor.attribute.models import (
    AssignedProductAttributeValue,
    AssignedVariantAttribute,
    AssignedVariantAttributeValue,
    AttributeProduct,
    AttributeVariant,
)

# Import your models based on the structure provided
from saleor.product.models import (
    Product,
    ProductType,
)


def change_product_type(product: Product, new_type: ProductType):
    if product.product_type == new_type:
        return

    with transaction.atomic():
        product.product_type = new_type
        product.save(update_fields=["product_type", "updated_at"])

        valid_product_attribute_ids = AttributeProduct.objects.filter(
            product_type=new_type
        ).values_list("attribute_id", flat=True)

        AssignedProductAttributeValue.objects.filter(product=product).exclude(
            value__attribute_id__in=valid_product_attribute_ids
        ).delete()

        # Map {attribute_id: new_attribute_variant_instance} for the NEW type
        new_attribute_variant_map = {
            av.attribute_id: av
            for av in AttributeVariant.objects.filter(product_type=new_type)
        }

        for variant in product.variants.all().prefetch_related(
            "attributes__values", "attributes__assignment"
        ):
            data_to_migrate = {}

            current_assignments = variant.attributes.all()
            for assignment in current_assignments:
                attr_id = assignment.assignment.attribute_id

                if attr_id in new_attribute_variant_map:
                    data_to_migrate[attr_id] = list(assignment.values.all())

            variant.attributes.all().delete()

            values_to_assign = []

            for attr_id, values in data_to_migrate.items():
                new_attr_variant = new_attribute_variant_map[attr_id]

                new_assignment = AssignedVariantAttribute.objects.create(
                    variant=variant, assignment=new_attr_variant
                )

                for val in values:
                    values_to_assign.append(
                        AssignedVariantAttributeValue(
                            value=val, assignment=new_assignment
                        )
                    )

            # Bulk create the connections between Assignment and Values
            if values_to_assign:
                AssignedVariantAttributeValue.objects.bulk_create(values_to_assign)


class Command(BaseCommand):
    help = "Change product type for a product"

    def add_arguments(self, parser):
        parser.add_argument(
            "product_slug", type=str, help="The unique slug of the product to update"
        )

        parser.add_argument(
            "product_type_slug",
            type=str,
            help="The unique slug of the new product type",
        )

    def handle(self, *args, **options):
        product_slug = options["product_slug"]
        product_type_slug = options["product_type_slug"]

        product = Product.objects.filter(slug=product_slug).first()

        if not product:
            self.stdout.write(
                self.style.ERROR(f"Product with slug '{product_slug}' does not exist.")
            )
            return

        product_type = ProductType.objects.filter(slug=product_type_slug).first()

        if not product_type:
            self.stdout.write(
                self.style.ERROR(
                    f"ProductType with slug '{product_type_slug}' does not exist."
                )
            )
            return

        change_product_type(product, product_type)
