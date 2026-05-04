import graphene
from django.core.exceptions import ValidationError
from django.db import transaction

from saleor.attribute.models import (
    AssignedProductAttributeValue,
    AssignedVariantAttribute,
    AssignedVariantAttributeValue,
    AttributeProduct,
    AttributeVariant,
)
from saleor.product.models import (
    Product,
    ProductType,
)

from .....discount.utils.promotion import mark_active_catalogue_promotion_rules_as_dirty
from .....permission.enums import ProductPermissions
from ....core import ResolveInfo
from ....core.context import ChannelContext
from ....core.mutations import BaseMutation
from ....core.types.common import ProductError
from ....plugins.dataloaders import get_plugin_manager_promise
from ...types import Product as ProductTypeGraphQL


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


class ProductTypeChangeInput(graphene.InputObjectType):
    product_id = graphene.ID(
        required=True,
        description="ID of the product to update.",
    )
    product_type_slug = graphene.String(
        required=True,
        description="Slug of the new product type.",
    )


class ProductTypeChange(BaseMutation):
    product = graphene.Field(
        ProductTypeGraphQL,
        description="The updated product.",
    )

    class Arguments:
        input = ProductTypeChangeInput(
            required=True,
            description="Fields required to change product type.",
        )

    class Meta:
        description = "Changes the product type of an existing product."
        permissions = (ProductPermissions.MANAGE_PRODUCTS,)
        error_type_class = ProductError
        error_type_field = "product_errors"

    @classmethod
    def clean_input(cls, info: ResolveInfo, data):
        product_id = data.get("product_id")
        product_type_slug = data.get("product_type_slug")

        product = cls.get_node_or_error(
            info,
            product_id,
            only_type="Product",
            field="product_id",
        )

        product_type = ProductType.objects.filter(slug=product_type_slug).first()

        if not product_type:
            raise ValidationError(
                {
                    "product_type_slug": ValidationError(
                        f"ProductType with slug '{product_type_slug}' does not exist.",
                        code="not_found",
                    )
                }
            )

        return product, product_type

    @classmethod
    def _post_save_action(cls, info: ResolveInfo, product: Product):
        product = Product.objects.get(pk=product.pk)

        manager = get_plugin_manager_promise(info.context).get()
        cls.call_event(manager.product_updated, product)

    @classmethod
    def perform_mutation(cls, _root, info: ResolveInfo, /, **data):
        input_data = data["input"]

        product, product_type = cls.clean_input(info, input_data)

        change_product_type(product, product_type)

        cls._post_save_action(info, product)

        product = Product.objects.get(pk=product.pk)

        return ProductTypeChange(
            product=ChannelContext(node=product, channel_slug=None)
        )
