"""Reproduce cross-entity attribute value corruption caused by slug collision.

``AttributeTypeHandler._update_or_create_value``
(saleor/graphql/attribute/utils/type_handlers.py) generates the value slug as
``f"{instance.id}_{attribute.id}"`` where ``instance`` is a ``Product``,
``ProductVariant`` or ``Page``. The slug is unique only per instance *id*, not
per instance *type*. Products, variants and pages use independent id
sequences, so a product and a variant sharing the same numeric pk is common.

When the same attribute is used as a product attribute on one product type
and as a variant attribute on another, a product and a variant with equal pks
resolve to the same ``AttributeValue`` row, and ``update_or_create`` by
``(attribute, slug)``:

* links the existing value of one entity to the other entity, and
* overwrites the value's name/numeric with whatever the other entity sets.

The result: updating a numeric (or plain text / rich text / date / datetime)
attribute on a variant silently changes the displayed attribute value of an
unrelated product with the same id, and vice versa.
"""

import graphene
import pytest

from saleor.attribute.models import AttributeValue
from saleor.graphql.tests.utils import get_graphql_content
from saleor.product import ProductTypeKind
from saleor.product.models import Product, ProductType, ProductVariant

PRODUCT_UPDATE_MUTATION = """
    mutation ProductUpdate($id: ID!, $input: ProductInput!) {
        productUpdate(id: $id, input: $input) {
            errors {
                field
                message
            }
            product {
                id
            }
        }
    }
"""

VARIANT_UPDATE_MUTATION = """
    mutation VariantUpdate($id: ID!, $attributes: [AttributeValueInput!]) {
        productVariantUpdate(id: $id, input: {attributes: $attributes}) {
            errors {
                field
                message
            }
            productVariant {
                id
            }
        }
    }
"""


@pytest.fixture
def unrelated_variant_with_colliding_id(product, numeric_attribute):
    """Return a variant of another product whose pk equals ``product.pk``.

    The variant belongs to a separate product of a separate product type that
    uses the numeric attribute as a *variant* attribute. Product and variant
    ids come from independent database sequences, so pk collisions between
    them naturally occur in real data.
    """
    variant_product_type = ProductType.objects.create(
        name="Type with variant numeric attribute",
        slug="type-with-variant-numeric-attribute",
        kind=ProductTypeKind.NORMAL,
        has_variants=True,
    )
    variant_product_type.variant_attributes.add(numeric_attribute)
    unrelated_product = Product.objects.create(
        name="Unrelated product",
        slug="unrelated-product",
        product_type=variant_product_type,
    )
    # Force the pk collision. update_or_create handles the case when fixture
    # data already created a variant under this pk.
    variant, _ = ProductVariant.objects.update_or_create(
        pk=product.pk,
        defaults={"product": unrelated_product, "sku": "sku-with-colliding-id"},
    )
    return variant


def set_numeric_attribute_on_product(staff_api_client, product, attribute, value_name):
    variables = {
        "id": graphene.Node.to_global_id("Product", product.pk),
        "input": {
            "attributes": [
                {
                    "id": graphene.Node.to_global_id("Attribute", attribute.pk),
                    "numeric": value_name,
                }
            ]
        },
    }
    response = staff_api_client.post_graphql(PRODUCT_UPDATE_MUTATION, variables)
    content = get_graphql_content(response)["data"]["productUpdate"]
    assert content["errors"] == []


def set_numeric_attribute_on_variant(staff_api_client, variant, attribute, value_name):
    variables = {
        "id": graphene.Node.to_global_id("ProductVariant", variant.pk),
        "attributes": [
            {
                "id": graphene.Node.to_global_id("Attribute", attribute.pk),
                "numeric": value_name,
            }
        ],
    }
    response = staff_api_client.post_graphql(VARIANT_UPDATE_MUTATION, variables)
    content = get_graphql_content(response)["data"]["productVariantUpdate"]
    assert content["errors"] == []


def test_variant_update_does_not_modify_numeric_value_of_product_with_same_id(
    staff_api_client,
    permission_manage_products,
    product,
    product_type,
    numeric_attribute,
    unrelated_variant_with_colliding_id,
):
    # given the numeric attribute used as a product attribute
    product_type.product_attributes.add(numeric_attribute)
    staff_api_client.user.user_permissions.add(permission_manage_products)

    # and the product having a numeric value set via the API
    product_value_name = "221"
    set_numeric_attribute_on_product(
        staff_api_client, product, numeric_attribute, product_value_name
    )
    product_value = AttributeValue.objects.get(
        attribute=numeric_attribute, productvalueassignment__product=product
    )
    assert product_value.name == product_value_name

    # when the same numeric attribute is set on an unrelated variant
    # with the same id
    variant_value_name = "999"
    set_numeric_attribute_on_variant(
        staff_api_client,
        unrelated_variant_with_colliding_id,
        numeric_attribute,
        variant_value_name,
    )

    # then the product's attribute value is untouched
    product_value.refresh_from_db()
    assert product_value.name == product_value_name

    # and the variant got its own value instead of reusing the product's one
    variant_value = AttributeValue.objects.get(
        attribute=numeric_attribute,
        variantvalueassignment__variant=unrelated_variant_with_colliding_id,
    )
    assert variant_value.pk != product_value.pk
    assert variant_value.name == variant_value_name

    # and the product is still linked to its own value only
    product_value_ids = list(
        product.attributevalues.filter(value__attribute=numeric_attribute).values_list(
            "value_id", flat=True
        )
    )
    assert product_value_ids == [product_value.pk]


def test_product_update_does_not_modify_numeric_value_of_variant_with_same_id(
    staff_api_client,
    permission_manage_products,
    product,
    product_type,
    numeric_attribute,
    unrelated_variant_with_colliding_id,
):
    # given the numeric attribute used as a product attribute
    product_type.product_attributes.add(numeric_attribute)
    staff_api_client.user.user_permissions.add(permission_manage_products)

    # and an unrelated variant with the same id having a numeric value
    # set via the API
    variant_value_name = "150"
    set_numeric_attribute_on_variant(
        staff_api_client,
        unrelated_variant_with_colliding_id,
        numeric_attribute,
        variant_value_name,
    )
    variant_value = AttributeValue.objects.get(
        attribute=numeric_attribute,
        variantvalueassignment__variant=unrelated_variant_with_colliding_id,
    )
    assert variant_value.name == variant_value_name

    # when the same numeric attribute is set on the product with the same id
    product_value_name = "221"
    set_numeric_attribute_on_product(
        staff_api_client, product, numeric_attribute, product_value_name
    )

    # then the variant's attribute value is untouched
    variant_value.refresh_from_db()
    assert variant_value.name == variant_value_name

    # and the product got its own value instead of reusing the variant's one
    product_value = AttributeValue.objects.get(
        attribute=numeric_attribute, productvalueassignment__product=product
    )
    assert product_value.pk != variant_value.pk
    assert product_value.name == product_value_name

    # and the variant is still linked to its own value only
    variant_value_ids = list(
        unrelated_variant_with_colliding_id.attributevalues.filter(
            value__attribute=numeric_attribute
        ).values_list("value_id", flat=True)
    )
    assert variant_value_ids == [variant_value.pk]
