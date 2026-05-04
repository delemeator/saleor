from unittest.mock import patch

import graphene

from .....attribute.models import (
    AssignedProductAttributeValue,
    AssignedVariantAttribute,
    Attribute,
    AttributeVariant,
)
from .....attribute.utils import associate_attribute_values_to_instance
from .....graphql.tests.utils import get_graphql_content
from .....product.error_codes import ProductErrorCode
from .....product.models import ProductType

MUTATION_PRODUCT_TYPE_CHANGE = """
mutation ProductTypeChange($productId: ID!, $productTypeSlug: String!) {
  productTypeChange(
    input: {
      productId: $productId
      productTypeSlug: $productTypeSlug
    }
  ) {
    product {
      id
      name
      slug
      productType {
        id
        name
        slug
      }
      attributes {
        attribute {
          id
          name
          slug
        }
        values {
          id
          name
          slug
        }
      }
      variants {
        id
        name
        attributes {
          attribute {
            id
            name
            slug
          }
          values {
            id
            name
            slug
          }
        }
      }
    }
    errors {
      field
      message
      code
    }
  }
}
"""


@patch("saleor.plugins.manager.PluginsManager.product_updated")
def test_product_type_change(
    updated_webhook_mock,
    staff_api_client,
    product,
    product_type,
    permission_manage_products,
):
    # given
    new_product_type = ProductType.objects.create(
        name="New product type",
        slug="new-product-type",
        has_variants=True,
    )

    product_id = graphene.Node.to_global_id("Product", product.pk)

    variables = {
        "productId": product_id,
        "productTypeSlug": new_product_type.slug,
    }

    # when
    response = staff_api_client.post_graphql(
        MUTATION_PRODUCT_TYPE_CHANGE,
        variables,
        permissions=[permission_manage_products],
    )

    # then
    content = get_graphql_content(response)
    data = content["data"]["productTypeChange"]

    assert data["errors"] == []

    product.refresh_from_db()

    assert product.product_type_id == new_product_type.pk
    assert data["product"]["id"] == product_id
    assert data["product"]["productType"]["slug"] == new_product_type.slug
    assert data["product"]["productType"]["name"] == new_product_type.name

    updated_webhook_mock.assert_called_once_with(product)


@patch("saleor.plugins.manager.PluginsManager.product_updated")
def test_product_type_change_keeps_matching_product_attributes_and_removes_invalid(
    updated_webhook_mock,
    staff_api_client,
    product,
    product_type,
    color_attribute,
    permission_manage_products,
):
    # given
    old_attribute = color_attribute

    invalid_attribute = Attribute.objects.create(
        name="Invalid old attribute",
        slug="invalid-old-attribute",
    )

    product_type.product_attributes.add(old_attribute, invalid_attribute)

    old_value = old_attribute.values.first()
    invalid_value = invalid_attribute.values.create(
        name="Invalid value",
        slug="invalid-value",
    )

    associate_attribute_values_to_instance(
        product,
        {
            old_attribute.pk: [old_value],
            invalid_attribute.pk: [invalid_value],
        },
    )

    new_product_type = ProductType.objects.create(
        name="New product type",
        slug="new-product-type",
        has_variants=True,
    )
    new_product_type.product_attributes.add(old_attribute)

    product_id = graphene.Node.to_global_id("Product", product.pk)

    variables = {
        "productId": product_id,
        "productTypeSlug": new_product_type.slug,
    }

    # when
    response = staff_api_client.post_graphql(
        MUTATION_PRODUCT_TYPE_CHANGE,
        variables,
        permissions=[permission_manage_products],
    )

    # then
    content = get_graphql_content(response)
    data = content["data"]["productTypeChange"]

    assert data["errors"] == []

    product.refresh_from_db()
    assert product.product_type_id == new_product_type.pk

    assert AssignedProductAttributeValue.objects.filter(
        product=product,
        value=old_value,
    ).exists()
    assert not AssignedProductAttributeValue.objects.filter(
        product=product,
        value=invalid_value,
    ).exists()

    attributes = data["product"]["attributes"]
    assert len(attributes) == 1
    assert attributes[0]["attribute"]["slug"] == old_attribute.slug
    assert attributes[0]["values"][0]["slug"] == old_value.slug

    updated_webhook_mock.assert_called_once_with(product)


@patch("saleor.plugins.manager.PluginsManager.product_updated")
def test_product_type_change_migrates_matching_variant_attributes(
    updated_webhook_mock,
    staff_api_client,
    product_with_variant_with_two_attributes,
    permission_manage_products,
):
    # given
    product = product_with_variant_with_two_attributes
    variant = product.variants.first()

    old_product_type = product.product_type

    old_variant_assignment = variant.attributes.first()
    attribute = old_variant_assignment.assignment.attribute
    values = list(old_variant_assignment.values.all())

    new_product_type = ProductType.objects.create(
        name="New product type",
        slug="new-product-type",
        has_variants=True,
    )

    AttributeVariant.objects.create(
        product_type=new_product_type,
        attribute=attribute,
        sort_order=0,
    )

    product_id = graphene.Node.to_global_id("Product", product.pk)

    variables = {
        "productId": product_id,
        "productTypeSlug": new_product_type.slug,
    }

    # sanity check
    assert AttributeVariant.objects.filter(
        product_type=old_product_type,
        attribute=attribute,
    ).exists()
    assert old_variant_assignment.values.count() == len(values)

    # when
    response = staff_api_client.post_graphql(
        MUTATION_PRODUCT_TYPE_CHANGE,
        variables,
        permissions=[permission_manage_products],
    )

    # then
    content = get_graphql_content(response)
    data = content["data"]["productTypeChange"]

    assert data["errors"] == []

    product.refresh_from_db()
    variant.refresh_from_db()

    assert product.product_type_id == new_product_type.pk

    new_variant_assignment = AssignedVariantAttribute.objects.get(
        variant=variant,
        assignment__product_type=new_product_type,
        assignment__attribute=attribute,
    )

    assert list(new_variant_assignment.values.all()) == values

    assert not AssignedVariantAttribute.objects.filter(
        variant=variant,
        assignment__product_type=old_product_type,
        assignment__attribute=attribute,
    ).exists()

    variant_attributes = data["product"]["variants"][0]["attributes"]
    assert any(
        attr_data["attribute"]["slug"] == attribute.slug
        for attr_data in variant_attributes
    )

    updated_webhook_mock.assert_called_once_with(product)


@patch("saleor.plugins.manager.PluginsManager.product_updated")
def test_product_type_change_removes_invalid_variant_attributes(
    updated_webhook_mock,
    staff_api_client,
    product_with_variant_with_two_attributes,
    permission_manage_products,
):
    # given
    product = product_with_variant_with_two_attributes
    variant = product.variants.first()

    old_assignments = list(variant.attributes.all())
    kept_assignment = old_assignments[0]
    removed_assignment = old_assignments[1]

    kept_attribute = kept_assignment.assignment.attribute
    removed_attribute = removed_assignment.assignment.attribute

    kept_values = list(kept_assignment.values.all())

    new_product_type = ProductType.objects.create(
        name="New product type",
        slug="new-product-type",
        has_variants=True,
    )

    AttributeVariant.objects.create(
        product_type=new_product_type,
        attribute=kept_attribute,
        sort_order=0,
    )

    product_id = graphene.Node.to_global_id("Product", product.pk)

    variables = {
        "productId": product_id,
        "productTypeSlug": new_product_type.slug,
    }

    # when
    response = staff_api_client.post_graphql(
        MUTATION_PRODUCT_TYPE_CHANGE,
        variables,
        permissions=[permission_manage_products],
    )

    # then
    content = get_graphql_content(response)
    data = content["data"]["productTypeChange"]

    assert data["errors"] == []

    product.refresh_from_db()
    variant.refresh_from_db()

    assert product.product_type_id == new_product_type.pk

    assert AssignedVariantAttribute.objects.filter(
        variant=variant,
        assignment__product_type=new_product_type,
        assignment__attribute=kept_attribute,
    ).exists()

    assert not AssignedVariantAttribute.objects.filter(
        variant=variant,
        assignment__attribute=removed_attribute,
    ).exists()

    new_assignment = AssignedVariantAttribute.objects.get(
        variant=variant,
        assignment__product_type=new_product_type,
        assignment__attribute=kept_attribute,
    )

    assert list(new_assignment.values.all()) == kept_values

    variant_attributes = data["product"]["variants"][0]["attributes"]
    attribute_slugs = {
        attr_data["attribute"]["slug"] for attr_data in variant_attributes
    }

    assert kept_attribute.slug in attribute_slugs
    assert removed_attribute.slug not in attribute_slugs

    updated_webhook_mock.assert_called_once_with(product)


@patch("saleor.plugins.manager.PluginsManager.product_updated")
def test_product_type_change_to_same_type_does_nothing(
    updated_webhook_mock,
    staff_api_client,
    product,
    permission_manage_products,
):
    # given
    product_type = product.product_type
    product_id = graphene.Node.to_global_id("Product", product.pk)

    variables = {
        "productId": product_id,
        "productTypeSlug": product_type.slug,
    }

    # when
    response = staff_api_client.post_graphql(
        MUTATION_PRODUCT_TYPE_CHANGE,
        variables,
        permissions=[permission_manage_products],
    )

    # then
    content = get_graphql_content(response)
    data = content["data"]["productTypeChange"]

    assert data["errors"] == []

    product.refresh_from_db()

    assert product.product_type_id == product_type.pk
    assert data["product"]["productType"]["slug"] == product_type.slug

    updated_webhook_mock.assert_called_once_with(product)


def test_product_type_change_product_type_slug_not_found(
    staff_api_client,
    product,
    permission_manage_products,
):
    # given
    product_id = graphene.Node.to_global_id("Product", product.pk)

    variables = {
        "productId": product_id,
        "productTypeSlug": "not-existing-product-type",
    }

    # when
    response = staff_api_client.post_graphql(
        MUTATION_PRODUCT_TYPE_CHANGE,
        variables,
        permissions=[permission_manage_products],
    )

    # then
    content = get_graphql_content(response)
    data = content["data"]["productTypeChange"]

    assert data["product"] is None
    assert len(data["errors"]) == 1
    assert data["errors"][0]["field"] == "productTypeSlug"
    assert data["errors"][0]["code"] == ProductErrorCode.NOT_FOUND.name


def test_product_type_change_product_id_not_found(
    staff_api_client,
    product_type,
    permission_manage_products,
):
    # given
    product_id = graphene.Node.to_global_id("Product", 0)

    variables = {
        "productId": product_id,
        "productTypeSlug": product_type.slug,
    }

    # when
    response = staff_api_client.post_graphql(
        MUTATION_PRODUCT_TYPE_CHANGE,
        variables,
        permissions=[permission_manage_products],
    )

    # then
    content = get_graphql_content(response)
    data = content["data"]["productTypeChange"]

    assert data["product"] is None
    assert len(data["errors"]) == 1
    assert data["errors"][0]["field"] == "productId"
    assert data["errors"][0]["code"] == ProductErrorCode.NOT_FOUND.name
