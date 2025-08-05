from .....attribute.models import Attribute, AttributeValue
from .....attribute.utils import associate_attribute_values_to_instance
from .....product import ProductTypeKind
from .....product.models import (
    Product,
    ProductChannelListing,
    ProductType,
)
from ....tests.utils import get_graphql_content

QUERY_PRODUCT_ATTRIBUTE_CHOICES = """
    query ($filter: ProductFilterInput, $channel: String!, $attributeSlugs: [String!]!) {
      productAttributeChoices(filter: $filter, channel: $channel, attributeSlugs: $attributeSlugs) {
        attribute {
          slug
        }
        choices {
          productCount
          value {
            name
          }
        }
      }
    }
    """


def test_product_attribute_choices(
    api_client,
    product,
    numeric_attribute,
    channel_USD,
):
    # given

    associate_attribute_values_to_instance(
        product,
        {numeric_attribute.id: list(numeric_attribute.values.all())},
    )

    attribute_slugs = [
        a.slug for a in product.product_type.product_attributes.all()
    ] + [a.slug for a in product.product_type.variant_attributes.all()]

    variables = {
        "channel": channel_USD.slug,
        "attributeSlugs": attribute_slugs,
    }

    # when
    response = api_client.post_graphql(QUERY_PRODUCT_ATTRIBUTE_CHOICES, variables)
    content = get_graphql_content(response)

    assert len(content["data"]["productAttributeChoices"]) == 2
    size = next(
        (
            choice
            for choice in content["data"]["productAttributeChoices"]
            if choice["attribute"]["slug"] == "size"
        ),
        None,
    )
    assert size is not None
    assert size["choices"][0]["productCount"] == 1
    assert size["choices"][0]["value"]["name"] == "Small"


def test_product_attribute_choices_without_products(
    api_client,
    product,
    category,
    numeric_attribute,
    channel_USD,
):
    # given

    associate_attribute_values_to_instance(
        product,
        {numeric_attribute.id: list(numeric_attribute.values.all())},
    )

    attribute_slugs = [
        a.slug for a in product.product_type.product_attributes.all()
    ] + [a.slug for a in product.product_type.variant_attributes.all()]

    variables = {
        "channel": channel_USD.slug,
        "attributeSlugs": attribute_slugs,
        "filter": {
            "search": "non-existing-product",
        },
    }

    # when
    response = api_client.post_graphql(QUERY_PRODUCT_ATTRIBUTE_CHOICES, variables)
    content = get_graphql_content(response)

    assert len(content["data"]["productAttributeChoices"]) == 2
    size = next(
        (
            choice
            for choice in content["data"]["productAttributeChoices"]
            if choice["attribute"]["slug"] == "size"
        ),
        None,
    )
    assert size is not None
    assert len(size["choices"]) == 0
