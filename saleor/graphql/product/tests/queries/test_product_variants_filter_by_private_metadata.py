import graphene
import pytest

from .....permission.enums import ProductPermissions
from ....tests.utils import get_graphql_content, get_graphql_content_from_response

PRODUCT_VARIANTS_FILTER_QUERY = """
    query($filter: ProductVariantFilterInput, $channel: String) {
      productVariants(first: 10, filter: $filter, channel: $channel) {
        edges {
          node {
            id
          }
        }
      }
    }
"""

PRODUCT_VARIANTS_WHERE_QUERY = """
    query($where: ProductVariantWhereInput, $channel: String) {
      productVariants(first: 10, where: $where, channel: $channel) {
        edges {
          node {
            id
          }
        }
      }
    }
"""

PRIVATE_METADATA_KEY = "erp-id"
PRIVATE_METADATA_VALUE = "ERP-1"


@pytest.fixture
def variants_with_private_metadata(product_variant_list):
    matching_variant, other_value_variant, *_ = product_variant_list
    matching_variant.store_value_in_private_metadata(
        {PRIVATE_METADATA_KEY: PRIVATE_METADATA_VALUE}
    )
    other_value_variant.store_value_in_private_metadata({PRIVATE_METADATA_KEY: "ERP-2"})
    for variant in (matching_variant, other_value_variant):
        variant.save(update_fields=["private_metadata"])
    return matching_variant, other_value_variant


def _variant_global_id(variant):
    return graphene.Node.to_global_id("ProductVariant", variant.pk)


def _returned_ids(response):
    content = get_graphql_content(response)
    return {edge["node"]["id"] for edge in content["data"]["productVariants"]["edges"]}


@pytest.mark.parametrize(
    ("query", "variables_key", "filter_value"),
    [
        (
            PRODUCT_VARIANTS_FILTER_QUERY,
            "filter",
            {
                "privateMetadata": [
                    {"key": PRIVATE_METADATA_KEY, "value": PRIVATE_METADATA_VALUE}
                ]
            },
        ),
        (
            PRODUCT_VARIANTS_WHERE_QUERY,
            "where",
            {
                "privateMetadata": [
                    {"key": PRIVATE_METADATA_KEY, "value": PRIVATE_METADATA_VALUE}
                ]
            },
        ),
        (
            PRODUCT_VARIANTS_WHERE_QUERY,
            "where",
            {
                "AND": [
                    {
                        "privateMetadata": [
                            {
                                "key": PRIVATE_METADATA_KEY,
                                "value": PRIVATE_METADATA_VALUE,
                            }
                        ]
                    }
                ]
            },
        ),
    ],
)
def test_product_variants_filter_by_private_metadata_key_and_value(
    query,
    variables_key,
    filter_value,
    staff_api_client,
    permission_manage_products,
    variants_with_private_metadata,
    channel_USD,
):
    # given
    matching_variant, _ = variants_with_private_metadata
    staff_api_client.user.user_permissions.add(permission_manage_products)
    variables = {"channel": channel_USD.slug, variables_key: filter_value}

    # when
    response = staff_api_client.post_graphql(query, variables)

    # then
    assert _returned_ids(response) == {_variant_global_id(matching_variant)}


@pytest.mark.parametrize(
    ("query", "variables_key"),
    [
        (PRODUCT_VARIANTS_FILTER_QUERY, "filter"),
        (PRODUCT_VARIANTS_WHERE_QUERY, "where"),
    ],
)
def test_product_variants_filter_by_private_metadata_key_only(
    query,
    variables_key,
    staff_api_client,
    permission_manage_products,
    variants_with_private_metadata,
    channel_USD,
):
    # given
    staff_api_client.user.user_permissions.add(permission_manage_products)
    variables = {
        "channel": channel_USD.slug,
        variables_key: {"privateMetadata": [{"key": PRIVATE_METADATA_KEY}]},
    }

    # when
    response = staff_api_client.post_graphql(query, variables)

    # then
    assert _returned_ids(response) == {
        _variant_global_id(variant) for variant in variants_with_private_metadata
    }


@pytest.mark.parametrize(
    ("query", "variables_key"),
    [
        (PRODUCT_VARIANTS_FILTER_QUERY, "filter"),
        (PRODUCT_VARIANTS_WHERE_QUERY, "where"),
    ],
)
def test_product_variants_filter_by_private_metadata_as_app(
    query,
    variables_key,
    app_api_client,
    permission_manage_products,
    variants_with_private_metadata,
    channel_USD,
):
    # given
    matching_variant, _ = variants_with_private_metadata
    app_api_client.app.permissions.add(permission_manage_products)
    variables = {
        "channel": channel_USD.slug,
        variables_key: {
            "privateMetadata": [
                {"key": PRIVATE_METADATA_KEY, "value": PRIVATE_METADATA_VALUE}
            ]
        },
    }

    # when
    response = app_api_client.post_graphql(query, variables)

    # then
    assert _returned_ids(response) == {_variant_global_id(matching_variant)}


@pytest.mark.parametrize(
    ("query", "variables_key"),
    [
        (PRODUCT_VARIANTS_FILTER_QUERY, "filter"),
        (PRODUCT_VARIANTS_WHERE_QUERY, "where"),
    ],
)
@pytest.mark.parametrize(
    "client_fixture", ["api_client", "staff_api_client", "app_api_client"]
)
def test_product_variants_filter_by_private_metadata_without_permission(
    query,
    variables_key,
    client_fixture,
    request,
    variants_with_private_metadata,
    channel_USD,
):
    # given
    client = request.getfixturevalue(client_fixture)
    variables = {
        "channel": channel_USD.slug,
        variables_key: {
            "privateMetadata": [
                {"key": PRIVATE_METADATA_KEY, "value": PRIVATE_METADATA_VALUE}
            ]
        },
    }

    # when
    response = client.post_graphql(query, variables)

    # then
    content = get_graphql_content_from_response(response)
    assert content["data"]["productVariants"] is None
    assert len(content["errors"]) == 1
    error = content["errors"][0]
    assert error["extensions"]["exception"]["code"] == "PermissionDenied"
    assert error["message"] == (
        "To access this path, you need one of the following permissions: "
        f"{ProductPermissions.MANAGE_PRODUCTS.name}"
    )
