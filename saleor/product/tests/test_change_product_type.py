import pytest

from saleor.product.management.commands.change_product_type import change_product_type


@pytest.mark.parametrize(
    ("product", "product_type"),
    [
        ("product_with_product_attributes", "product_type_with_variant_attributes"),
        ("product_with_variant_attributes", "product_type_with_product_attributes"),
    ],
    indirect=True,
)
def test_product_type_change(
    product,
    product_type,
):
    # Change the product type
    change_product_type(product, product_type)

    # Refresh from DB
    product.refresh_from_db()

    # Verify the product type has been updated
    assert product.product_type == product_type
