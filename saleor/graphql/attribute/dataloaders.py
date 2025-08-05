from collections import defaultdict

from django.db.models import Q
from promise import Promise

from ...attribute.models import (
    AssignedProductAttributeValue,
    AssignedVariantAttributeValue,
    Attribute,
    AttributeValue,
)
from ..core.dataloaders import DataLoader


class AttributeValuesByAttributeIdLoader(DataLoader[int, list[AttributeValue]]):
    context_key = "attributevalues_by_attribute"

    def batch_load(self, keys):
        attribute_values = AttributeValue.objects.using(
            self.database_connection_name
        ).filter(attribute_id__in=keys)
        attribute_to_attributevalues = defaultdict(list)
        for attribute_value in attribute_values.iterator():
            attribute_to_attributevalues[attribute_value.attribute_id].append(
                attribute_value
            )
        return [attribute_to_attributevalues[attribute_id] for attribute_id in keys]


class AttributesByAttributeId(DataLoader[int, Attribute]):
    context_key = "attributes_by_id"

    def batch_load(self, keys):
        attributes = Attribute.objects.using(self.database_connection_name).in_bulk(
            keys
        )
        return [attributes.get(key) for key in keys]


class AttributeValueByIdLoader(DataLoader[int, AttributeValue]):
    context_key = "attributevalue_by_id"

    def batch_load(self, keys):
        attribute_values = AttributeValue.objects.using(
            self.database_connection_name
        ).in_bulk(keys)
        return [attribute_values.get(attribute_value_id) for attribute_value_id in keys]


class AttributeBySlugLoader(DataLoader):
    context_key = "attribute_by_slug"

    def batch_load_fn(self, slugs):
        attributes_qs = Attribute.objects.using(self.database_connection_name).filter(
            slug__in=slugs
        )
        attributes_by_slug = {attr.slug: attr for attr in attributes_qs}
        return Promise.resolve([attributes_by_slug.get(slug) for slug in slugs])


class AttributeChoicesByProductIdsLoader(
    DataLoader[tuple[tuple[int], tuple[str]], list]
):
    context_key = "attribute_choices_by_product_ids"

    def batch_load(self, keys):
        from .types import ProductAttributeChoices, ProductAttributeChoiceStats

        results = []

        for product_ids, slug in keys:
            if not product_ids:
                results.append(Promise.resolve([]))
                continue

            # Query product attribute values
            product_rows = (
                AssignedProductAttributeValue.objects.using(
                    self.database_connection_name
                )
                .filter(product_id__in=product_ids, value__attribute__slug=slug)
                .values("value_id", "product_id")
                .distinct()
            )

            # Query variant attribute values
            variant_rows = (
                AssignedVariantAttributeValue.objects.using(
                    self.database_connection_name
                )
                .filter(
                    assignment__variant__product_id__in=product_ids,
                    value__attribute__slug=slug,
                )
                .values("value_id", "assignment__variant__product_id")
                .distinct()
            )

            # Group by value_id
            value_to_products = defaultdict(set)
            for row in product_rows:
                value_to_products[row["value_id"]].add(row["product_id"])
            for row in variant_rows:
                value_to_products[row["value_id"]].add(
                    row["assignment__variant__product_id"]
                )

            value_ids = list(value_to_products.keys())

            def build_choices(attribute, value_map=value_to_products):
                def with_values(values):
                    value_dict = {v.id: v for v in values}
                    return ProductAttributeChoices(
                        attribute=attribute,
                        choices=[
                            ProductAttributeChoiceStats(
                                product_count=len(product_ids),
                                value=value_dict[value_id],
                            )
                            for value_id in value_map
                            if value_id in value_dict
                        ],
                    )

                return (
                    AttributeValueByIdLoader(self.context)
                    .load_many(value_ids)
                    .then(with_values)
                )

            promise = AttributeBySlugLoader(self.context).load(slug).then(build_choices)
            results.append(promise)

        return Promise.all(results)
