from collections import defaultdict

from django.db.models import Exists, OuterRef, Sum

from ...attribute.models import (
    AssignedProductAttributeValue,
    AssignedVariantAttributeValue,
)
from ...channel.models import Channel
from ...order import OrderStatus
from ...order.models import Order
from ...permission.utils import has_one_of_permissions
from ...product import models
from ...product.models import ALL_PRODUCTS_PERMISSIONS
from ..attribute.dataloaders import (
    AttributeBySlugLoader,
    AttributeValueByIdLoader,
)
from ..attribute.types import ProductAttributeChoices, ProductAttributeChoiceStats
from ..channel import ChannelQsContext
from ..channel.dataloaders import ChannelBySlugLoader
from ..core import ResolveInfo
from ..core.connection import (
    filter_qs,
    where_filter_qs,
)
from ..core.context import get_database_connection_name
from ..core.tracing import traced_resolver
from ..core.utils import from_global_id_or_error
from ..utils import get_user_or_app_from_context
from ..utils.filters import filter_by_period
from .filters import (
    ProductFilter,
    ProductWhere,
)


def resolve_categories(info: ResolveInfo, level=None):
    qs = models.Category.objects.using(
        get_database_connection_name(info.context)
    ).prefetch_related("children")
    if level is not None:
        qs = qs.filter(level=level)
    return qs


def resolve_category_by_translated_slug(info: ResolveInfo, slug, slug_language_code):
    return (
        models.Category.objects.using(get_database_connection_name(info.context))
        .filter(translations__language_code=slug_language_code, translations__slug=slug)
        .first()
    )


def resolve_collection_by_id(info: ResolveInfo, id, channel_slug, requestor):
    return (
        models.Collection.objects.using(get_database_connection_name(info.context))
        .visible_to_user(requestor, channel_slug=channel_slug)
        .filter(id=id)
        .first()
    )


def resolve_collection_by_slug(info: ResolveInfo, slug, channel_slug, requestor):
    return (
        models.Collection.objects.using(get_database_connection_name(info.context))
        .visible_to_user(requestor, channel_slug)
        .filter(slug=slug)
        .first()
    )


def resolve_collection_by_translated_slug(
    info: ResolveInfo, slug, channel_slug, slug_language_code, requestor
):
    return (
        models.Collection.objects.using(get_database_connection_name(info.context))
        .visible_to_user(requestor, channel_slug)
        .filter(translations__language_code=slug_language_code, translations__slug=slug)
        .first()
    )


def resolve_collections(info: ResolveInfo, channel_slug):
    requestor = get_user_or_app_from_context(info.context)
    qs = models.Collection.objects.using(
        get_database_connection_name(info.context)
    ).visible_to_user(requestor, channel_slug)

    return ChannelQsContext(qs=qs, channel_slug=channel_slug)


def resolve_digital_content_by_id(info, id):
    return (
        models.DigitalContent.objects.using(get_database_connection_name(info.context))
        .filter(pk=id)
        .first()
    )


def resolve_digital_contents(info: ResolveInfo):
    return models.DigitalContent.objects.using(
        get_database_connection_name(info.context)
    ).all()


def resolve_product(
    info: ResolveInfo,
    id,
    slug,
    slug_language_code,
    external_reference,
    channel: Channel | None,
    limited_channel_access: bool,
    requestor,
):
    database_connection_name = get_database_connection_name(info.context)
    qs = models.Product.objects.using(database_connection_name).visible_to_user(
        requestor, channel, limited_channel_access
    )
    if id:
        _type, id = from_global_id_or_error(id, "Product")
        return qs.filter(id=id).first()
    if slug:
        if slug_language_code:
            return qs.filter(
                translations__language_code=slug_language_code, translations__slug=slug
            ).first()

        return qs.filter(slug=slug).first()
    return qs.filter(external_reference=external_reference).first()


@traced_resolver
def resolve_products(
    info: ResolveInfo,
    requestor,
    channel: Channel | None,
    limited_channel_access: bool,
) -> ChannelQsContext:
    connection_name = get_database_connection_name(info.context)
    qs = models.Product.objects.using(connection_name).visible_to_user(
        requestor, channel, limited_channel_access
    )
    if not has_one_of_permissions(requestor, ALL_PRODUCTS_PERMISSIONS):
        if channel:
            product_channel_listings = (
                models.ProductChannelListing.objects.using(connection_name)
                .filter(channel_id=channel.id, visible_in_listings=True)
                .values("id")
            )
            qs = qs.filter(
                Exists(product_channel_listings.filter(product_id=OuterRef("pk")))
            )
        else:
            qs = models.Product.objects.none()
    channel_slug = channel.slug if channel else None
    return ChannelQsContext(qs=qs, channel_slug=channel_slug)


def resolve_product_type_by_id(info, id):
    return (
        models.ProductType.objects.using(get_database_connection_name(info.context))
        .filter(pk=id)
        .first()
    )


def resolve_product_types(info: ResolveInfo):
    return models.ProductType.objects.using(
        get_database_connection_name(info.context)
    ).all()


@traced_resolver
def resolve_variant(
    info: ResolveInfo,
    id,
    sku,
    external_reference,
    *,
    channel: Channel | None,
    limited_channel_access: bool,
    requestor,
    requestor_has_access_to_all,
):
    connection_name = get_database_connection_name(info.context)
    visible_products = (
        models.Product.objects.using(connection_name)
        .visible_to_user(requestor, channel, limited_channel_access)
        .values_list("pk", flat=True)
    )
    qs = models.ProductVariant.objects.using(connection_name).filter(
        product__id__in=visible_products
    )
    if not requestor_has_access_to_all:
        qs = qs.available_in_channel(channel)
    if id:
        _, id = from_global_id_or_error(id, "ProductVariant")
        return qs.filter(pk=id).first()
    if sku:
        return qs.filter(sku=sku).first()
    return qs.filter(external_reference=external_reference).first()


@traced_resolver
def resolve_product_variants(
    info: ResolveInfo,
    requestor,
    ids=None,
    channel: Channel | None = None,
    product_id: int | None = None,
    limited_channel_access: bool = False,
) -> ChannelQsContext:
    connection_name = get_database_connection_name(info.context)

    qs = models.ProductVariant.objects.using(connection_name).visible_to_user(
        requestor, channel, limited_channel_access
    )

    if ids:
        db_ids = [
            from_global_id_or_error(node_id, "ProductVariant")[1] for node_id in ids
        ]
        qs = qs.filter(pk__in=db_ids)

    if product_id:
        qs = qs.filter(product_id=product_id)

    channel_slug = channel.slug if channel else None
    return ChannelQsContext(qs=qs, channel_slug=channel_slug)


def resolve_report_product_sales(info, period, channel_slug) -> ChannelQsContext:
    connection_name = get_database_connection_name(info.context)
    qs = models.ProductVariant.objects.using(connection_name).all()

    # filter by period
    qs = filter_by_period(qs, period, "order_lines__order__created_at")

    # annotate quantity
    qs = qs.annotate(quantity_ordered=Sum("order_lines__quantity"))

    # filter by channel and order status
    channels = (
        Channel.objects.using(connection_name).filter(slug=channel_slug).values("pk")
    )
    exclude_status = [OrderStatus.DRAFT, OrderStatus.CANCELED, OrderStatus.EXPIRED]
    orders = (
        Order.objects.using(connection_name)
        .exclude(status__in=exclude_status)
        .filter(Exists(channels.filter(pk=OuterRef("channel_id")).values("pk")))
    )
    qs = qs.filter(
        Exists(orders.filter(pk=OuterRef("order_lines__order_id"))),
        quantity_ordered__isnull=False,
    )

    # order by quantity ordered
    qs = qs.order_by("-quantity_ordered")

    return ChannelQsContext(qs=qs, channel_slug=channel_slug)


def resolve_product_attribute_choices(
    info: ResolveInfo,
    channel,
    attribute_slugs,
    filter=None,
    where=None,
):
    channel_obj = ChannelBySlugLoader(info.context).load(channel).get()
    limited_channel_access = True
    requestor = None
    qs = resolve_products(info, requestor, channel_obj, limited_channel_access).qs

    if filter:
        qs = filter_qs(
            qs,
            {"channel": channel},
            filterset_class=ProductFilter,
            filter_input=filter,
            request=None,
            allow_replica=info.context.allow_replica,
        )
    if where:
        qs = where_filter_qs(
            qs,
            {"channel": channel},
            filterset_class=ProductWhere,
            filter_input=where,
            request=None,
            allow_replica=info.context.allow_replica,
        )
    product_ids = list(qs.values_list("id", flat=True))

    # Query product attribute values
    product_rows = (
        AssignedProductAttributeValue.objects.using(
            get_database_connection_name(info.context)
        )
        .filter(product_id__in=product_ids, value__attribute__slug__in=attribute_slugs)
        .values("value__attribute__slug", "value_id", "product_id")
        .distinct()
    )

    # Query variant attribute values
    variant_rows = (
        AssignedVariantAttributeValue.objects.using(
            get_database_connection_name(info.context)
        )
        .filter(
            assignment__variant__product_id__in=product_ids,
            value__attribute__slug__in=attribute_slugs,
        )
        .values("value__attribute__slug", "value_id", "assignment__variant__product_id")
        .distinct()
    )

    all_value_ids = {row["value_id"] for row in product_rows}.union(
        {row["value_id"] for row in variant_rows}
    )

    values = AttributeValueByIdLoader(info.context).batch_load(all_value_ids)
    attributes = AttributeBySlugLoader(info.context).batch_load(attribute_slugs)

    values_dict = {value.id: value for value in values if value}
    attributes_dict = {
        attribute.slug: attribute for attribute in attributes if attribute
    }

    # Group by value_id
    value_to_products = defaultdict(set)
    for row in product_rows:
        value_to_products[row["value_id"]].add(row["product_id"])
    for row in variant_rows:
        value_to_products[row["value_id"]].add(row["assignment__variant__product_id"])

    attribute_slug_to_values = defaultdict(set)
    for row in product_rows:
        if row["product_id"] in product_ids:
            attribute_slug_to_values[row["value__attribute__slug"]].add(row["value_id"])
    for row in variant_rows:
        if row["assignment__variant__product_id"] in product_ids:
            attribute_slug_to_values[row["value__attribute__slug"]].add(row["value_id"])

    output = []
    for slug in attribute_slugs:
        attribute = attributes_dict.get(slug)
        if not attribute:
            continue

        output.append(
            ProductAttributeChoices(
                attribute=attribute,
                choices=[
                    ProductAttributeChoiceStats(
                        product_count=len(value_to_products[value_id]),
                        value=values_dict[value_id],
                    )
                    for value_id in attribute_slug_to_values.get(slug, [])
                    if value_id in values_dict
                ],
            )
        )

    return output
