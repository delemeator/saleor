from django.conf import settings
from django.db import connection, transaction
from django.db.models import F, FloatField
from django.db.models.functions import Cast

from ....celeryconf import app
from ....core.db.connection import allow_writer
from ...models import (
    AssignedPageAttributeValue,
    AssignedProductAttributeValue,
    AssignedVariantAttributeValue,
)
from ...models.base import AttributeValue

# Takes around 0.11 seconds to process the batch.
# The memory usage is marginal (~1MB).
BATCH_SIZE = 500

# Input types whose values are scoped to a single product/variant/page and
# get an auto-generated slug.
INSTANCE_SCOPED_INPUT_TYPES = [
    "numeric",
    "plain-text",
    "rich-text",
    "date",
    "date-time",
]

# The old auto-generated slug format: "{instance_id}_{attribute_id}".
INSTANCE_SCOPED_SLUG_PATTERN = r"^\d+_\d+$"


@app.task(queue=settings.DATA_MIGRATIONS_TASKS_QUEUE_NAME)
@allow_writer()
def fulfill_attribute_value_numeric_field(attribute_value_pk=0):
    value_ids = list(
        AttributeValue.objects.filter(
            pk__gte=attribute_value_pk,
            numeric__isnull=True,
            attribute__input_type="numeric",
        )
        .order_by("pk")
        .values_list("id", flat=True)[:BATCH_SIZE]
    )

    if not value_ids:
        return

    with transaction.atomic():
        locked_values = (
            AttributeValue.objects.filter(id__in=value_ids)
            .order_by("sort_order", "pk")
            .select_for_update()
            .values_list("id", flat=True)
        )
        AttributeValue.objects.filter(id__in=locked_values).update(
            numeric=Cast(F("name"), FloatField())
        )
    fulfill_attribute_value_numeric_field.delay(value_ids[-1])


def update_product_variant_assignment():
    """Assign variant_id to a new field on assignedproductattributevalue.

    Take the values from attribute_assignedvariantattribute to variant_id and copy it over
    to attribute_assignedvariantattributevalue variant_id.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE attribute_assignedvariantattributevalue
            SET variant_id = (
                SELECT variant_id
                FROM attribute_assignedvariantattribute
                WHERE attribute_assignedvariantattributevalue.assignment_id = attribute_assignedvariantattribute.id
            )
            WHERE id IN (
                SELECT ID FROM attribute_assignedvariantattributevalue
                WHERE VARIANT_ID IS NULL
                ORDER BY SORT_ORDER, ID DESC
                FOR UPDATE
                LIMIT %s
            );
            """,
            [BATCH_SIZE],
        )


def _generate_scoped_slug(attribute_id, model_name, entity_id):
    base_slug = f"{model_name}-{entity_id}_{attribute_id}"
    slug = base_slug
    suffix = 2
    while AttributeValue.objects.filter(attribute_id=attribute_id, slug=slug).exists():
        slug = f"{base_slug}-{suffix}"
        suffix += 1
    return slug


def _get_assignment_links(value_id):
    """Return (model_name, assignment_row, entity_id) for every entity linked."""
    links = [
        ("product", link, link.product_id)
        for link in AssignedProductAttributeValue.objects.filter(value_id=value_id)
    ]
    links += [
        ("productvariant", link, link.variant_id)
        for link in AssignedVariantAttributeValue.objects.filter(value_id=value_id)
    ]
    links += [
        ("page", link, link.page_id)
        for link in AssignedPageAttributeValue.objects.filter(value_id=value_id)
    ]
    return links


def _split_value_between_entities(value, links):
    # Keep the row for the entity the slug was generated for, when it is
    # still among the linked ones.
    slug_owner_id = int(value.slug.split("_")[0])
    links.sort(key=lambda link: link[2] != slug_owner_id)

    keeper_model_name, _, keeper_entity_id = links[0]
    value.slug = _generate_scoped_slug(
        value.attribute_id, keeper_model_name, keeper_entity_id
    )
    value.save(update_fields=["slug"])

    for model_name, assignment, entity_id in links[1:]:
        duplicate = AttributeValue.objects.create(
            attribute_id=value.attribute_id,
            slug=_generate_scoped_slug(value.attribute_id, model_name, entity_id),
            name=value.name,
            plain_text=value.plain_text,
            rich_text=value.rich_text,
            numeric=value.numeric,
            date_time=value.date_time,
        )
        assignment.value = duplicate
        assignment.save(update_fields=["value"])


@allow_writer()
def process_instance_scoped_slug_batch(start_pk=0):
    """Process one batch of old-format instance-scoped value slugs.

    Returns a tuple of (last processed pk or None when there is nothing
    left, per-batch stats dict).
    """
    stats = {"renamed": 0, "split_copies": 0, "skipped_orphans": 0, "skipped_other": 0}
    value_ids = list(
        AttributeValue.objects.filter(
            pk__gt=start_pk,
            slug__regex=INSTANCE_SCOPED_SLUG_PATTERN,
            attribute__input_type__in=INSTANCE_SCOPED_INPUT_TYPES,
        )
        .order_by("pk")
        .values_list("id", flat=True)[:BATCH_SIZE]
    )

    if not value_ids:
        return None, stats

    with transaction.atomic():
        values = (
            AttributeValue.objects.filter(id__in=value_ids)
            .order_by("pk")
            .select_for_update(of=["self"])
        )
        for value in values:
            if value.slug.split("_")[-1] != str(value.attribute_id):
                # not an auto-generated instance-scoped slug
                stats["skipped_other"] += 1
                continue
            links = _get_assignment_links(value.pk)
            if not links:
                stats["skipped_orphans"] += 1
                continue
            _split_value_between_entities(value, links)
            stats["renamed"] += 1
            stats["split_copies"] += len(links) - 1

    return value_ids[-1], stats


@app.task(queue=settings.DATA_MIGRATIONS_TASKS_QUEUE_NAME)
@allow_writer()
def fix_instance_scoped_attribute_value_slugs_task(start_pk=0):
    """Migrate auto-generated value slugs to the type-scoped format.

    The old "{instance_id}_{attribute_id}" slug format is not unique across
    products, variants, and pages, so entities of different types with equal
    pks ended up sharing (and overwriting) one value row. Rewrite slugs to
    "{model_name}-{instance_id}_{attribute_id}" and give every additionally
    linked entity its own copy of the shared value.
    """
    last_pk, _stats = process_instance_scoped_slug_batch(start_pk)
    if last_pk is not None:
        fix_instance_scoped_attribute_value_slugs_task.delay(last_pk)


@app.task(queue=settings.DATA_MIGRATIONS_TASKS_QUEUE_NAME)
@allow_writer()
def assign_product_variants_to_attribute_values_task():
    # Order events proceed from the newest to the oldest
    database_connection_name = settings.DATABASE_CONNECTION_REPLICA_NAME
    assigned_values = (
        AssignedVariantAttributeValue.objects.filter(variant__isnull=True)
        .using(database_connection_name)
        .values_list("pk", flat=True)
        .exists()
    )
    # If we found data, queue next execution of the task
    if assigned_values:
        update_product_variant_assignment()
        assign_product_variants_to_attribute_values_task.delay()
