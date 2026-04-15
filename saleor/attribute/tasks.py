from django.db import connection

from ..celeryconf import app
from ..core.db.connection import allow_writer
from .models import AttributeValueTranslation


@app.task
@allow_writer()
def generate_reference_attribute_value_translations():
    sql = """
        SELECT
            rows.attribute_value_id,
            rows.language_code,
            rows.translated_name,
            avt.id AS attribute_value_translation_id
        FROM (
            SELECT
                av.id AS attribute_value_id,
                pt.language_code AS language_code,
                pt.title AS translated_name
            FROM attribute_attributevalue av
            INNER JOIN page_pagetranslation pt
                ON pt.page_id = av.reference_page_id
            LEFT JOIN attribute_attributevaluetranslation avt
                ON avt.attribute_value_id = av.id
               AND avt.language_code = pt.language_code
            WHERE av.reference_page_id IS NOT NULL
              AND pt.title IS NOT NULL
              AND pt.title != ''
              AND (
                  avt.id IS NULL
                  OR avt.name IS DISTINCT FROM pt.title
              )

            UNION ALL

            SELECT
                av.id AS attribute_value_id,
                ct.language_code AS language_code,
                ct.name AS translated_name
            FROM attribute_attributevalue av
            INNER JOIN product_collectiontranslation ct
                ON ct.collection_id = av.reference_collection_id
            LEFT JOIN attribute_attributevaluetranslation avt
                ON avt.attribute_value_id = av.id
               AND avt.language_code = ct.language_code
            WHERE av.reference_collection_id IS NOT NULL
              AND ct.name IS NOT NULL
              AND ct.name != ''
              AND (
                  avt.id IS NULL
                  OR avt.name IS DISTINCT FROM ct.name
              )
        ) AS rows
        LEFT JOIN attribute_attributevaluetranslation avt
            ON avt.attribute_value_id = rows.attribute_value_id
           AND avt.language_code = rows.language_code
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    if not rows:
        return

    to_create = []
    to_update = []

    for (
        attribute_value_id,
        language_code,
        translated_name,
        attribute_value_translation_id,
    ) in rows:
        if attribute_value_translation_id is None:
            to_create.append(
                AttributeValueTranslation(
                    attribute_value_id=attribute_value_id,
                    language_code=language_code,
                    name=translated_name,
                )
            )
        else:
            to_update.append(
                AttributeValueTranslation(
                    id=attribute_value_translation_id,
                    attribute_value_id=attribute_value_id,
                    language_code=language_code,
                    name=translated_name,
                )
            )

    if to_create:
        AttributeValueTranslation.objects.bulk_create(
            to_create,
            batch_size=1000,
        )

    if to_update:
        AttributeValueTranslation.objects.bulk_update(
            to_update,
            fields=["name"],
            batch_size=1000,
        )
