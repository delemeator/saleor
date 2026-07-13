from collections import Counter

from django.core.management.base import BaseCommand

from ...migrations.tasks.saleor3_22 import process_instance_scoped_slug_batch


class Command(BaseCommand):
    help = (
        "Rewrite auto-generated attribute value slugs from the "
        "'{instance_id}_{attribute_id}' format to the type-scoped "
        "'{model_name}-{instance_id}_{attribute_id}' format and split value "
        "rows shared between colliding products/variants/pages. Runs "
        "synchronously, without celery, and is safe to re-run."
    )

    def handle(self, *args, **options):
        totals = Counter()
        last_pk, stats = process_instance_scoped_slug_batch()
        totals.update(stats)
        while last_pk is not None:
            self.stdout.write(f"processed up to value pk {last_pk}: {stats}")
            last_pk, stats = process_instance_scoped_slug_batch(last_pk)
            totals.update(stats)
        self.stdout.write(self.style.SUCCESS(f"done: {dict(totals)}"))
