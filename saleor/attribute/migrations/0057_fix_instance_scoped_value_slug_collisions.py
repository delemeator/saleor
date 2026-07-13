from django.apps import apps as registry
from django.db import migrations
from django.db.models.signals import post_migrate

from .tasks.saleor3_22 import fix_instance_scoped_attribute_value_slugs_task


def fix_instance_scoped_value_slugs(apps, _schema_editor):
    def on_migrations_complete(sender=None, **kwargs):
        fix_instance_scoped_attribute_value_slugs_task.delay()

    sender = registry.get_app_config("attribute")
    post_migrate.connect(on_migrations_complete, weak=False, sender=sender)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("attribute", "0056_alter_attribute_unit"),
    ]

    operations = [
        migrations.RunPython(
            fix_instance_scoped_value_slugs,
            reverse_code=migrations.RunPython.noop,
        )
    ]
