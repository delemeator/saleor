from __future__ import annotations

import os
from io import BytesIO

from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand
from django.db import transaction
from PIL import Image

from saleor.product.models import ProductMedia


class Command(BaseCommand):
    help = "Convert all ProductMedia AVIF images to JPG and update ProductMedia.image."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be converted without saving changes.",
        )
        parser.add_argument(
            "--delete-original",
            action="store_true",
            help="Delete the original AVIF file after successful conversion.",
        )
        parser.add_argument(
            "--quality",
            type=int,
            default=100,
            help="JPEG quality (default: 100).",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        delete_original = options["delete_original"]
        quality = options["quality"]

        queryset = ProductMedia.objects.exclude(image="").exclude(image__isnull=True)

        converted = 0
        skipped = 0
        failed = 0

        for media in queryset.iterator():
            image_field = media.image
            if not image_field:
                skipped += 1
                continue

            name = image_field.name or ""
            lower_name = name.lower()

            if not lower_name.endswith(".avif"):
                skipped += 1
                continue

            self.stdout.write(f"Processing ProductMedia id={media.pk}: {name}")

            try:
                image_field.open("rb")
                with Image.open(image_field) as img:
                    # Convert to RGB because JPEG does not support alpha/transparency.
                    # If source has alpha, paste onto white background.
                    if img.mode in ("RGBA", "LA") or (
                        img.mode == "P" and "transparency" in img.info
                    ):
                        background = Image.new("RGB", img.size, (255, 255, 255))
                        alpha = img.convert("RGBA")
                        background.paste(alpha, mask=alpha.split()[-1])
                        converted_img = background
                    else:
                        converted_img = img.convert("RGB")

                    output = BytesIO()
                    converted_img.save(
                        output,
                        format="JPEG",
                        quality=quality,
                        optimize=True,
                    )
                    output.seek(0)

                new_name = self._build_jpg_name(name)

                if dry_run:
                    self.stdout.write(
                        self.style.WARNING(
                            f"[DRY RUN] Would replace {name} -> {new_name}"
                        )
                    )
                    converted += 1
                    continue

                old_name = image_field.name

                with transaction.atomic():
                    media.image.save(
                        new_name,
                        ContentFile(output.read()),
                        save=False,
                    )
                    media.save(update_fields=["image"])

                if delete_original and old_name != media.image.name:
                    storage = image_field.storage
                    if storage.exists(old_name):
                        storage.delete(old_name)

                converted += 1
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Converted ProductMedia id={media.pk}: {old_name} -> {media.image.name}"
                    )
                )

            except Exception as exc:
                failed += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed ProductMedia id={media.pk} ({name}): {exc}"
                    )
                )

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Converted: {converted}"))
        self.stdout.write(self.style.WARNING(f"Skipped:   {skipped}"))
        self.stdout.write(self.style.ERROR(f"Failed:    {failed}"))

    def _build_jpg_name(self, original_name: str) -> str:
        base, _ext = os.path.splitext(original_name)
        return f"{base}.jpg"
