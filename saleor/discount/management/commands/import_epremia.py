import csv
import uuid
from decimal import Decimal, InvalidOperation

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from saleor.giftcard.models import GiftCard, GiftCardTag


class Command(BaseCommand):
    help = (
        "Import epremia gift cards from CSV with columns: email,balance. "
        "Creates or updates exactly one epremia gift card per user."
    )

    ROLE = "epremia"
    TAG_NAME = "epremia"

    def add_arguments(self, parser):
        parser.add_argument(
            "csv_file",
            type=str,
            help="Path to CSV file with columns: email,balance",
        )
        parser.add_argument(
            "--currency",
            type=str,
            default="PLN",
            help="Currency for created gift cards (default: PLN)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and show what would change without saving",
        )

    def normalize_email(self, email: str) -> str:
        return (email or "").strip().lower()

    def parse_balance(self, raw_value: str) -> Decimal:
        value = (raw_value or "").strip().replace(",", ".")
        if not value:
            raise CommandError("Balance cannot be empty.")
        try:
            amount = Decimal(value) / 100
        except InvalidOperation as exc:
            raise CommandError(f"Invalid balance value: {raw_value}") from exc

        if amount < 0:
            raise CommandError(f"Balance cannot be negative: {raw_value}")

        return amount

    def generate_code(self) -> str:
        # max_length=32 on GiftCard.code
        # "EPREMIA-" = 8 chars, + 24 hex chars = 32
        return f"EPREMIA-{uuid.uuid4().hex[:24].upper()}"

    def set_role_metadata(self, gift_card: GiftCard):
        updated = False

        if hasattr(gift_card, "metadata"):
            metadata = dict(gift_card.metadata or {})
            metadata["role"] = self.ROLE
            gift_card.metadata = metadata
            updated = True

        if not updated:
            raise CommandError(
                "GiftCard metadata field not found. "
                "Please adjust set_role_metadata() to your ModelWithMetadata implementation."
            )

    def has_epremia_role_metadata(self, gift_card: GiftCard) -> bool:
        metadata = getattr(gift_card, "metadata", None) or {}
        private_metadata = getattr(gift_card, "private_metadata", None) or {}

        return (
            metadata.get("role") == self.ROLE
            or private_metadata.get("role") == self.ROLE
        )

    def get_existing_epremia_gift_cards_for_user(self, user, email: str):
        candidates = (
            GiftCard.objects.filter(tags__name=self.TAG_NAME)
            .filter(used_by=user)
            .distinct()
        )

        result = [gc for gc in candidates if self.has_epremia_role_metadata(gc)]

        if result:
            return result

        # Fallback for older data where used_by might be null but used_by_email is set
        candidates = GiftCard.objects.filter(
            tags__name=self.TAG_NAME, used_by_email=email
        ).distinct()
        return [gc for gc in candidates if self.has_epremia_role_metadata(gc)]

    @transaction.atomic
    def create_or_update_gift_card(
        self, *, user, email: str, balance: Decimal, currency: str, dry_run: bool
    ):
        tag, _ = GiftCardTag.objects.get_or_create(name=self.TAG_NAME)

        existing_cards = self.get_existing_epremia_gift_cards_for_user(user, email)

        primary = existing_cards[0] if existing_cards else None
        duplicates = existing_cards[1:] if len(existing_cards) > 1 else []

        if primary:
            changed_fields = []

            if primary.currency != currency:
                primary.currency = currency
                changed_fields.append("currency")

            if primary.initial_balance_amount != balance:
                primary.initial_balance_amount = balance
                changed_fields.append("initial_balance_amount")

            if primary.current_balance_amount != balance:
                primary.current_balance_amount = balance
                changed_fields.append("current_balance_amount")

            if primary.used_by_id != user.id:
                primary.used_by = user
                changed_fields.append("used_by")

            if primary.used_by_email != email:
                primary.used_by_email = email
                changed_fields.append("used_by_email")

            if not primary.is_active:
                primary.is_active = True
                changed_fields.append("is_active")

            self.set_role_metadata(primary)
            if hasattr(primary, "metadata"):
                changed_fields.append("metadata")

            if dry_run:
                self.stdout.write(
                    self.style.WARNING(
                        f"[DRY RUN] Would update gift card {primary.code} for {email}"
                    )
                )
            else:
                if changed_fields:
                    # remove duplicates while keeping order
                    changed_fields = list(dict.fromkeys(changed_fields))
                    primary.save(update_fields=changed_fields)

                primary.tags.add(tag)

                # Deactivate duplicates so there is only one usable epremia card per user
                for duplicate in duplicates:
                    duplicate.is_active = False
                    duplicate.save(update_fields=["is_active"])

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Updated epremia gift card {primary.code} for {email}"
                    )
                )

            return "updated", len(duplicates)

        code = self.generate_code()
        gift_card = GiftCard(
            code=code,
            is_active=True,
            currency=currency,
            initial_balance_amount=balance,
            current_balance_amount=balance,
            used_by=user,
            used_by_email=email,
        )
        self.set_role_metadata(gift_card)

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"[DRY RUN] Would create epremia gift card for {email} with balance {balance} {currency}"
                )
            )
            return "created", 0

        gift_card.save()
        gift_card.tags.add(tag)

        self.stdout.write(
            self.style.SUCCESS(
                f"Created epremia gift card {gift_card.code} for {email}"
            )
        )
        return "created", 0

    def handle(self, csv_file, currency, dry_run=False, **options):
        User = get_user_model()

        created = 0
        updated = 0
        skipped = 0
        deactivated_duplicates = 0

        try:
            with open(csv_file, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)

                for row_num, row in enumerate(reader, start=1):
                    email = self.normalize_email(row[0])
                    raw_balance = row[1]

                    if not email:
                        self.stdout.write(
                            self.style.WARNING(f"Row {row_num}: empty email, skipping")
                        )
                        skipped += 1
                        continue

                    try:
                        balance = self.parse_balance(raw_balance)
                    except CommandError as exc:
                        raise CommandError(f"Row {row_num}: {exc}") from exc

                    user = User.objects.filter(email=email).first()
                    if not user:
                        message = f"Row {row_num}: user not found for email {email}"
                        self.stdout.write(self.style.WARNING(message + ", skipping"))
                        skipped += 1
                        continue

                    action, duplicates = self.create_or_update_gift_card(
                        user=user,
                        email=email,
                        balance=balance,
                        currency=currency,
                        dry_run=dry_run,
                    )

                    if action == "created":
                        created += 1
                    else:
                        updated += 1

                    deactivated_duplicates += duplicates

        except FileNotFoundError as exc:
            raise CommandError(f"CSV file not found: {csv_file}") from exc

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Created: {created}, updated: {updated}, "
                f"skipped: {skipped}, duplicates deactivated: {deactivated_duplicates}"
            )
        )
