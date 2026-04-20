import csv
import uuid
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from saleor.giftcard.models import GiftCard, GiftCardTag


class Command(BaseCommand):
    help = (
        "Import epremia gift cards from CSV without header row, format: email,balance. "
        "Creates or updates exactly one epremia gift card per user using bulk operations."
    )

    ROLE = "epremia"
    TAG_NAME = "epremia"
    BATCH_SIZE = 1000

    def add_arguments(self, parser):
        parser.add_argument(
            "csv_file",
            type=str,
            help="Path to CSV file without header row, format: email,balance",
        )
        parser.add_argument(
            "--currency",
            type=str,
            default="PLN",
            help="Currency for created gift cards (default: PLN)",
        )
        parser.add_argument(
            "--expiry-date",
            type=str,
            required=True,
            help="Fixed expiry date for all gift cards, format: YYYY-MM-DD",
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

    def parse_expiry_date(self, raw_value: str):
        try:
            return datetime.strptime(raw_value.strip(), "%Y-%m-%d").date()
        except (ValueError, AttributeError) as exc:
            raise CommandError(
                f"Invalid expiry date: {raw_value}. Expected format: YYYY-MM-DD"
            ) from exc

    def generate_code(self) -> str:
        return f"EPREMIA-{uuid.uuid4().hex[:12].upper()}"

    def set_role_metadata(self, gift_card: GiftCard):
        if not hasattr(gift_card, "metadata"):
            raise CommandError(
                "GiftCard metadata field not found. "
                "Please adjust set_role_metadata() to your ModelWithMetadata implementation."
            )

        metadata = dict(gift_card.metadata or {})
        metadata["role"] = self.ROLE
        gift_card.metadata = metadata

    def has_epremia_role_metadata(self, gift_card: GiftCard) -> bool:
        metadata = getattr(gift_card, "metadata", None) or {}
        private_metadata = getattr(gift_card, "private_metadata", None) or {}

        return (
            metadata.get("role") == self.ROLE
            or private_metadata.get("role") == self.ROLE
        )

    def load_csv_rows(self, csv_file: str):
        parsed_rows = []
        skipped = 0

        try:
            with open(csv_file, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)

                for row_num, row in enumerate(reader, start=1):
                    if not row or all(not str(cell).strip() for cell in row):
                        skipped += 1
                        continue

                    if len(row) < 2:
                        raise CommandError(
                            f"Row {row_num}: expected at least 2 columns: email,balance"
                        )

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

                    parsed_rows.append(
                        {
                            "row_num": row_num,
                            "email": email,
                            "balance": balance,
                        }
                    )
        except FileNotFoundError as exc:
            raise CommandError(f"CSV file not found: {csv_file}") from exc

        return parsed_rows, skipped

    def load_users_by_email(self, emails):
        User = get_user_model()
        users = User.objects.filter(email__in=emails).only("id", "email")
        return {user.email.lower(): user for user in users}

    def load_existing_epremia_cards(self):
        qs = (
            GiftCard.objects.filter(tags__name=self.TAG_NAME)
            .distinct()
            .select_related("used_by")
        )

        cards = [gc for gc in qs if self.has_epremia_role_metadata(gc)]

        by_user_id = defaultdict(list)
        by_email = defaultdict(list)

        for gc in cards:
            if gc.used_by_id:
                by_user_id[gc.used_by_id].append(gc)
            if gc.used_by_email:
                by_email[self.normalize_email(gc.used_by_email)].append(gc)

        return by_user_id, by_email

    def choose_existing_cards_for_user(
        self, user, email, cards_by_user_id, cards_by_email
    ):
        cards = []

        if user.id in cards_by_user_id:
            cards.extend(cards_by_user_id[user.id])

        if email in cards_by_email:
            existing_ids = {gc.id for gc in cards}
            cards.extend(gc for gc in cards_by_email[email] if gc.id not in existing_ids)

        return sorted(cards, key=lambda gc: gc.pk)

    def ensure_tag_links(self, tag, gift_cards):
        if not gift_cards:
            return

        through_model = GiftCard.tags.through
        gift_card_ids = [gc.id for gc in gift_cards if gc.id]

        existing_links = set(
            through_model.objects.filter(
                giftcard_id__in=gift_card_ids,
                giftcardtag_id=tag.id,
            ).values_list("giftcard_id", flat=True)
        )

        new_links = [
            through_model(giftcard_id=gift_card_id, giftcardtag_id=tag.id)
            for gift_card_id in gift_card_ids
            if gift_card_id not in existing_links
        ]

        if new_links:
            through_model.objects.bulk_create(
                new_links,
                batch_size=self.BATCH_SIZE,
                ignore_conflicts=True,
            )

    @transaction.atomic
    def handle(self, csv_file, currency, expiry_date, dry_run=False, **options):
        expiry_date = self.parse_expiry_date(expiry_date)

        parsed_rows, skipped = self.load_csv_rows(csv_file)
        if not parsed_rows:
            self.stdout.write(
                self.style.WARNING(f"No valid rows found. Skipped: {skipped}")
            )
            return

        input_by_email = {}
        duplicate_input_rows = 0

        for item in parsed_rows:
            email = item["email"]
            if email in input_by_email:
                duplicate_input_rows += 1
            input_by_email[email] = item

        emails = list(input_by_email.keys())
        users_by_email = self.load_users_by_email(emails)
        cards_by_user_id, cards_by_email = self.load_existing_epremia_cards()

        tag, _ = GiftCardTag.objects.get_or_create(name=self.TAG_NAME)

        to_create = []
        to_update = []
        to_deactivate = []

        created = 0
        updated = 0
        missing_users = 0
        deactivated_duplicates = 0

        for email, item in input_by_email.items():
            balance = item["balance"]
            row_num = item["row_num"]

            user = users_by_email.get(email)
            if not user:
                self.stdout.write(
                    self.style.WARNING(
                        f"Row {row_num}: user not found for email {email}, skipping"
                    )
                )
                skipped += 1
                missing_users += 1
                continue

            existing_cards = self.choose_existing_cards_for_user(
                user=user,
                email=email,
                cards_by_user_id=cards_by_user_id,
                cards_by_email=cards_by_email,
            )

            primary = existing_cards[0] if existing_cards else None
            duplicates = existing_cards[1:] if len(existing_cards) > 1 else []

            if primary:
                if primary.currency != currency:
                    primary.currency = currency

                if primary.initial_balance_amount != balance:
                    primary.initial_balance_amount = balance

                if primary.current_balance_amount != balance:
                    primary.current_balance_amount = balance

                if primary.expiry_date != expiry_date:
                    primary.expiry_date = expiry_date

                if primary.used_by_id != user.id:
                    primary.used_by = user

                if primary.used_by_email != email:
                    primary.used_by_email = email

                if not primary.is_active:
                    primary.is_active = True

                self.set_role_metadata(primary)

                if dry_run:
                    self.stdout.write(
                        self.style.WARNING(
                            f"[DRY RUN] Would update gift card {primary.code} for {email}"
                        )
                    )
                else:
                    to_update.append(primary)

                updated += 1

                for duplicate in duplicates:
                    if duplicate.is_active:
                        duplicate.is_active = False
                        if dry_run:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"[DRY RUN] Would deactivate duplicate gift card {duplicate.code} for {email}"
                                )
                            )
                        else:
                            to_deactivate.append(duplicate)
                        deactivated_duplicates += 1

            elif balance > 0:
                gift_card = GiftCard(
                    code=self.generate_code(),
                    is_active=True,
                    currency=currency,
                    expiry_date=expiry_date,
                    initial_balance_amount=balance,
                    current_balance_amount=balance,
                    used_by=user,
                    used_by_email=email,
                )
                self.set_role_metadata(gift_card)

                if dry_run:
                    self.stdout.write(
                        self.style.WARNING(
                            f"[DRY RUN] Would create epremia gift card for {email} "
                            f"with balance {balance} {currency} and expiry date {expiry_date}"
                        )
                    )
                else:
                    to_create.append(gift_card)

                created += 1

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f"[DRY RUN] Done. Created: {created}, updated: {updated}, "
                    f"skipped: {skipped}, duplicates deactivated: {deactivated_duplicates}, "
                    f"duplicate input rows collapsed: {duplicate_input_rows}, missing users: {missing_users}"
                )
            )
            return

        if to_create:
            GiftCard.objects.bulk_create(to_create, batch_size=self.BATCH_SIZE)

        if to_update:
            GiftCard.objects.bulk_update(
                to_update,
                fields=[
                    "currency",
                    "initial_balance_amount",
                    "current_balance_amount",
                    "expiry_date",
                    "used_by",
                    "used_by_email",
                    "is_active",
                    "metadata",
                ],
                batch_size=self.BATCH_SIZE,
            )

        if to_deactivate:
            GiftCard.objects.bulk_update(
                to_deactivate,
                fields=["is_active"],
                batch_size=self.BATCH_SIZE,
            )

        self.ensure_tag_links(tag, to_create + to_update)

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Created: {created}, updated: {updated}, "
                f"skipped: {skipped}, duplicates deactivated: {deactivated_duplicates}, "
                f"duplicate input rows collapsed: {duplicate_input_rows}, missing users: {missing_users}"
            )
        )