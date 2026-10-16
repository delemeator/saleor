import csv

from django.core.management.base import BaseCommand
from django.db import transaction

from saleor.account.models import User


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "csv_file",
            type=str,
            help="Path to CSV file without header row, format: email for account to clean",
        )
        parser.add_argument(
            "--max_id",
            type=int,
            help="Maximum ID for address to delete",
        )

    def _read_emails_from_csv(self, csv_file):
        emails = set()
        with open(csv_file) as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:
                    emails.add(row[0].strip())
        return emails

    @transaction.atomic
    def handle(self, csv_file, max_id, **options):
        users = User.objects.filter(email__in=self._read_emails_from_csv(csv_file))

        for user in users:
            addresses = (
                user.addresses.filter(id__lte=max_id)
                if max_id
                else user.addresses.all()
            )
            address_count = addresses.count()
            addresses.delete()
            self.stdout.write(
                self.style.SUCCESS(
                    f"Deleted {address_count} addresses for user {user.email}"
                )
            )
