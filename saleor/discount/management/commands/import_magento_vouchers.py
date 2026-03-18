import json
import re
from collections import defaultdict
from datetime import date, datetime, timezone

import requests
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from graphene import Decimal

from saleor.channel.models import Channel
from saleor.discount import DiscountValueType, VoucherType
from saleor.discount.models import Voucher, VoucherChannelListing, VoucherCode
from saleor.giftcard.models import GiftCard, GiftCardTag


class Magento:
    def __init__(self, domain, admin_username, admin_password):
        self.domain = domain
        self.admin_username = admin_username
        self.admin_password = admin_password
        self._token = None

    @property
    def api_base(self):
        return f"https://{self.domain}"

    @property
    def token(self):
        if not self._token:
            token = requests.post(
                self.api_base + "/rest/V1/integration/admin/token",
                data=json.dumps(
                    {
                        "username": self.admin_username,
                        "password": self.admin_password,
                    }
                ),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            )
            token_json = token.json()
            if isinstance(token_json, dict) and "message" in token_json:
                raise Exception(token_json["message"])
            self._token = token_json

        return self._token

    def request_method(self, method):
        return {"GET": requests.get, "PUT": requests.put, "POST": requests.post}[method]

    def request(self, method, endpoint, payload=None):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        url = self.api_base + endpoint

        response = self.request_method(method)(
            url, headers=headers, data=None if payload is None else json.dumps(payload)
        )
        response.raise_for_status()

        return response

    def paged_get(self, endpoint, page_size=500, max_pages=100):
        if not endpoint.endswith("&") and not endpoint.endswith("?"):
            endpoint = endpoint + "&"

        data = []
        for i in range(1, max_pages + 1):
            new_data = None
            new_data = self.request(
                "GET",
                f"{endpoint}searchCriteria[pageSize]={page_size}&searchCriteria[currentPage]={i}",
            ).json()
            if "items" not in new_data:
                new_data = []
            else:
                new_data = new_data["items"]
            if new_data is None:
                raise Exception("Failed to get page from m")
            data.extend(new_data)
            if len(new_data) == 0:
                break

        return data


def coupon_still_usable(coupon):
    usage_limit = coupon.get("usage_limit")
    times_used = coupon.get("times_used") or 0

    if usage_limit in (None, 0):
        return True
    return times_used < usage_limit


def build_rule_id_coupon_endpoint(rule_id):
    return (
        "/rest/V1/coupons/search?"
        f"searchCriteria[filterGroups][0][filters][0][field]=rule_id&"
        f"searchCriteria[filterGroups][0][filters][0][value]={rule_id}&"
        "searchCriteria[filterGroups][0][filters][0][conditionType]=eq"
    )


class Command(BaseCommand):
    help = "Import vouchers from m"

    def add_arguments(self, parser):
        parser.add_argument(
            "m_url",
            type=str,
            help="The unique URL of the m voucher to import",
        )

        parser.add_argument(
            "m_username",
            type=str,
        )

        parser.add_argument(
            "m_password",
            type=str,
        )

        parser.add_argument("--update-existing", action="store_true")

    def parse_m_datetime(self, value):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        raise CommandError(f"Unsupported m datetime format: {value}")

    def parse_m_date(self, value):
        dt = self.parse_m_datetime(value)
        return dt.date()

    def get_or_create_voucher(self, rule, shipping=False):
        start_date = (
            self.parse_m_date(rule.get("from_date"))
            if rule.get("from_date")
            else date.today()
        )
        end_date = (
            self.parse_m_date(rule.get("to_date")) if rule.get("to_date") else None
        )

        voucher, created = Voucher.objects.get_or_create(
            name=rule["name"],
            defaults={
                "type": VoucherType.SHIPPING if shipping else VoucherType.ENTIRE_ORDER,
                "name": rule["name"],
                "start_date": start_date or timezone.now(),
                "end_date": end_date,
                "discount_value_type": DiscountValueType.PERCENTAGE,
                "apply_once_per_customer": bool(rule.get("uses_per_customer") == 1),
                "single_use": bool(rule.get("uses_per_coupon") == 1),
                "usage_limit": None,
            },
        )

        if not created:
            changed = False
            fields = {
                "discount_value_type": DiscountValueType.PERCENTAGE,
                "apply_once_per_customer": bool(rule.get("uses_per_customer") == 1),
                "single_use": bool(rule.get("uses_per_coupon") == 1),
                "end_date": end_date,
            }
            for field, value in fields.items():
                if getattr(voucher, field) != value:
                    setattr(voucher, field, value)
                    changed = True

            if changed:
                voucher.save(update_fields=list(fields.keys()))

        return voucher

    def ensure_voucher_in_all_channels(self, voucher, rule):
        for channel in Channel.objects.all():
            VoucherChannelListing.objects.update_or_create(
                voucher=voucher,
                channel=channel,
                defaults={
                    "discount_value": rule.get("discount_amount", 0),
                    "currency": channel.currency_code,
                    "min_spent_amount": None,
                },
            )

    def ensure_voucher_codes(self, voucher, coupons):
        existing_codes = set(
            VoucherCode.objects.filter(voucher=voucher).values_list("code", flat=True)
        )

        to_create = []
        for coupon in coupons:
            code = (coupon.get("code") or "").strip()
            if not code or code in existing_codes:
                continue

            to_create.append(
                VoucherCode(
                    voucher=voucher,
                    code=code,
                    used=int(coupon.get("times_used") or 0),
                    is_active=True,
                )
            )

        if to_create:
            VoucherCode.objects.bulk_create(to_create, ignore_conflicts=True)

    def get_fixed_value_rules(self, m):
        return m.paged_get("/rest/V1/salesRules/search?")

    def get_coupons(self, m):
        return m.paged_get("/rest/V1/coupons/search?")

    def is_supported_rule(self, rule):
        return (
            rule.get("is_active") is True
            and rule.get("coupon_type") == "SPECIFIC_COUPON"
            and rule.get("simple_action") == "cart_fixed"
        )

    def build_coupons_by_rule_id(self, coupons):
        coupons_by_rule_id = defaultdict(list)
        for coupon in coupons:
            rule_id = coupon.get("rule_id")
            if rule_id is not None:
                coupons_by_rule_id[rule_id].append(coupon)
        return coupons_by_rule_id

    def get_balances(self, rule, coupon):
        amount = rule.get("discount_amount", 0) or 0
        times_used = int(coupon.get("times_used") or 0)

        current_balance = 0 if times_used > 0 else amount
        return amount, current_balance

    @transaction.atomic
    def import_coupon_as_gift_card(
        self, *, rule, coupon, currency, update_existing=False
    ):
        code = (coupon.get("code") or "").strip()
        if not code:
            self.stdout.write(
                self.style.WARNING(
                    f"Skipping m coupon_id={coupon.get('coupon_id')} with empty code"
                )
            )
            return False

        expiry_date = (
            self.parse_m_date(rule.get("to_date"))
            if rule.get("to_date")
            else date(2027, 12, 31)
        )
        initial_balance, current_balance = self.get_balances(rule, coupon)

        defaults = {
            "is_active": bool(rule.get("is_active", True)),
            "expiry_date": expiry_date,
            "currency": currency,
            "initial_balance_amount": initial_balance,
            "current_balance_amount": current_balance,
        }

        existing = GiftCard.objects.filter(code=code).first()
        if existing:
            if update_existing:
                changed = False
                for field, value in defaults.items():
                    if getattr(existing, field) != value:
                        setattr(existing, field, value)
                        changed = True

                if changed:
                    try:
                        existing.save(update_fields=list(defaults.keys()))
                    except Exception as e:
                        breakpoint()
                    self.stdout.write(self.style.SUCCESS(f"Updated gift card {code}"))
                else:
                    self.stdout.write(f"Gift card {code} already up to date")

                self.assign_gift_card_tags(existing)
            else:
                self.stdout.write(f"Gift card {code} already exists, skipping")
        elif current_balance > 0:
            gift_card = GiftCard.objects.create(
                code=code,
                **defaults,
            )
            self.assign_gift_card_tags(gift_card)
            self.stdout.write(self.style.SUCCESS(f"Created gift card {code}"))
            return True
        return False

    @transaction.atomic
    def import_percentage_rule_as_voucher(self, rule, coupons):
        voucher = self.get_or_create_voucher(rule)
        self.ensure_voucher_in_all_channels(voucher, rule)
        self.ensure_voucher_codes(voucher, coupons)
        self.stdout.write(
            self.style.SUCCESS(
                f"Imported percentage rule {rule['rule_id']} as voucher {voucher.name}"
            )
        )

    @transaction.atomic
    def import_shipping_rule_as_voucher(self, rule, coupons):
        voucher = self.get_or_create_voucher(rule, shipping=True)
        self.ensure_voucher_codes(voucher, coupons)
        self.stdout.write(
            self.style.SUCCESS(
                f"Imported shipping rule {rule['rule_id']} as voucher {voucher.name}"
            )
        )

    def get_rule_kind(self, rule):
        simple_action = rule.get("simple_action")

        if "DOSTAWA" in rule["name"] and rule["apply_to_shipping"]:
            return "shipping"

        if simple_action == "cart_fixed":
            return "gift_card"

        if simple_action == "by_percent":
            return "voucher"

        return None

    def get_gift_card_tag_name(self, code: str) -> str | None:
        code = (code or "").strip()
        if not code or "-" not in code:
            return None

        prefix = code.split("-", 1)[0].strip().upper()
        if not prefix:
            return None

        if "REKL" in prefix:
            return "reklamacja"

        if "MB" in prefix:
            return "MyBenefit"

        if "BCAN" in prefix:
            return "BonCard"

        if "KAP" in prefix:
            return "karta prezentowa"

        tag = re.sub(r"\d+", "", prefix).strip()
        return tag or None

    def assign_gift_card_tags(self, gift_card: GiftCard):
        tag_name = self.get_gift_card_tag_name(gift_card.code)
        if not tag_name:
            return

        tag, _ = GiftCardTag.objects.get_or_create(name=tag_name)
        gift_card.tags.add(tag)

    def handle(self, m_url, m_username, m_password, update_existing=False, **options):
        today = date.today()

        m = Magento(m_url, m_username, m_password)

        websites = m.request("GET", "/rest/V1/store/storeConfigs").json()
        store_currencies = {
            website["website_id"]: website["base_currency_code"] for website in websites
        }

        rules = m.paged_get(
            "/rest/V1/salesRules/search?"
            "searchCriteria[filterGroups][0][filters][0][field]=is_active&"
            "searchCriteria[filterGroups][0][filters][0][value]=1&"
            "searchCriteria[filterGroups][0][filters][0][conditionType]=eq&"
            "searchCriteria[filterGroups][1][filters][0][field]=to_date&"
            f"searchCriteria[filterGroups][1][filters][0][value]={today}&"
            "searchCriteria[filterGroups][1][filters][0][conditionType]=gt&"
        ) + m.paged_get(
            "/rest/V1/salesRules/search?"
            "searchCriteria[filterGroups][0][filters][0][field]=is_active&"
            "searchCriteria[filterGroups][0][filters][0][value]=1&"
            "searchCriteria[filterGroups][0][filters][0][conditionType]=eq&"
            "searchCriteria[filterGroups][1][filters][0][field]=to_date&"
            "searchCriteria[filterGroups][1][filters][0][conditionType]=null"
        )

        rules = [
            rule
            for rule in rules
            if not rule.get("to_date") or self.parse_m_date(rule["to_date"]) >= today
        ]

        active_rule_ids = {rule["rule_id"] for rule in rules}

        coupons = []
        for rule in rules:
            coupons.extend(m.paged_get(build_rule_id_coupon_endpoint(rule["rule_id"])))

        coupons = [
            coupon
            for coupon in coupons
            if coupon.get("rule_id") in active_rule_ids and coupon_still_usable(coupon)
        ]
        coupons_by_rule_id = self.build_coupons_by_rule_id(coupons)

        imported_gift_cards = 0
        imported_vouchers = 0
        skipped = 0

        for rule in rules:
            if not rule.get("is_active"):
                skipped += 1
                continue

            kind = self.get_rule_kind(rule)
            if kind == "gift_card":
                rule_coupons = coupons_by_rule_id.get(rule["rule_id"], [])
                for coupon in rule_coupons:
                    created_flag = self.import_coupon_as_gift_card(
                        rule=rule,
                        coupon=coupon,
                        currency=store_currencies.get(
                            rule.get("website_ids", [None])[0], "PLN"
                        ),
                        update_existing=update_existing,
                    )
                    if created_flag:
                        imported_gift_cards += 1

            elif kind == "voucher":
                relevant_coupons = coupons_by_rule_id.get(rule["rule_id"], [])

                if not relevant_coupons:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping percentage rule {rule['rule_id']} with no coupons"
                        )
                    )
                    skipped += 1
                else:
                    self.import_percentage_rule_as_voucher(rule, relevant_coupons)
                    imported_vouchers += 1

            elif kind == "shipping":
                relevant_coupons = coupons_by_rule_id.get(rule["rule_id"], [])

                if not relevant_coupons:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping shipping rule {rule['rule_id']} with no coupons"
                        )
                    )
                    skipped += 1
                else:
                    self.import_shipping_rule_as_voucher(rule, relevant_coupons)
                    imported_vouchers += 1

            else:
                skipped += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Gift cards created: {imported_gift_cards}, "
                f"vouchers imported: {imported_vouchers}, skipped: {skipped}"
            )
        )
