"""
airtable_client.py — Fetch records from Airtable with pagination, retry, and date filtering.
"""

import time
import urllib.parse
from typing import Optional

import requests

BASE_URL = "https://api.airtable.com/v0"


def _normalize_client(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()


def _to_float(value) -> float:
    if value is None:
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return 0.0

# Table IDs
TABLE_LEADS = "tblXjvte4qs1LmQeT"
TABLE_APPOINTMENTS = "tbl7oe48CtR3gOkZW"
TABLE_CALLS = "tblFNdmiLNpmOI0l4"
TABLE_PERFORMANCE = "tbl3dMCaCs2N6nLjt"
TABLE_CLIENTS = "tblekTNvSHqqXz17p"

LEADS_FIELDS = [
    "Client",
    "Created Date",
    "Status",
    "Campaign",
    "Service",
    "Speed to Lead (Minutes)",
]

APPOINTMENT_FIELDS = [
    "Lead",
    "Client (from Lead)",
    "Appointment Date",
    "Status (from Lead)",
    "Booked By",
    "Service (from Lead)",
]

CALL_FIELDS = [
    "Client (from Leads)",
    "Call Timestamp",
    "Call Duration",
    "Direction",
    "Call Agent Name",
]


class AirtableClient:
    def __init__(self, pat: str, base_id: str):
        self.pat = pat
        self.base_id = base_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {pat}",
            "Content-Type": "application/json",
        })

    def _get_with_retry(self, url: str, params: dict) -> dict:
        """GET with exponential backoff on 429. Raises after 4 attempts."""
        for attempt in range(4):
            resp = self.session.get(url, params=params)
            if resp.status_code == 429:
                delay = 1.0 * (2 ** attempt)
                print(f"  [Airtable] Rate limited. Waiting {delay:.0f}s (attempt {attempt + 1}/4)...")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Airtable rate limit exceeded after 4 attempts: {url}")

    def _paginate(self, table_id: str, params: dict) -> list:
        """Fetch all pages for a table query, returning a flat list of records."""
        url = f"{BASE_URL}/{self.base_id}/{table_id}"
        records = []
        page_params = dict(params)

        while True:
            data = self._get_with_retry(url, page_params)
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            page_params["offset"] = offset
            time.sleep(0.2)  # 200ms between pages to respect rate limits

        return records

    def _build_date_formula(self, date_field: str, start_iso: str, end_iso: str) -> str:
        """Build an Airtable filterByFormula for a date range (inclusive)."""
        return (
            f'AND('
            f'IS_AFTER({{{date_field}}}, "{start_iso}T00:00:00.000Z"), '
            f'IS_BEFORE({{{date_field}}}, "{end_iso}T23:59:59.999Z")'
            f')'
        )

    def fetch_leads(self, start_iso: str, end_iso: str) -> list:
        """Fetch leads created within the date range."""
        params = {
            "filterByFormula": self._build_date_formula("Created Date", start_iso, end_iso),
        }
        for field in LEADS_FIELDS:
            params.setdefault("fields[]", [])
            if isinstance(params["fields[]"], list):
                params["fields[]"].append(field)
            else:
                params["fields[]"] = [params["fields[]"], field]

        # requests handles list params correctly for fields[]
        flat_params = []
        for k, v in params.items():
            if isinstance(v, list):
                for item in v:
                    flat_params.append((k, item))
            else:
                flat_params.append((k, v))

        return self._paginate_with_flat_params(TABLE_LEADS, flat_params)

    def fetch_appointments(self, start_iso: str, end_iso: str) -> list:
        """Fetch appointments created within the date range (by record creation time)."""
        formula = (
            f'AND('
            f'IS_AFTER(CREATED_TIME(), "{start_iso}T00:00:00.000Z"), '
            f'IS_BEFORE(CREATED_TIME(), "{end_iso}T23:59:59.999Z")'
            f')'
        )
        flat_params = [("filterByFormula", formula)]
        for field in APPOINTMENT_FIELDS:
            flat_params.append(("fields[]", field))
        return self._paginate_with_flat_params(TABLE_APPOINTMENTS, flat_params)

    def fetch_calls(self, start_iso: str, end_iso: str) -> list:
        """Fetch calls within the date range."""
        flat_params = [
            ("filterByFormula", self._build_date_formula("Call Timestamp", start_iso, end_iso)),
        ]
        for field in CALL_FIELDS:
            flat_params.append(("fields[]", field))
        return self._paginate_with_flat_params(TABLE_CALLS, flat_params)

    def fetch_clients_with_fb_accounts(self) -> list:
        """
        Fetch all client records that have a FB Ad Account ID set.
        Returns: [{"client_name": str, "fb_account_id": str}, ...]
        """
        flat_params = [
            ("fields[]", "Client Name"),
            ("fields[]", "FB Ad Account ID"),
            ("filterByFormula", 'NOT({FB Ad Account ID} = "")'),
        ]
        records = self._paginate_with_flat_params(TABLE_CLIENTS, flat_params)
        clients = []
        for r in records:
            fields = r.get("fields", {})
            name = _normalize_client(fields.get("Client Name"))
            fb_id = _normalize_client(fields.get("FB Ad Account ID"))
            if name and fb_id:
                clients.append({"client_name": name, "fb_account_id": fb_id})
        return clients

    def fetch_performance_spend(self) -> dict:
        """
        Fetch ad spend from the Performance table.
        Returns {client_name: {"7d": float, "30d": float}}
        """
        flat_params = [
            ("fields[]", "Client"),
            ("fields[]", "7d Spend"),
            ("fields[]", "14d Spend"),
            ("fields[]", "30d Spend"),
        ]
        records = self._paginate_with_flat_params(TABLE_PERFORMANCE, flat_params)
        spend_map = {}
        for r in records:
            fields = r.get("fields", {})
            client = _normalize_client(fields.get("Client"))
            if not client:
                continue
            spend_map[client] = {
                "7d": _to_float(fields.get("7d Spend")),
                "14d": _to_float(fields.get("14d Spend")),
                "30d": _to_float(fields.get("30d Spend")),
            }
        return spend_map

    def _paginate_with_flat_params(self, table_id: str, flat_params: list) -> list:
        """Paginate using a list of (key, value) tuples to support repeated keys."""
        url = f"{BASE_URL}/{self.base_id}/{table_id}"
        records = []
        offset: Optional[str] = None

        while True:
            params = list(flat_params)
            if offset:
                params.append(("offset", offset))

            resp = None
            for attempt in range(4):
                r = self.session.get(url, params=params)
                if r.status_code == 429:
                    delay = 1.0 * (2 ** attempt)
                    print(f"  [Airtable] Rate limited. Waiting {delay:.0f}s...")
                    time.sleep(delay)
                    continue
                r.raise_for_status()
                resp = r.json()
                break

            if resp is None:
                raise RuntimeError(f"Airtable rate limit exceeded after 4 attempts: {url}")

            records.extend(resp.get("records", []))
            offset = resp.get("offset")
            if not offset:
                break
            time.sleep(0.2)

        return records
