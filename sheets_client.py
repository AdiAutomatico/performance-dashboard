"""
sheets_client.py — Google Sheets I/O: read Ad Spend, write tabs, apply formatting.
"""

import json
from datetime import datetime
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from formatting import (
    build_conditional_format_requests,
    build_delete_all_cf_requests,
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SETTINGS_TAB = "⚙ Settings"

HEADERS = [
    "Client",
    "Industry",
    "Total Leads",
    "Total Calls Made",
    "Answer Rate %",
    "Confirmed Appts",
    "Total Appts",
    "Appointment Rate %",
    "Ad Spend",
    "Cost Per Lead",
    "Cost Per Total Appt",
    "Cost Per Confirmed Appt",
    "Show Rate %",
]

PERCENT_COLS   = [4, 7, 12]      # E, H, M (0-indexed)
CURRENCY_COLS  = [8, 9, 10, 11]  # I, J, K, L — Ad Spend, CPL, Cost Per Total, CPA
NUM_COLS = 13  # A through M


class SheetsClient:
    def __init__(self, creds_path: str, sheet_id: str):
        # Support both a file path (local) and inline JSON string (Streamlit Cloud)
        try:
            info = json.loads(creds_path)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=SCOPES
            )
        except (json.JSONDecodeError, TypeError):
            creds_file = Path(creds_path)
            if not creds_file.exists():
                raise FileNotFoundError(f"Google credentials file not found: {creds_path}")
            creds = service_account.Credentials.from_service_account_file(
                str(creds_file), scopes=SCOPES
            )
        self.service = build("sheets", "v4", credentials=creds)
        self.sheet_id = sheet_id
        self._tab_gid_cache: dict[str, int] = {}

    def _sheets(self):
        return self.service.spreadsheets()

    def ensure_settings_tab(self):
        """Create the Settings tab if it doesn't exist and apply basic formatting."""
        meta = self._sheets().get(spreadsheetId=self.sheet_id).execute()
        existing = {s["properties"]["title"] for s in meta["sheets"]}

        if SETTINGS_TAB not in existing:
            self._sheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": SETTINGS_TAB}}}]},
            ).execute()
            # Write headers
            self._sheets().values().update(
                spreadsheetId=self.sheet_id,
                range=f"'{SETTINGS_TAB}'!A1:B1",
                valueInputOption="USER_ENTERED",
                body={"values": [["Client", "Industry"]]},
            ).execute()

        # Refresh GID cache
        meta = self._sheets().get(spreadsheetId=self.sheet_id).execute()
        for s in meta["sheets"]:
            self._tab_gid_cache[s["properties"]["title"]] = s["properties"]["sheetId"]

        # Format: bold header, freeze row 1, column widths
        gid = self._tab_gid_cache[SETTINGS_TAB]
        self._sheets().batchUpdate(
            spreadsheetId=self.sheet_id,
            body={"requests": [
                {"updateSheetProperties": {
                    "properties": {"sheetId": gid,
                                   "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount",
                }},
                {"repeatCell": {
                    "range": {"sheetId": gid, "startRowIndex": 0, "endRowIndex": 1,
                              "startColumnIndex": 0, "endColumnIndex": 2},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }},
                {"updateDimensionProperties": {
                    "range": {"sheetId": gid, "dimension": "COLUMNS",
                              "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 220},
                    "fields": "pixelSize",
                }},
                {"updateDimensionProperties": {
                    "range": {"sheetId": gid, "dimension": "COLUMNS",
                              "startIndex": 1, "endIndex": 2},
                    "properties": {"pixelSize": 180},
                    "fields": "pixelSize",
                }},
            ]},
        ).execute()

    def read_industry_from_settings(self) -> dict:
        """Read Client → Industry mapping from the Settings tab."""
        try:
            result = self._sheets().values().get(
                spreadsheetId=self.sheet_id,
                range=f"'{SETTINGS_TAB}'!A2:B",
            ).execute()
        except HttpError:
            return {}
        rows = result.get("values", [])
        return {
            row[0].strip(): row[1].strip()
            for row in rows
            if len(row) >= 2 and row[0].strip() and row[1].strip()
        }

    def update_settings_clients(self, all_clients: set):
        """Append any new clients to the Settings tab (never overwrites existing rows)."""
        try:
            result = self._sheets().values().get(
                spreadsheetId=self.sheet_id,
                range=f"'{SETTINGS_TAB}'!A2:A",
            ).execute()
            existing_clients = {r[0].strip() for r in result.get("values", []) if r}
            next_row = len(result.get("values", [])) + 2  # +1 for header, +1 for next empty
        except HttpError:
            existing_clients = set()
            next_row = 2

        new_clients = sorted(c for c in all_clients if c not in existing_clients)
        if not new_clients:
            return

        self._sheets().values().update(
            spreadsheetId=self.sheet_id,
            range=f"'{SETTINGS_TAB}'!A{next_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [[client, ""] for client in new_clients]},
        ).execute()
        print(f"  [Settings] Added {len(new_clients)} new client(s) to Settings tab.")

    def ensure_tabs_exist(self, tab_names: list[str]):
        """Create any tabs that don't already exist."""
        meta = self._sheets().get(spreadsheetId=self.sheet_id).execute()
        existing = {s["properties"]["title"] for s in meta["sheets"]}

        requests = []
        for name in tab_names:
            if name not in existing:
                requests.append({
                    "addSheet": {
                        "properties": {"title": name}
                    }
                })

        if requests:
            self._sheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body={"requests": requests},
            ).execute()

        # Refresh GID cache
        meta = self._sheets().get(spreadsheetId=self.sheet_id).execute()
        for s in meta["sheets"]:
            self._tab_gid_cache[s["properties"]["title"]] = s["properties"]["sheetId"]

    def _get_tab_gid(self, tab_name: str) -> int:
        if tab_name not in self._tab_gid_cache:
            meta = self._sheets().get(spreadsheetId=self.sheet_id).execute()
            for s in meta["sheets"]:
                self._tab_gid_cache[s["properties"]["title"]] = s["properties"]["sheetId"]
        return self._tab_gid_cache[tab_name]

    def read_ad_spend(self, tab_name: str) -> dict:
        """
        Read existing Ad Spend values from column G.
        Returns {client_name: ad_spend_value} where value is float or "".
        """
        range_notation = f"'{tab_name}'!A2:I"
        try:
            result = self._sheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_notation,
            ).execute()
        except HttpError as e:
            if e.resp.status == 400:
                return {}
            raise

        rows = result.get("values", [])
        ad_spend_map = {}
        for row in rows:
            if not row:
                continue
            client = row[0].strip() if row else ""
            if not client:
                continue
            # Ad Spend is col I = index 8 in A:I range
            if len(row) >= 9:
                raw = row[8]
                try:
                    ad_spend_map[client] = float(str(raw).replace(",", "").replace("$", ""))
                except (ValueError, TypeError):
                    ad_spend_map[client] = ""
            else:
                ad_spend_map[client] = ""
        return ad_spend_map

    def read_industry_map(self, tab_name: str) -> dict:
        """
        Read existing Industry values from column L.
        Returns {client_name: industry_string}.
        """
        range_notation = f"'{tab_name}'!A2:B"
        try:
            result = self._sheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_notation,
            ).execute()
        except HttpError as e:
            if e.resp.status == 400:
                return {}
            raise

        rows = result.get("values", [])
        industry_map = {}
        for row in rows:
            if not row:
                continue
            client = row[0].strip() if row else ""
            if not client:
                continue
            # Industry is col B = index 1 in A:B range
            if len(row) >= 2:
                industry_map[client] = str(row[1]).strip()
        return industry_map

    def write_tab(
        self,
        tab_name: str,
        metrics: dict,
        ad_spend_map: dict,
        industry_map: dict = None,
    ):
        """
        Write the full tab (header + all client rows). Ad Spend values are
        taken from ad_spend_map (pre-read), never overwritten.
        """
        if industry_map is None:
            industry_map = {}

        sorted_clients = sorted(metrics.keys())
        rows = [HEADERS]

        for client in sorted_clients:
            m = metrics[client]
            ad_spend = ad_spend_map.get(client, "")
            industry = industry_map.get(client, "")

            # CPL and CPA — only compute when ad spend is a real number
            try:
                spend_val = float(ad_spend) if ad_spend != "" else None
            except (ValueError, TypeError):
                spend_val = None

            cpl = ""
            cpa = ""
            cpa_total = ""
            if spend_val and spend_val > 0:
                if m["total_leads"]:
                    cpl = round(spend_val / m["total_leads"], 2)
                if m["confirmed_appts"]:
                    cpa = round(spend_val / m["confirmed_appts"], 2)
                if m["total_appts"]:
                    cpa_total = round(spend_val / m["total_appts"], 2)

            # Format percentage decimals to 4dp for display precision
            def pct(v):
                if v == "" or v is None:
                    return ""
                return round(float(v), 4)

            rows.append([
                client,
                industry,              # B — manually managed, preserved from read
                m["total_leads"],
                m["total_calls"],
                pct(m["answer_rate"]),
                m["confirmed_appts"],
                m["total_appts"],
                pct(m["appt_rate"]),
                ad_spend,              # I — manually managed, preserved from read
                cpl,                   # J
                cpa_total,             # K
                cpa,                   # L
                pct(m["show_rate"]),
            ])

        range_notation = f"'{tab_name}'!A1"

        # Clear existing content first
        self._sheets().values().clear(
            spreadsheetId=self.sheet_id,
            range=f"'{tab_name}'!A:M",
        ).execute()

        self._sheets().values().update(
            spreadsheetId=self.sheet_id,
            range=range_notation,
            valueInputOption="USER_ENTERED",
            body={"values": rows},
        ).execute()

        # Write last updated timestamp to cell N1
        now = datetime.now().strftime("Last updated: %b %d, %Y at %I:%M %p")
        self._sheets().values().update(
            spreadsheetId=self.sheet_id,
            range=f"'{tab_name}'!O1",
            valueInputOption="USER_ENTERED",
            body={"values": [[now]]},
        ).execute()

        # Apply formatting: freeze header, column widths, number formats
        self._apply_sheet_formatting(tab_name, len(sorted_clients))

        return sorted_clients

    def _apply_sheet_formatting(self, tab_name: str, num_data_rows: int):
        """Apply freeze, column widths, and percent number formats."""
        gid = self._get_tab_gid(tab_name)
        requests = []

        # Freeze row 1 and column A
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": gid,
                    "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1},
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        })

        # Column A (Client) = 180, B (Industry) = 160, C–L = 130
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": gid, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 180},
                "fields": "pixelSize",
            }
        })
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": gid, "dimension": "COLUMNS",
                          "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 160},
                "fields": "pixelSize",
            }
        })
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": gid, "dimension": "COLUMNS",
                          "startIndex": 2, "endIndex": 13},
                "properties": {"pixelSize": 130},
                "fields": "pixelSize",
            }
        })

        # Bold header row
        requests.append({
            "repeatCell": {
                "range": {"sheetId": gid, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": 13},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        })

        # First reset ALL data columns to plain number format (clears any stale percent
        # format left over from previous layout changes)
        if num_data_rows > 0:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": gid,
                        "startRowIndex": 1,
                        "endRowIndex": 1 + num_data_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": NUM_COLS,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "NUMBER", "pattern": ""}
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            })

        # Then apply percent format only to the correct columns
        if num_data_rows > 0:
            for col_idx in PERCENT_COLS:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": gid,
                            "startRowIndex": 1,
                            "endRowIndex": 1 + num_data_rows,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {"type": "PERCENT", "pattern": "0.0%"}
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat",
                    }
                })

        # Apply $ currency format to CPL and CPA columns
        if num_data_rows > 0:
            for col_idx in CURRENCY_COLS:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": gid,
                            "startRowIndex": 1,
                            "endRowIndex": 1 + num_data_rows,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat",
                    }
                })

        if requests:
            self._sheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body={"requests": requests},
            ).execute()

    def apply_formatting(self, tab_name: str, num_data_rows: int, niche_map: dict, sorted_clients: list, industry_map: dict = None):
        """Delete existing conditional format rules and apply fresh ones."""
        gid = self._get_tab_gid(tab_name)

        # Get existing conditional format rules for this sheet
        meta = self._sheets().get(
            spreadsheetId=self.sheet_id,
            fields="sheets(properties(sheetId,title),conditionalFormats)",
        ).execute()

        existing_rules = []
        for s in meta.get("sheets", []):
            if s["properties"]["sheetId"] == gid:
                existing_rules = s.get("conditionalFormats", [])
                break

        all_requests = []

        # Delete existing rules
        delete_requests = build_delete_all_cf_requests(gid, existing_rules)
        all_requests.extend(delete_requests)

        # Add new rules — industry_map (from sheet col L) takes precedence over niche_map (.env)
        effective_industry_map = {**(niche_map or {}), **(industry_map or {})}
        add_requests = build_conditional_format_requests(
            sheet_gid=gid,
            num_data_rows=num_data_rows,
            niche_map=effective_industry_map,
            sorted_clients=sorted_clients,
        )
        all_requests.extend(add_requests)

        if all_requests:
            self._sheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body={"requests": all_requests},
            ).execute()
