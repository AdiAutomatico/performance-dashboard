"""
config.py — Environment loading, first-run prompts, date range helpers.
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv, set_key

ENV_PATH = Path(__file__).parent / ".env"

REQUIRED_KEYS = [
    ("AIRTABLE_PAT", "Airtable Personal Access Token"),
    ("AIRTABLE_BASE_ID", "Airtable Base ID (default: apptXzOS7cenTCkcr)"),
    ("GOOGLE_SHEETS_CREDS_PATH", "Path to Google service account JSON file"),
    ("GOOGLE_SHEET_ID", "Google Sheet ID (from the sheet URL)"),
]

OPTIONAL_KEYS = [
    ("FB_ACCESS_TOKEN", "Facebook API Access Token (leave blank to skip automatic Ad Spend fetching)"),
]


def load_config() -> dict:
    """Load .env, prompt for any missing required values, return config dict."""
    # Ensure .env exists
    if not ENV_PATH.exists():
        ENV_PATH.touch()

    load_dotenv(ENV_PATH)

    # Prompt for missing required keys
    for key, description in REQUIRED_KEYS:
        if not os.getenv(key):
            print(f"\n[Setup] Missing: {key}")
            print(f"  {description}")
            value = input(f"  Enter value: ").strip()
            if not value:
                print(f"ERROR: {key} is required. Aborting.")
                sys.exit(1)
            set_key(str(ENV_PATH), key, value)
            os.environ[key] = value

    # Prompt for missing optional keys
    for key, description in OPTIONAL_KEYS:
        if not os.getenv(key):
            print(f"\n[Setup] Optional: {key}")
            print(f"  {description}")
            value = input(f"  Enter value (or press Enter to skip): ").strip()
            set_key(str(ENV_PATH), key, value)
            os.environ[key] = value

    niche_map_raw = os.getenv("CLIENT_NICHE_MAP", "")
    niche_map = parse_niche_map(niche_map_raw)

    return {
        "airtable_pat": os.getenv("AIRTABLE_PAT"),
        "airtable_base_id": os.getenv("AIRTABLE_BASE_ID"),
        "google_creds_path": os.getenv("GOOGLE_SHEETS_CREDS_PATH"),
        "google_sheet_id": os.getenv("GOOGLE_SHEET_ID"),
        "niche_map": niche_map,
        "date_ranges": get_date_ranges(),
        "fb_access_token": os.getenv("FB_ACCESS_TOKEN", "").strip() or None,
    }


def parse_niche_map(raw: str) -> dict:
    """Parse 'ClientA:Air Duct,ClientB:Garage Door' into a dict."""
    if not raw.strip():
        return {}
    result = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            client, niche = part.split(":", 1)
            result[client.strip()] = niche.strip()
    return result


def get_date_ranges() -> dict:
    """Return ISO date strings for each dashboard period."""
    today = date.today()
    return {
        "Last 7 Days":  ((today - timedelta(days=7)).isoformat(),  today.isoformat()),
        "Last 14 Days": ((today - timedelta(days=14)).isoformat(), today.isoformat()),
        "Last 30 Days": ((today - timedelta(days=30)).isoformat(), today.isoformat()),
    }
