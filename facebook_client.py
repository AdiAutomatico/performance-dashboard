"""
facebook_client.py — Fetch ad spend from Facebook Graph API.
"""

import time
import requests

GRAPH_URL = "https://graph.facebook.com/v21.0"


def fetch_fb_spend(ad_account_id: str, start_iso: str, end_iso: str, access_token: str) -> float:
    """
    Fetch total spend for an ad account in the given date range (inclusive).
    Returns spend as float (e.g. 1234.56), or 0.0 if no data / error.
    ad_account_id: numeric ID only — 'act_' prefix is added automatically.
    """
    # Strip act_ prefix if someone already included it
    account_id = ad_account_id.strip().removeprefix("act_")

    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        "fields": "spend",
        "time_range": f'{{"since":"{start_iso}","until":"{end_iso}"}}',
        "time_increment": "all_days",   # one aggregated row for the whole range
        "level": "account",
        "access_token": access_token,
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            print(f"    [FB] Network error for act_{account_id}: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return 0.0

        if resp.status_code == 429:
            delay = 2 ** attempt
            print(f"    [FB] Rate limited — waiting {delay}s (attempt {attempt + 1}/3)...")
            time.sleep(delay)
            continue

        data = resp.json()

        if resp.status_code != 200:
            err_msg = data.get("error", {}).get("message", f"HTTP {resp.status_code}")
            print(f"    [FB] API error for act_{account_id}: {err_msg}")
            return 0.0

        records = data.get("data", [])
        if not records:
            return 0.0

        try:
            return float(records[0].get("spend", 0.0))
        except (ValueError, TypeError):
            return 0.0

    print(f"    [FB] Failed after 3 attempts for act_{account_id}")
    return 0.0
