#!/usr/bin/env python3
"""
main.py — Airtable → Google Sheets performance dashboard.

Usage:
    python main.py

Cron example (every Monday at 7am):
    0 7 * * 1 cd /path/to/airtable-dashboard && python main.py >> dashboard.log 2>&1
"""

import json
import sys
import traceback
from datetime import date
from pathlib import Path

from config import load_config
from airtable_client import AirtableClient
from metrics import compute_metrics
from sheets_client import SheetsClient
from facebook_client import fetch_fb_spend

ERRORS_FILE = Path(__file__).parent / "errors.json"


def print_summary(
    date_ranges: dict,
    clients_updated: int,
    counts: dict,
    skipped: int,
    errors: list,
):
    print()
    print("=" * 40)
    print("  Dashboard Update Complete")
    print("=" * 40)
    for tab_name, (start, end) in date_ranges.items():
        label = f"Period: {tab_name}"
        print(f"  {label:<22} ({start} → {end})")
    print(f"  Clients updated  : {clients_updated}")
    for table, count in counts.items():
        print(f"  {table:<18} : {count:,}")
    if skipped:
        print(f"  Records skipped  : {skipped} (see errors.json)")
    else:
        print(f"  Records skipped  : 0")
    print(f"  Errors           : {len(errors)}")
    print("=" * 40)
    print()


def run():
    errors = []
    record_counts = {}
    clients_updated = 0
    skipped = 0

    # ── Config ──────────────────────────────────────────────────────────────
    print("\n[1/4] Loading configuration...")
    try:
        config = load_config()
    except SystemExit:
        raise
    except Exception as e:
        print(f"ERROR: Failed to load config: {e}")
        sys.exit(1)

    # ── Clients ──────────────────────────────────────────────────────────────
    print("[2/4] Connecting to Google Sheets...")
    try:
        sheets = SheetsClient(config["google_creds_path"], config["google_sheet_id"])
        sheets.ensure_settings_tab()
        tab_names = list(config["date_ranges"].keys())
        sheets.ensure_tabs_exist(tab_names)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Could not connect to Google Sheets: {e}")
        sys.exit(1)

    # ── Read industry map once from Settings tab ─────────────────────────────
    print("  Reading industry map from Settings tab...")
    industry_map = sheets.read_industry_from_settings()

    # ── Per-period processing ────────────────────────────────────────────────
    print("[3/4] Fetching Airtable data and computing metrics...")
    airtable = AirtableClient(config["airtable_pat"], config["airtable_base_id"])

    # ── Facebook spend setup ─────────────────────────────────────────────────
    fb_token = config.get("fb_access_token")
    fb_client_accounts = {}  # {client_name: fb_account_id}

    if fb_token:
        print("\n  [FB] Fetching client Facebook Ad Account IDs from Airtable...")
        try:
            fb_clients = airtable.fetch_clients_with_fb_accounts()
            fb_client_accounts = {c["client_name"]: c["fb_account_id"] for c in fb_clients}
            print(f"  [FB] {len(fb_client_accounts)} clients with FB Ad Accounts found.")
        except Exception as e:
            print(f"  [FB] WARNING: Could not fetch FB account IDs: {e}")
            print(f"  [FB] Will fall back to reading Ad Spend from sheet.")
            fb_token = None  # disable FB fetching for this run
    else:
        print("\n  [FB] No FB_ACCESS_TOKEN set — reading Ad Spend from sheet as usual.")

    all_sorted_clients = set()

    for tab_name, (start_iso, end_iso) in config["date_ranges"].items():
        print(f"\n  [{tab_name}] {start_iso} → {end_iso}")

        leads, appointments, calls = [], [], []
        table_errors = []

        try:
            print(f"    Fetching leads...")
            leads = airtable.fetch_leads(start_iso, end_iso)
            print(f"    → {len(leads):,} leads")
        except Exception as e:
            msg = f"Failed to fetch Leads for {tab_name}: {e}"
            print(f"    WARNING: {msg}")
            table_errors.append({"table": "Leads", "period": tab_name, "error": str(e)})

        try:
            print(f"    Fetching appointments...")
            appointments = airtable.fetch_appointments(start_iso, end_iso)
            print(f"    → {len(appointments):,} appointments")
        except Exception as e:
            msg = f"Failed to fetch Appointments for {tab_name}: {e}"
            print(f"    WARNING: {msg}")
            table_errors.append({"table": "Appointments", "period": tab_name, "error": str(e)})

        try:
            print(f"    Fetching calls...")
            calls = airtable.fetch_calls(start_iso, end_iso)
            print(f"    → {len(calls):,} calls")
        except Exception as e:
            msg = f"Failed to fetch Calls for {tab_name}: {e}"
            print(f"    WARNING: {msg}")
            table_errors.append({"table": "Calls", "period": tab_name, "error": str(e)})

        errors.extend(table_errors)

        record_counts[f"{tab_name} leads"] = len(leads)
        record_counts[f"{tab_name} calls"] = len(calls)
        record_counts[f"{tab_name} appts"] = len(appointments)

        # Compute metrics
        metrics = compute_metrics(leads, appointments, calls)
        num_clients = len(metrics)
        print(f"    → {num_clients} clients identified")

        if num_clients == 0:
            print(f"    No clients found for {tab_name} — skipping write.")
            continue

        # ── Build Ad Spend map ────────────────────────────────────────────────
        # Always read existing sheet values first (for clients without FB accounts
        # and as a safety fallback).
        print(f"    Reading existing Ad Spend values from sheet...")
        try:
            sheet_ad_spend_map = sheets.read_ad_spend(tab_name)
        except Exception as e:
            print(f"ERROR: Cannot read Ad Spend from sheet '{tab_name}': {e}")
            print("Aborting to protect existing Ad Spend data.")
            sys.exit(1)

        if fb_token and fb_client_accounts:
            print(f"    Fetching Ad Spend from Facebook API...")
            ad_spend_map = dict(sheet_ad_spend_map)  # start with sheet values as base
            for client_name, fb_account_id in fb_client_accounts.items():
                try:
                    spend = fetch_fb_spend(fb_account_id, start_iso, end_iso, fb_token)
                    ad_spend_map[client_name] = spend if spend > 0 else ""
                    if spend > 0:
                        print(f"      {client_name}: ${spend:,.2f}")
                    else:
                        print(f"      {client_name}: no spend data")
                except Exception as e:
                    print(f"      WARNING: FB spend fetch failed for {client_name}: {e}")
                    # keep whatever was already in sheet_ad_spend_map
        else:
            ad_spend_map = sheet_ad_spend_map

        # Write metrics + preserved Ad Spend + preserved Industry
        print(f"    Writing to sheet tab '{tab_name}'...")
        try:
            sorted_clients = sheets.write_tab(tab_name, metrics, ad_spend_map, industry_map)
        except Exception as e:
            print(f"ERROR: Failed to write sheet tab '{tab_name}': {e}")
            traceback.print_exc()
            sys.exit(1)

        all_sorted_clients.update(sorted_clients)
        clients_updated = max(clients_updated, num_clients)

        # Apply conditional formatting (industry_map from sheet takes precedence)
        print(f"    Applying conditional formatting...")
        try:
            sheets.apply_formatting(tab_name, num_clients, config["niche_map"], sorted_clients, industry_map)
        except Exception as e:
            msg = f"Conditional formatting failed for {tab_name}: {e}"
            print(f"    WARNING: {msg}")
            errors.append({"stage": "formatting", "period": tab_name, "error": str(e)})

    # ── Update Settings tab with any new clients ─────────────────────────────
    if all_sorted_clients:
        try:
            sheets.update_settings_clients(all_sorted_clients)
        except Exception as e:
            print(f"  WARNING: Could not update Settings tab: {e}")

    # ── Write errors.json if needed ──────────────────────────────────────────
    print("\n[4/4] Finalizing...")
    if errors:
        with open(ERRORS_FILE, "w") as f:
            json.dump(errors, f, indent=2)
        print(f"  Errors written to {ERRORS_FILE}")

    # ── Summary ──────────────────────────────────────────────────────────────
    # Flatten counts for display
    display_counts = {}
    for tab_name in config["date_ranges"]:
        display_counts[f"Leads ({tab_name})"] = record_counts.get(f"{tab_name} leads", 0)
        display_counts[f"Calls ({tab_name})"] = record_counts.get(f"{tab_name} calls", 0)
        display_counts[f"Appts ({tab_name})"] = record_counts.get(f"{tab_name} appts", 0)

    print_summary(
        date_ranges=config["date_ranges"],
        clients_updated=clients_updated,
        counts=display_counts,
        skipped=skipped,
        errors=errors,
    )


if __name__ == "__main__":
    run()
