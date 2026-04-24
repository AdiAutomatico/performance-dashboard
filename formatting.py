"""
formatting.py — Builds Google Sheets API batchUpdate payloads for conditional formatting.

All percentage metrics are stored as decimals (0.0–1.0) in the sheet.
Thresholds here match those stored values.
"""

# Column indices (0-based)
COL_CLIENT          = 0   # A
COL_INDUSTRY        = 1   # B
COL_TOTAL_LEADS     = 2   # C
COL_TOTAL_CALLS     = 3   # D
COL_ANSWER_RATE     = 4   # E
COL_CONFIRMED_APPTS = 5   # F
COL_TOTAL_APPTS     = 6   # G
COL_APPT_RATE       = 7   # H
COL_AD_SPEND        = 8   # I
COL_CPL             = 9   # J
COL_COST_PER_TOTAL  = 10  # K
COL_CPA             = 11  # L
COL_SHOW_RATE       = 12  # M

# Green / Yellow / Red RGB
GREEN  = (87,  187, 138)
YELLOW = (255, 214, 102)
RED    = (230, 124, 115)
WHITE  = (255, 255, 255)

# CPL thresholds per industry: {keyword: (green_max, yellow_max)}
# Green:  CPL < green_max
# Yellow: green_max ≤ CPL < yellow_max
# Red:    CPL ≥ yellow_max
INDUSTRY_CPL_THRESHOLDS = {
    "Air Duct":   (15,  25),
    "Garage Door":(35,  55),
    "Remodeling": (50,  75),
    "Kitchen":    (50,  75),
    "Bathroom":   (50,  75),
}

# CPA confirmed thresholds = CPL × 4
INDUSTRY_CPA_THRESHOLDS = {
    key: (g * 4, y * 4)
    for key, (g, y) in INDUSTRY_CPL_THRESHOLDS.items()
}

# Cost Per Total Appt thresholds: green = CPL × 2, yellow = CPL × 3
INDUSTRY_COST_PER_TOTAL_THRESHOLDS = {
    key: (g * 2, g * 3)
    for key, (g, _) in INDUSTRY_CPL_THRESHOLDS.items()
}


def _get_industry_thresholds(industry: str, threshold_map: dict):
    """
    Partial, case-insensitive match of industry string against threshold_map keys.
    Returns (green_max, yellow_max) tuple or (None, None) if no match.
    """
    if not industry:
        return None, None
    industry_lower = industry.lower()
    for key, thresholds in threshold_map.items():
        if key.lower() in industry_lower:
            return thresholds
    return None, None


def _col_letter(col_idx: int) -> str:
    """Convert 0-based column index to spreadsheet letter (A, B, ..., L, ...)."""
    result = ""
    n = col_idx + 1
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _color(r: int, g: int, b: int) -> dict:
    return {"red": r / 255, "green": g / 255, "blue": b / 255}


def _boolean_rule(condition_type: str, value: str, bg: tuple) -> dict:
    return {
        "booleanRule": {
            "condition": {
                "type": condition_type,
                "values": [{"userEnteredValue": value}],
            },
            "format": {"backgroundColor": _color(*bg)},
        }
    }


def _custom_formula_rule(formula: str, bg: tuple) -> dict:
    return {
        "booleanRule": {
            "condition": {
                "type": "CUSTOM_FORMULA",
                "values": [{"userEnteredValue": formula}],
            },
            "format": {"backgroundColor": _color(*bg)},
        }
    }


def build_conditional_format_requests(
    sheet_gid: int,
    num_data_rows: int,
    niche_map: dict,
    sorted_clients: list,
) -> list:
    """
    Returns a list of AddConditionalFormatRule request dicts.
    Rules are ordered green → yellow → red (Sheets stops at first match).
    num_data_rows: number of client rows (excludes header).
    """
    if num_data_rows == 0:
        return []

    start_row = 1  # 0-indexed; row 0 is header
    end_row = start_row + num_data_rows
    requests = []

    def base_range(col):
        return {"sheetId": sheet_gid, "startRowIndex": start_row, "endRowIndex": end_row,
                "startColumnIndex": col, "endColumnIndex": col + 1}

    def add_three_tier(col, green_gte, yellow_gte):
        """Green/yellow/red for higher-is-better metrics. Rules are mutually exclusive."""
        b = base_range(col)
        # Green: >= green threshold
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [b],
            **_boolean_rule("NUMBER_GREATER_THAN_EQ", str(green_gte), GREEN)}, "index": 0}})
        # Yellow: explicitly between yellow and green (no overlap with green)
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [b],
            **_custom_formula_rule(
                f'=AND(INDIRECT(ADDRESS(ROW(),{col+1}))>={yellow_gte},INDIRECT(ADDRESS(ROW(),{col+1}))<{green_gte})',
                YELLOW)}, "index": 0}})
        # Red: < yellow threshold
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [b],
            **_boolean_rule("NUMBER_LESS", str(yellow_gte), RED)}, "index": 0}})

    # ── Percentage metrics ────────────────────────────────────────────────────

    # Answer Rate (E): green ≥ 15%, yellow 10–14%, red < 10%
    add_three_tier(COL_ANSWER_RATE, 0.15, 0.10)

    # Appointment Rate (H): green ≥ 35%, yellow 25–34%, red < 25%
    add_three_tier(COL_APPT_RATE, 0.35, 0.25)

    # Show Rate (L): green ≥ 60%, yellow 40–59%, red < 40%
    add_three_tier(COL_SHOW_RATE, 0.60, 0.40)

    # ── Per-client CPL and CPA (industry-specific thresholds) ─────────────────
    cpl_letter = _col_letter(COL_CPL)
    cpa_letter = _col_letter(COL_CPA)

    for row_idx, client in enumerate(sorted_clients):
        industry = niche_map.get(client, "")

        cpl_green, cpl_yellow       = _get_industry_thresholds(industry, INDUSTRY_CPL_THRESHOLDS)
        cpt_green, cpt_yellow       = _get_industry_thresholds(industry, INDUSTRY_COST_PER_TOTAL_THRESHOLDS)
        cpa_green, cpa_yellow       = _get_industry_thresholds(industry, INDUSTRY_CPA_THRESHOLDS)

        sheet_row = start_row + row_idx
        formula_row = sheet_row + 1  # 1-indexed for Sheets formula

        def add_per_row_three_tier(col, col_letter, green_t, yellow_t):
            cell_range = {
                "sheetId": sheet_gid,
                "startRowIndex": sheet_row,
                "endRowIndex": sheet_row + 1,
                "startColumnIndex": col,
                "endColumnIndex": col + 1,
            }
            c = col_letter
            # Green: not blank AND below green threshold
            requests.append({"addConditionalFormatRule": {"rule": {"ranges": [cell_range],
                **_custom_formula_rule(
                    f'=AND($A{formula_row}="{client}",{c}{formula_row}<>"",{c}{formula_row}<{green_t})',
                    GREEN)}, "index": 0}})
            # Yellow: not blank AND between green and yellow thresholds
            requests.append({"addConditionalFormatRule": {"rule": {"ranges": [cell_range],
                **_custom_formula_rule(
                    f'=AND($A{formula_row}="{client}",{c}{formula_row}<>"",{c}{formula_row}>={green_t},{c}{formula_row}<{yellow_t})',
                    YELLOW)}, "index": 0}})
            # Red: not blank AND at or above yellow threshold
            requests.append({"addConditionalFormatRule": {"rule": {"ranges": [cell_range],
                **_custom_formula_rule(
                    f'=AND($A{formula_row}="{client}",{c}{formula_row}<>"",{c}{formula_row}>={yellow_t})',
                    RED)}, "index": 0}})

        if cpl_green is not None:
            add_per_row_three_tier(COL_CPL, cpl_letter, cpl_green, cpl_yellow)

        if cpt_green is not None:
            add_per_row_three_tier(COL_COST_PER_TOTAL, _col_letter(COL_COST_PER_TOTAL), cpt_green, cpt_yellow)

        if cpa_green is not None:
            add_per_row_three_tier(COL_CPA, cpa_letter, cpa_green, cpa_yellow)

    return requests


def build_delete_all_cf_requests(sheet_gid: int, existing_rules: list) -> list:
    """
    Build DeleteConditionalFormatRule requests for all existing rules on a sheet.
    Deleted in reverse order so indices don't shift.
    """
    return [
        {"deleteConditionalFormatRule": {"sheetId": sheet_gid, "index": i}}
        for i in range(len(existing_rules) - 1, -1, -1)
    ]
