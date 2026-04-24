"""
metrics.py — Pure transform: raw Airtable records → per-client metric dicts.
No I/O. All division is guarded against zero denominators.
"""

CONFIRMED_STATUSES = {
    "Appointment Booked",
    "Appointment Confirmed",
    "Show - Not Sold",
    "Update Required",
}


def _safe_div(numerator, denominator, default=""):
    """Return numerator/denominator, or default if denominator is 0/None."""
    if not denominator:
        return default
    return numerator / denominator


def _normalize(value) -> str:
    """Strip whitespace from a client name; return empty string if None."""
    if value is None:
        return ""
    if isinstance(value, list):
        # Airtable linked fields return arrays
        value = value[0] if value else ""
    return str(value).strip()


def compute_metrics(leads: list, appointments: list, calls: list) -> dict:
    """
    Returns {client_name: metrics_dict} for every client seen across all tables.

    metrics_dict keys:
        total_leads, total_calls, pickup_rate, confirmed_appts,
        appt_rate, show_rate, avg_speed_to_lead
    """
    errors = []

    # Build per-client record buckets
    client_leads: dict[str, list] = {}
    client_appts: dict[str, list] = {}
    client_calls: dict[str, list] = {}

    for r in leads:
        name = _normalize(r.get("fields", {}).get("Client"))
        if not name:
            continue
        client_leads.setdefault(name, []).append(r)

    for r in appointments:
        name = _normalize(r.get("fields", {}).get("Client (from Lead)"))
        if not name:
            continue
        client_appts.setdefault(name, []).append(r)

    for r in calls:
        name = _normalize(r.get("fields", {}).get("Client (from Leads)"))
        if not name:
            continue
        client_calls.setdefault(name, []).append(r)

    all_clients = sorted(
        set(client_leads) | set(client_appts) | set(client_calls)
    )

    metrics = {}
    for client in all_clients:
        c_leads = client_leads.get(client, [])
        c_appts = client_appts.get(client, [])
        c_calls = client_calls.get(client, [])

        # --- Total Leads ---
        total_leads = len(c_leads)

        # --- Total Calls ---
        total_calls = len(c_calls)

        # --- Pickup Rate (any answered outbound call, duration > 0) ---
        outbound_calls = [
            r for r in c_calls
            if _normalize(r.get("fields", {}).get("Direction")).lower() == "outbound"
        ]
        answered_calls = [
            r for r in outbound_calls
            if _call_duration(r) > 0
        ]
        pickup_rate = _safe_div(len(answered_calls), len(outbound_calls), default=0.0)

        # --- Answer Rate (outbound calls ≥ 60 seconds) ---
        answered_60s = [
            r for r in outbound_calls
            if _call_duration(r) >= 60
        ]
        answer_rate = _safe_div(len(answered_60s), len(outbound_calls), default=0.0)

        # --- Total Appointments (all, regardless of status) ---
        total_appts = len(c_appts)

        # --- Confirmed Appointments ---
        def _appt_status(r):
            v = r.get("fields", {}).get("Status (from Lead)")
            if isinstance(v, list):
                return _normalize(v[0]) if v else ""
            return _normalize(v)

        confirmed_appts = sum(
            1 for r in c_appts
            if _appt_status(r) in CONFIRMED_STATUSES
        )

        # --- Appointment Rate ---
        appt_rate = _safe_div(confirmed_appts, total_leads)

        # --- Show Rate ---
        # Shows = appointments in the period whose linked lead status is "Show - Not Sold"
        shows = sum(
            1 for r in c_appts
            if _appt_status(r) == "Show - Not Sold"
        )
        show_rate = _safe_div(shows, confirmed_appts)

        metrics[client] = {
            "total_leads": total_leads,
            "total_calls": total_calls,
            "pickup_rate": pickup_rate,
            "answer_rate": answer_rate,
            "total_appts": total_appts,
            "confirmed_appts": confirmed_appts,
            "appt_rate": appt_rate,
            "show_rate": show_rate,
        }

    return metrics


def _call_duration(record: dict) -> float:
    """Return call duration in seconds, or 0 if missing/invalid."""
    raw = record.get("fields", {}).get("Call Duration")
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0
