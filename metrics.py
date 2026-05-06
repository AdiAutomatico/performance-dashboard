"""
metrics.py — Pure transform: raw Airtable records → per-client metric dicts.
No I/O. All division is guarded against zero denominators.

Appointments are joined to leads by record ID (the "Lead" linked field),
not by date range — this ensures appt rate and show rate are always accurate.
"""

CONFIRMED_STATUSES = {
    "Appointment Booked",
    "Appointment Confirmed",
    "Show - Not Sold",
    "Update Required",
}


def _safe_div(numerator, denominator, default=""):
    if not denominator:
        return default
    return numerator / denominator


def _normalize(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()


def _get_linked_lead_ids(appointment: dict) -> set:
    """Return the set of lead record IDs linked to this appointment via the 'Lead' field."""
    v = appointment.get("fields", {}).get("Lead")
    if not v:
        return set()
    if isinstance(v, list):
        return set(v)
    return {str(v)}


def _appt_status(r) -> str:
    v = r.get("fields", {}).get("Status (from Lead)")
    if isinstance(v, list):
        return _normalize(v[0]) if v else ""
    return _normalize(v)


def compute_metrics(leads: list, appointments: list, calls: list) -> dict:
    """
    Returns {client_name: metrics_dict}.

    Key change: appointments are matched to leads by record ID, not by date.
    This means appt rate = appointments from leads in the selected period /
    total leads in the selected period — always accurate.
    """

    # Build lead record ID → client name map
    lead_id_to_client: dict[str, str] = {}
    client_leads: dict[str, list] = {}
    client_calls: dict[str, list] = {}

    for r in leads:
        lead_id = r.get("id", "")
        name = _normalize(r.get("fields", {}).get("Client"))
        if not name:
            continue
        if lead_id:
            lead_id_to_client[lead_id] = name
        client_leads.setdefault(name, []).append(r)

    # Match appointments to clients via lead record ID
    client_appts: dict[str, list] = {}
    for r in appointments:
        linked_ids = _get_linked_lead_ids(r)
        # Find which client this appointment belongs to via lead ID
        matched_client = None
        for lid in linked_ids:
            if lid in lead_id_to_client:
                matched_client = lead_id_to_client[lid]
                break
        # Fallback: use Client (from Lead) lookup if no ID match
        if not matched_client:
            matched_client = _normalize(r.get("fields", {}).get("Client (from Lead)"))
            # Only include fallback if that client has leads in our period
            if matched_client not in client_leads:
                continue
        if matched_client:
            client_appts.setdefault(matched_client, []).append(r)

    for r in calls:
        name = _normalize(r.get("fields", {}).get("Client (from Leads)"))
        if not name:
            continue
        client_calls.setdefault(name, []).append(r)

    # Only show clients that have leads in the selected period
    all_clients = sorted(set(client_leads) | set(client_calls))

    metrics = {}
    for client in all_clients:
        c_leads = client_leads.get(client, [])
        c_appts = client_appts.get(client, [])
        c_calls = client_calls.get(client, [])

        total_leads = len(c_leads)
        total_calls = len(c_calls)

        outbound_calls = [
            r for r in c_calls
            if _normalize(r.get("fields", {}).get("Direction")).lower() == "outbound"
        ]
        answered_60s = [r for r in outbound_calls if _call_duration(r) >= 60]
        answer_rate = _safe_div(len(answered_60s), len(outbound_calls), default=0.0)

        total_appts = len(c_appts)

        confirmed_appts = sum(
            1 for r in c_appts
            if _appt_status(r) in CONFIRMED_STATUSES
        )

        appt_rate = _safe_div(confirmed_appts, total_leads)

        shows = sum(1 for r in c_appts if _appt_status(r) == "Show - Not Sold")
        show_rate = _safe_div(shows, confirmed_appts)

        metrics[client] = {
            "total_leads":     total_leads,
            "total_calls":     total_calls,
            "answer_rate":     answer_rate,
            "total_appts":     total_appts,
            "confirmed_appts": confirmed_appts,
            "appt_rate":       appt_rate,
            "show_rate":       show_rate,
        }

    return metrics


def _call_duration(record: dict) -> float:
    raw = record.get("fields", {}).get("Call Duration")
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0
