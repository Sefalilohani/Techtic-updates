import os
import time
import requests
from collections import defaultdict

REDASH_URL        = "https://redash.springworks.in"
QUERY_ID          = 1994
PER_QUERY_API_KEY = "UTxJGMzhatseXiRDnvKum6cybkFb4u3gm9EwOOHD"
REDASH_REPORT_URL = f"{REDASH_URL}/queries/{QUERY_ID}#2782"

_raw_token    = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN   = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"
SLACK_CHANNEL = "C0AHW3CMNF3"   # #techtic-sv

CC_USERS = [
    "<@U026K56UJQL>",   # Anjana
    "<@U06UBBS4QHJ>",   # Armaan
    "<@UURRMS3MG>",     # Shalini
    "<@U03GURJS6SZ>",   # Anusha Kumari
    "<@U08FT9QD9U3>",    # Sristy Jaydhar
    "<@U08HF79BTSM>",    # Ritu Issrani
    "<@U092KHH1MSQ>",    # Udita ​Singh
]

SEV_ORDER = ["0-1", "2 - 3", "4 - 5", "6 - 7", "8 - 14", "15 - 30", "31 - 90", "90+"]

SEV_SHORT = {
    "0-1": "0-1", "2 - 3": "2-3", "4 - 5": "4-5", "6 - 7": "6-7",
    "8 - 14": "8-14", "15 - 30": "15-30", "31 - 90": "31-90", "90+": "90+",
}


def fetch_results():
    """Fetch fresh results (max_age=0 forces re-execution if cache is stale)."""
    url = f"{REDASH_URL}/api/queries/{QUERY_ID}/results.json"
    resp = requests.get(url, params={"api_key": PER_QUERY_API_KEY, "max_age": 0}, timeout=60)
    print(f"GET {url} → {resp.status_code}")
    resp.raise_for_status()
    data = resp.json()

    if "job" in data:
        job_id = data["job"]["id"]
        print(f"Query running (job {job_id}), polling…")
        for _ in range(60):
            time.sleep(5)
            poll = requests.get(
                url,
                params={"api_key": PER_QUERY_API_KEY, "max_age": 30},
                timeout=60,
            )
            poll_data = poll.json()
            if "query_result" in poll_data:
                rows = poll_data["query_result"]["data"]["rows"]
                print(f"Got {len(rows)} rows after polling")
                return rows
        raise RuntimeError("Timed out waiting for Redash result")

    rows = data["query_result"]["data"]["rows"]
    print(f"Retrieved {len(rows)} rows from Redash")
    return rows


# ── Table 1: Check Name | Verification Type × Severity ────────────────────────

def build_check_pivot(rows):
    pivot = defaultdict(lambda: defaultdict(int))
    for row in rows:
        sev   = row.get("New Severity") or "0-1"
        combo = f"{row.get('Check Name', '?')} | {row.get('Verification Type', 'N/A')}"
        pivot[sev][combo] += 1
    return pivot


def format_check_table(pivot):
    combos = sorted(set(c for sev_data in pivot.values() for c in sev_data))
    sevs   = [s for s in SEV_ORDER if s in pivot]

    def abbrev(combo):
        check, _, vtype = combo.partition(" | ")
        short_check = {
            "Universal Account Number Check":              "UAN Check",
            "Moonlighting Check":                          "Moonlighting",
            "University Recognition check":                "Univ Recognition",
            "Social Media Lite":                           "Social Media",
            "Police Clearance Certificate Acknowledgement":"PCC Acknowledgement",
            "Police Clearance Certificate":                "PCC",
        }.get(check, check)
        short_vtype = {
            "DIGITAL":                        "Digital",
            "PHYSICAL":                       "Physical",
            "OFFICIAL":                       "Official",
            "REGIONAL_PARTNER":               "Regional",
            "UNIVERSAL_ACCOUNT_NUMBER_CHECK": "UAN",
        }.get(vtype, "")
        return f"{short_check} ({short_vtype})" if short_vtype else short_check

    labels = [abbrev(c) for c in combos]
    lw = max(25, max(len(l) for l in labels) + 2)
    sw = 7
    tw = 7

    sev_hdrs = [SEV_SHORT.get(s, s) for s in sevs]
    header = f"{'Check':<{lw}}" + "".join(f"{h:>{sw}}" for h in sev_hdrs) + f"{'Total':>{tw}}"
    sep    = "-" * len(header)

    lines = ["```", header, sep]
    grand_total = 0

    for label, combo in zip(labels, combos):
        row_total = sum(pivot[s].get(combo, 0) for s in sevs)
        grand_total += row_total
        cells = "".join(f"{pivot[s].get(combo, 0) or '-':>{sw}}" for s in sevs)
        lines.append(f"{label:<{lw}}{cells}{row_total:>{tw}}")

    lines.append(sep)
    col_tots = "".join(
        f"{sum(pivot[s].get(c, 0) for c in combos):>{sw}}" for s in sevs
    )
    lines.append(f"{'Total':<{lw}}{col_tots}{grand_total:>{tw}}")
    lines.append("```")

    return "\n".join(lines), grand_total


# ── Table 2: Task Type × Severity ─────────────────────────────────────────────

def build_task_type_pivot(rows):
    pivot = defaultdict(lambda: defaultdict(int))
    for row in rows:
        sev       = row.get("New Severity") or "0-1"
        task_type = row.get("Task Type") or row.get("task_type") or "Unknown"
        pivot[sev][task_type] += 1
    return pivot


def format_task_type_table(pivot):
    task_types = sorted(set(t for sev_data in pivot.values() for t in sev_data))
    sevs       = [s for s in SEV_ORDER if s in pivot]

    lw = max(20, max(len(t) for t in task_types) + 2)
    sw = 7
    tw = 7

    sev_hdrs = [SEV_SHORT.get(s, s) for s in sevs]
    header = f"{'Task Type':<{lw}}" + "".join(f"{h:>{sw}}" for h in sev_hdrs) + f"{'Total':>{tw}}"
    sep    = "-" * len(header)

    lines = ["```", header, sep]
    grand_total = 0

    for task_type in task_types:
        row_total = sum(pivot[s].get(task_type, 0) for s in sevs)
        grand_total += row_total
        cells = "".join(f"{pivot[s].get(task_type, 0) or '-':>{sw}}" for s in sevs)
        lines.append(f"{task_type:<{lw}}{cells}{row_total:>{tw}}")

    lines.append(sep)
    col_tots = "".join(
        f"{sum(pivot[s].get(t, 0) for t in task_types):>{sw}}" for s in sevs
    )
    lines.append(f"{'Total':<{lw}}{col_tots}{grand_total:>{tw}}")
    lines.append("```")

    return "\n".join(lines)


# ── Message assembly ───────────────────────────────────────────────────────────

def build_message(rows):
    check_pivot              = build_check_pivot(rows)
    check_table, total       = format_check_table(check_pivot)

    task_pivot  = build_task_type_pivot(rows)
    task_table  = format_task_type_table(task_pivot)

    cc = " ".join(CC_USERS)

    message = (
        f"*Update on Techtic client In Progress checks*\n\n"
        f"*By Check Type*\n"
        f"{check_table}\n\n"
        f"*By Task Type*\n"
        f"{task_table}\n\n"
        f"*Total In-Progress checks: {total}*\n"
        f"<{REDASH_REPORT_URL}|View full report on Redash>\n\n"
        f"{cc}"
    )
    return message


def send_slack(message):
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        json={"channel": SLACK_CHANNEL, "text": message, "mrkdwn": True},
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    if not result.get("ok"):
        raise RuntimeError(f"Slack error: {result.get('error')}")
    print("Message sent to Slack successfully")


if __name__ == "__main__":
    rows    = fetch_results()
    message = build_message(rows)
    print("--- Message preview ---")
    print(message)
    print("-----------------------")
    send_slack(message)
