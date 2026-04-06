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
]

SEV_ORDER = ["0-1", "2 - 3", "4 - 5", "6 - 7", "8 - 14", "15 - 30", "31 - 90", "90+"]

TAT_7_PLUS  = 7
TAT_12_PLUS = 12


def fetch_results():
    """Fetch fresh results (max_age=0 forces re-execution if cache is stale)."""
    url = f"{REDASH_URL}/api/queries/{QUERY_ID}/results.json"
    resp = requests.get(url, params={"api_key": PER_QUERY_API_KEY, "max_age": 0}, timeout=60)
    print(f"GET {url} → {resp.status_code}")
    resp.raise_for_status()
    data = resp.json()

    # If Redash queued a job instead of returning results directly
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


def build_pivot(rows):
    """pivot[severity][check|verif] = count"""
    pivot = defaultdict(lambda: defaultdict(int))
    for row in rows:
        sev   = row.get("New Severity") or "0-1"
        combo = f"{row.get('Check Name','?')} | {row.get('Verification Type','N/A')}"
        pivot[sev][combo] += 1
    return pivot


def format_pivot_table(pivot):
    combos = sorted(set(c for sev_data in pivot.values() for c in sev_data))
    sevs   = [s for s in SEV_ORDER if s in pivot]

    sev_w = 10
    col_w = max(10, max(len(c) for c in combos) + 2)
    tot_w = 8

    header = f"{'Severity':<{sev_w}}" + "".join(f"{c:>{col_w}}" for c in combos) + f"{'Total':>{tot_w}}"
    sep    = "-" * len(header)

    lines = ["```", header, sep]
    grand_total = 0

    for sev in sevs:
        row_total = sum(pivot[sev].values())
        grand_total += row_total
        cells = "".join(
            f"{pivot[sev].get(c, '-'):>{col_w}}" for c in combos
        )
        lines.append(f"{sev:<{sev_w}}{cells}{row_total:>{tot_w}}")

    lines.append(sep)
    col_totals = "".join(
        f"{sum(pivot[s].get(c, 0) for s in sevs):>{col_w}}" for c in combos
    )
    lines.append(f"{'Total':<{sev_w}}{col_totals}{grand_total:>{tot_w}}")
    lines.append("```")

    return "\n".join(lines), grand_total


def compute_crossed_days(rows):
    """Count rows per Check Name where NET TAT >= threshold."""
    counts_7  = defaultdict(int)
    counts_12 = defaultdict(int)
    for row in rows:
        check   = row.get("Check Name", "Unknown")
        net_tat = row.get("NET TAT")
        if net_tat is None:
            continue
        net_tat = float(net_tat)
        if net_tat >= TAT_7_PLUS:
            counts_7[check] += 1
        if net_tat >= TAT_12_PLUS:
            counts_12[check] += 1
    return counts_7, counts_12


def build_message(rows):
    pivot             = build_pivot(rows)
    table, total      = format_pivot_table(pivot)
    counts_7, counts_12 = compute_crossed_days(rows)

    # Bullet lines for crossed-days summary
    bullet_lines = []
    for check in sorted(set(list(counts_7.keys()) + list(counts_12.keys()))):
        c7  = counts_7.get(check, 0)
        c12 = counts_12.get(check, 0)
        if c7 > 0:
            bullet_lines.append(f"• {c7} checks has crossed 7+ days in {check}")
        if c12 > 0:
            bullet_lines.append(f"• {c12} checks has crossed 12+ days in {check}")

    bullets = "\n".join(bullet_lines) if bullet_lines else "• No checks have crossed 7+ days"
    cc      = " ".join(CC_USERS)

    message = (
        f"*Update on Techtic client In Progress checks*\n\n"
        f"{table}\n\n"
        f"{bullets}\n\n"
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
