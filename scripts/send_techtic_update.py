import os
import requests
from collections import defaultdict

REDASH_URL        = "https://redash.springworks.in"
QUERY_ID          = 1994
PER_QUERY_API_KEY = "UTxJGMzhatseXiRDnvKum6cybkFb4u3gm9EwOOHD"
REDASH_REPORT_URL = f"{REDASH_URL}/queries/{QUERY_ID}#2782"

_raw_token    = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN   = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"
SLACK_CHANNEL = "C0AGRE19V6U"   # #testing-sefali

# Slack user IDs to CC
CC_USERS = [
    "<@U026K56UJQL>",   # Anjana
    "<@U06UBBS4QHJ>",   # Armaan
    "<@UURRMS3MG>",     # Shalini
    "<@U03GURJS6SZ>",   # Anusha Kumari
]

# Severity bucket order
SEV_ORDER = ["0-1", "2 - 3", "4 - 5", "6 - 7", "8 - 14", "15 - 30", "31 - 90", "90+"]


def fetch_results():
    url = f"{REDASH_URL}/api/queries/{QUERY_ID}/results.json"
    resp = requests.get(url, params={"api_key": PER_QUERY_API_KEY}, timeout=60)
    print(f"GET {url} → {resp.status_code}")
    resp.raise_for_status()
    rows = resp.json()["query_result"]["data"]["rows"]
    print(f"Retrieved {len(rows)} rows from Redash")
    return rows


def build_pivot(rows):
    """Build pivot: New Severity (rows) x Check Name (cols), count."""
    # pivot[severity][check_name] = count
    pivot = defaultdict(lambda: defaultdict(int))

    for row in rows:
        sev   = row.get("New Severity") or "0-1"
        check = row.get("Check Name") or "Unknown"
        pivot[sev][check] += 1

    return pivot


def format_pivot_table(pivot):
    checks = sorted(set(c for sev_data in pivot.values() for c in sev_data))
    sevs   = [s for s in SEV_ORDER if s in pivot]

    # Column widths
    sev_w   = 10
    col_w   = max(12, max(len(c) for c in checks) + 2)
    tot_w   = 8

    header = f"{'Severity':<{sev_w}}" + "".join(f"{c:>{col_w}}" for c in checks) + f"{'Total':>{tot_w}}"
    sep    = "-" * len(header)

    lines = ["```", header, sep]
    grand_total = 0

    for sev in sevs:
        row_total = sum(pivot[sev].values())
        grand_total += row_total
        line = f"{sev:<{sev_w}}" + "".join(
            f"{pivot[sev].get(c, '-'):>{col_w}}" for c in checks
        ) + f"{row_total:>{tot_w}}"
        lines.append(line)

    lines.append(sep)
    totals = f"{'Total':<{sev_w}}" + "".join(
        f"{sum(pivot[s].get(c, 0) for s in sevs):>{col_w}}" for c in checks
    ) + f"{grand_total:>{tot_w}}"
    lines.append(totals)
    lines.append("```")

    return "\n".join(lines), grand_total


def build_message(rows):
    pivot = build_pivot(rows)
    table, total = format_pivot_table(pivot)

    cc = " ".join(CC_USERS)

    message = (
        f"*Update on Techtic client In Progress checks*\n\n"
        f"{table}\n\n"
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
