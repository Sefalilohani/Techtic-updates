import os
import requests
from collections import defaultdict

REDASH_URL          = "https://redash.springworks.in"
QUERY_ID            = 1994
PER_QUERY_API_KEY   = "UTxJGMzhatseXiRDnvKum6cybkFb4u3gm9EwOOHD"

_raw_token    = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN   = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"
SLACK_CHANNEL = "C0AGRE19V6U"   # #testing-sefali

TAT_7_PLUS  = 7
TAT_12_PLUS = 12


def refresh_and_get_results():
    """Fetch latest cached results using the per-query API key."""
    url = f"{REDASH_URL}/api/queries/{QUERY_ID}/results.json"
    resp = requests.get(
        url,
        params={"api_key": PER_QUERY_API_KEY},
        timeout=60,
    )
    print(f"GET {url} → {resp.status_code}")
    resp.raise_for_status()
    rows = resp.json()["query_result"]["data"]["rows"]
    print(f"Retrieved {len(rows)} rows from Redash")
    return rows


def compute_counts(rows):
    counts_7plus  = defaultdict(int)
    counts_12plus = defaultdict(int)

    for row in rows:
        check_name = row.get("Check Name", "Unknown")
        net_tat    = row.get("NET TAT")

        if net_tat is None:
            continue
        net_tat = float(net_tat)

        if net_tat >= TAT_7_PLUS:
            counts_7plus[check_name] += 1
        if net_tat >= TAT_12_PLUS:
            counts_12plus[check_name] += 1

    return counts_7plus, counts_12plus


def build_message(counts_7plus, counts_12plus):
    all_checks = sorted(
        set(list(counts_7plus.keys()) + list(counts_12plus.keys()))
    )

    lines = ["*Update on Techtic client In Progress checks*\n"]

    for check in all_checks:
        c7  = counts_7plus.get(check, 0)
        c12 = counts_12plus.get(check, 0)
        if c7 > 0:
            lines.append(f"{c7} checks has crossed 7+ days in {check}")
        if c12 > 0:
            lines.append(f"{c12} checks has crossed 12+ days in {check}")

    if len(lines) == 1:
        lines.append("No checks have crossed 7+ days. All good!")

    return "\n".join(lines)


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
    rows = refresh_and_get_results()
    counts_7plus, counts_12plus = compute_counts(rows)
    message = build_message(counts_7plus, counts_12plus)
    print("--- Message preview ---")
    print(message)
    print("-----------------------")
    send_slack(message)
