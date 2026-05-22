#!/usr/bin/env python3
"""一次性診斷：查指定 UID 的合格狀態 + 區間交易量。

用法：.venv/bin/python diag.py <UID>
"""
from __future__ import annotations
import hashlib
import hmac
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode

import requests

ENV_FILE = Path(__file__).resolve().parent / ".env"
BASE = "https://api.pionex.com"


def load_env(p: Path):
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def signed_get(secret: str, key: str, path: str, params: dict) -> dict:
    params = dict(params)
    params["timestamp"] = str(int(time.time() * 1000))
    sorted_items = sorted(params.items(), key=lambda kv: kv[0])
    query = urlencode(sorted_items)
    payload = f"GET{path}?{query}"
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    r = requests.get(
        f"{BASE}{path}?{query}",
        headers={"PIONEX-KEY": key, "PIONEX-SIGNATURE": sig},
        timeout=15,
    )
    return r.json()


def main():
    if len(sys.argv) < 2:
        print("用法: diag.py <UID>")
        sys.exit(1)
    uid = sys.argv[1]
    load_env(ENV_FILE)
    key = os.environ["PIONEX_API_KEY"]
    secret = os.environ["PIONEX_API_SECRET"]
    bot_id = os.environ["PIONEX_BOT_ID"]

    print(f"=== checkQualification ({uid}) ===")
    q = signed_get(secret, key, "/api/v1/partner/kol/checkQualification",
                   {"uid": uid, "bot_id": bot_id})
    print(q)

    today = date.today()
    start = (today - timedelta(days=30)).isoformat()
    end = today.isoformat()
    print(f"\n=== inviteTradeStat ({start} ~ {end}) ===")
    s = signed_get(secret, key, "/api/v1/partner/kol/inviteTradeStat",
                   {"startTime": start, "endTime": end})
    data = s.get("data", {})
    total = data.get("total")
    lst = data.get("list") or []
    print(f"total reported: {total}")
    print(f"list length:    {len(lst)}")
    if lst:
        print(f"first row keys: {list(lst[0].keys())}")
    matches = [r for r in lst if str(r.get("uid")) == str(uid)]
    print(f"\n=== rows matching UID {uid} ===")
    if matches:
        for m in matches:
            print(m)
    else:
        print("(無)")
        print(f"first 5 uids in list: {[r.get('uid') for r in lst[:5]]}")


if __name__ == "__main__":
    main()
