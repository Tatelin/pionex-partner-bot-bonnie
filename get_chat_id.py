#!/usr/bin/env python3
"""一次性工具：用你自己的 TG bot token 從 getUpdates 撈出 chat_id。

用法：
  1. 確保 .env 裡的 TELEGRAM_BOT_TOKEN 已經填好
  2. 你的 bot 已經被加進目標群組
  3. 在群組裡隨便發一則訊息（讓 bot 看到一筆 update）
  4. 執行：.venv/bin/python get_chat_id.py
  5. 把列出來的群組 chat_id 填回 .env 的 ALLOWED_CHAT_IDS

注意：bot.py 還在跑的話，請先停掉，否則 getUpdates 會衝突。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import requests

ENV_FILE = Path(__file__).resolve().parent / ".env"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip("\"'")
        if k and k not in os.environ:
            os.environ[k] = v


def main() -> None:
    load_env(ENV_FILE)
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("❌ TELEGRAM_BOT_TOKEN 沒設。請先到 .env 填好。", file=sys.stderr)
        sys.exit(1)

    base = f"https://api.telegram.org/bot{token}"

    # 1) 確認 token 有效，順便印 bot 身分
    print("🔎 確認 bot 身分 (getMe) ...")
    try:
        me = requests.get(f"{base}/getMe", timeout=10).json()
    except requests.RequestException as e:
        print(f"❌ 網路錯誤：{e}", file=sys.stderr)
        sys.exit(1)
    if not me.get("ok"):
        print(f"❌ token 無效或 API 拒絕：{me}", file=sys.stderr)
        sys.exit(1)
    bot_info = me["result"]
    print(
        f"   ✓ bot @{bot_info.get('username')} (id={bot_info.get('id')})"
        f" can_read_all_group_messages={bot_info.get('can_read_all_group_messages')}"
    )
    if not bot_info.get("can_read_all_group_messages"):
        print(
            "   ⚠️ 這個 bot 在群組裡只看得到指令訊息！\n"
            "      去 @BotFather → /setprivacy → 選此 bot → 點 Disable，然後重新加進群組。"
        )

    # 2) 檢查有沒有設 webhook（會搶走 getUpdates）
    print("🔎 檢查 webhook 設定 (getWebhookInfo) ...")
    wh = requests.get(f"{base}/getWebhookInfo", timeout=10).json()
    wh_url = (wh.get("result") or {}).get("url", "")
    if wh_url:
        print(
            f"   ⚠️ 偵測到 webhook：{wh_url}\n"
            "      這會讓 getUpdates 永遠是空的。\n"
            "      要移除請執行：.venv/bin/python get_chat_id.py --delete-webhook"
        )
        if "--delete-webhook" in sys.argv:
            d = requests.get(f"{base}/deleteWebhook", timeout=10).json()
            print(f"   deleteWebhook 回應：{d}")
        else:
            sys.exit(1)
    else:
        print("   ✓ 沒有 webhook")

    # 3) 拉 updates
    print("📡 呼叫 Telegram getUpdates ...")
    try:
        r = requests.get(
            f"{base}/getUpdates",
            params={"timeout": 0, "limit": 100},
            timeout=15,
        )
    except requests.RequestException as e:
        print(f"❌ 網路錯誤：{e}", file=sys.stderr)
        sys.exit(1)

    if r.status_code != 200:
        print(f"❌ HTTP {r.status_code}: {r.text[:400]}", file=sys.stderr)
        sys.exit(1)

    data = r.json()
    if not data.get("ok"):
        print(f"❌ TG 回應錯誤：{data}", file=sys.stderr)
        sys.exit(1)

    updates = data.get("result", [])
    if not updates:
        print(
            "\nℹ️ getUpdates 沒回任何訊息。\n"
            "請確認：\n"
            "  1. bot 已加進目標群組\n"
            "  2. BotFather → /setprivacy → 該 bot 設為 Disable\n"
            "     （或把 bot 設為群組管理員）\n"
            "  3. 加入後，在群組裡發一則新訊息\n"
            "  4. 再執行一次本腳本\n"
        )
        sys.exit(0)

    seen: dict[int, dict[str, str | None]] = {}
    for u in updates:
        msg = (
            u.get("message")
            or u.get("edited_message")
            or u.get("channel_post")
            or u.get("my_chat_member")
            or {}
        )
        chat = msg.get("chat") if isinstance(msg, dict) else None
        if not isinstance(chat, dict):
            continue
        cid = chat.get("id")
        if cid is None or cid in seen:
            continue
        seen[cid] = {
            "type": chat.get("type"),
            "title": chat.get("title")
            or chat.get("username")
            or chat.get("first_name"),
        }

    if not seen:
        print("⚠️ 有 update 但抓不到 chat 資訊，原始資料如下：")
        print(data)
        sys.exit(0)

    print(f"\n🔍 偵測到 {len(seen)} 個 chat：\n")
    for cid, info in seen.items():
        is_group = info["type"] in ("group", "supergroup")
        marker = "👥 群組  " if is_group else "👤 私人  "
        print(
            f"  {marker} chat_id = {cid}   名稱：{info['title']}   type={info['type']}"
        )
    print(
        "\n👉 把目標群組的 chat_id（含負號）填到 .env 的 ALLOWED_CHAT_IDS"
    )


if __name__ == "__main__":
    main()
