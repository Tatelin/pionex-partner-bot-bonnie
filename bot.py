#!/usr/bin/env python3
"""Pionex Partner KOL 查詢 Telegram Bot (Bonnie 版).

支援用「兩個 Pionex Open-API Bot ID」分別查 KOL 與資產門檻：
  PIONEX_BOT_ID_KOL    — Min Balance 0，純 KOL 鏈路檢查
  PIONEX_BOT_ID_ASSET  — Min Balance N USDT，KOL + 資產達標檢查
  PIONEX_BOT_ID        — /check 與純 UID 訊息的預設 bot；未設定時自動 fallback 到 _KOL

支援指令：
  /kol <UID>                       純 KOL 檢查（短回覆）
  /asset <UID>                     KOL + 資產達標（短回覆）
  /check <UID>                     完整報告：KOL 狀態 + 近 30 天交易量
  /check <UID> <days>              改成近 N 天
  /check <UID> <start> <end>       自訂日期區間 (YYYY-MM-DD)
  /help, /start
"""
from __future__ import annotations

import asyncio
import getpass
import hashlib
import hmac
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

PIONEX_BASE = "https://api.pionex.com"
UID_RE = re.compile(r"^\d{8}$")
UID_TOKEN_RE = re.compile(r"(?<!\d)\d{8}(?!\d)")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DEFAULT_DAYS = 30
MAX_DAYS = 365
MAX_BATCH_UIDS = 20
STAT_CACHE_TTL = 300  # 5 分鐘，避免短時間內重複拉同一區間
HTTP_TIMEOUT = 15
ENV_FILE = Path(__file__).resolve().parent / ".env"

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
# httpx 預設 INFO 會把含 token 的 URL 印出來，壓成 WARNING
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("pionex-partner-bot")


# ---------------------------------------------------------------------------
# Pionex API client
# ---------------------------------------------------------------------------


@dataclass
class PionexCreds:
    api_key: str
    api_secret: str


class PionexError(Exception):
    pass


class PionexClient:
    MAX_CONCURRENT = 3  # 每帳號同時最多 N 個 API 呼叫，避免 Pionex rate limit

    def __init__(self, creds: PionexCreds):
        self.creds = creds
        self._stat_cache: dict[tuple[str, str], tuple[float, list[dict[str, Any]]]] = {}
        self._semaphore: asyncio.Semaphore | None = None

    def _get_semaphore(self) -> asyncio.Semaphore:
        # 延遲到當前 event loop 才建立 semaphore（lazy init）
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        return self._semaphore

    async def check_qualification_async(
        self, uid: str, bot_id: str
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        async with self._get_semaphore():
            return await loop.run_in_executor(
                None, self.check_qualification, uid, bot_id
            )

    async def trade_volume_for_uid_async(
        self, uid: str, start: str, end: str
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        async with self._get_semaphore():
            return await loop.run_in_executor(
                None, self.trade_volume_for_uid, uid, start, end
            )
        self._qual_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}

    def _signed_get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        params = dict(params)
        params["timestamp"] = str(int(time.time() * 1000))
        sorted_items = sorted(params.items(), key=lambda kv: kv[0])
        query = urlencode(sorted_items)
        # Pionex 官方 SDK 的簽章格式：METHOD + PATH + "?" + sortedQueryString
        # （timestamp 已包含在 sortedQueryString 中，不需另外 append）
        payload = f"GET{path}?{query}"
        sig = hmac.new(
            self.creds.api_secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "PIONEX-KEY": self.creds.api_key,
            "PIONEX-SIGNATURE": sig,
        }
        url = f"{PIONEX_BASE}{path}?{query}"
        log.info("→ GET %s   payload-to-sign=%r", path, payload)
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        log.info("← %s [%d] %s", path, r.status_code, r.text[:400])
        if r.status_code != 200:
            raise PionexError(
                f"HTTP {r.status_code} on {path}: {r.text[:400]}"
            )
        try:
            data = r.json()
        except ValueError as e:
            raise PionexError(f"Invalid JSON from {path}: {r.text[:400]}") from e
        return data

    def check_qualification(self, uid: str, bot_id: str) -> dict[str, Any]:
        # 短期快取：同 (uid, bot_id) 30 秒內重複查直接走快取
        cache_key = (uid, bot_id)
        now = time.time()
        cached = self._qual_cache.get(cache_key)
        if cached and now - cached[0] < 30:
            log.info("cache hit checkQualification uid=%s bot_id=%s", uid, bot_id[:8])
            return cached[1]
        resp = self._signed_get(
            "/api/v1/partner/kol/checkQualification",
            {"uid": uid, "bot_id": bot_id},
        )
        self._qual_cache[cache_key] = (now, resp)
        return resp

    def invite_trade_stat(self, start: str, end: str) -> list[dict[str, Any]]:
        cache_key = (start, end)
        now = time.time()
        if cache_key in self._stat_cache:
            ts, rows = self._stat_cache[cache_key]
            if now - ts < STAT_CACHE_TTL:
                log.info("cache hit inviteTradeStat %s..%s", start, end)
                return rows
        resp = self._signed_get(
            "/api/v1/partner/kol/inviteTradeStat",
            {"startTime": start, "endTime": end},
        )
        rows = _extract_rows(resp)
        self._stat_cache[cache_key] = (now, rows)
        return rows

    def trade_volume_for_uid(
        self, uid: str, start: str, end: str
    ) -> dict[str, Any]:
        rows = self.invite_trade_stat(start, end)
        matched = [r for r in rows if str(r.get("uid")) == str(uid)]
        spot = sum(_to_float(r.get("spotTradeAmount")) for r in matched)
        perp = sum(_to_float(r.get("perpTradeAmount")) for r in matched)
        return {
            "appeared_in_invite_list": bool(matched),
            "rows": matched,
            "spot": spot,
            "perp": perp,
            "total": spot + perp,
        }


def _extract_rows(resp: Any) -> list[dict[str, Any]]:
    """Pionex 各 API 包裝風格不一致，這裡盡量寬鬆地把列表拉出來。"""
    if isinstance(resp, list):
        return resp
    if not isinstance(resp, dict):
        return []
    for key in ("data", "rows", "list"):
        val = resp.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            inner = _extract_rows(val)
            if inner:
                return inner
    return []


def _extract_qualified(qual: Any) -> tuple[bool | None, str]:
    """從 checkQualification 回應抓「是否合格」與失敗原因說明。

    回傳 (qualified, reason)。qualified=None 表示無法判斷。
    """
    if not isinstance(qual, dict):
        return None, ""
    # Pionex 慣例：頂層 result 通常是 API 呼叫成功與否，真正資料在 data
    data = qual.get("data")
    result_top = qual.get("result")
    candidates: list[Any] = []
    if isinstance(data, dict):
        candidates.append(data.get("qualified"))
        candidates.append(data.get("isQualified"))
        candidates.append(data.get("result"))
    candidates.append(qual.get("qualified"))
    candidates.append(qual.get("isQualified"))
    qualified: bool | None = None
    for c in candidates:
        if isinstance(c, bool):
            qualified = c
            break
    # 若沒抓到內層 qualified，但頂層 result 是 bool，退而求其次
    if qualified is None and isinstance(result_top, bool):
        qualified = result_top
    reason = ""
    for k in ("message", "msg", "reason", "errorMessage"):
        v = qual.get(k)
        if isinstance(v, str) and v:
            reason = v
            break
        if isinstance(data, dict):
            v = data.get(k)
            if isinstance(v, str) and v:
                reason = v
                break
    return qualified, reason


def _to_float(v: Any) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# 多帳號（一個 KOL 有多個推薦碼/帳號時用）
# ---------------------------------------------------------------------------


@dataclass
class Account:
    label: str               # 顯示用 (例 "bonnie")
    client: PionexClient
    bot_id_kol: str | None        # 純 KOL 檢查用 (Min Balance 0)
    bot_id_asset: str | None      # 低門檻資產（例 200 USDT）
    bot_id_asset_high: str | None  # 高門檻資產（例 1000 USDT）

    @property
    def bot_id_default(self) -> str | None:
        return self.bot_id_kol or self.bot_id_asset or self.bot_id_asset_high


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


def parse_date_args(args: list[str]) -> tuple[str, str, str]:
    """從 /check 後的參數 (扣掉 uid) 解析出 (start, end, label)。"""
    today = date.today()
    if not args:
        start = today - timedelta(days=DEFAULT_DAYS)
        return start.isoformat(), today.isoformat(), f"近 {DEFAULT_DAYS} 天"
    if len(args) == 1:
        try:
            days = int(args[0])
        except ValueError:
            raise ValueError("天數必須是整數，或改用 <start> <end> 日期")
        if days <= 0 or days > MAX_DAYS:
            raise ValueError(f"天數需在 1–{MAX_DAYS} 之間")
        start = today - timedelta(days=days)
        return start.isoformat(), today.isoformat(), f"近 {days} 天"
    if len(args) == 2:
        s, e = args[0], args[1]
        if not (DATE_RE.match(s) and DATE_RE.match(e)):
            raise ValueError("日期格式請用 YYYY-MM-DD")
        if s > e:
            raise ValueError("起始日不可晚於結束日")
        return s, e, f"{s} ~ {e}"
    raise ValueError(
        "參數過多。用法：/check <uid> [days] 或 /check <uid> <start> <end>"
    )


# ---------------------------------------------------------------------------
# Telegram bot
# ---------------------------------------------------------------------------


HELP_TEXT = (
    "🤖 <b>Bonnie KOL 查詢機器人</b>\n\n"
    "<b>快速查詢（短回覆）</b>\n"
    "• <code>/kol &lt;UID&gt;</code> — 是否為 Bonnie 邀請的 KOL 用戶\n"
    "• <code>/asset &lt;UID&gt;</code> — 資產達到的最高門檻（200 / 1000 USDT）\n\n"
    "<b>完整報告（含交易量）</b>\n"
    "• 直接貼 UID：<code>12345678</code> — 近 30 天\n"
    "• 一次最多 20 個（空白、逗號或換行分隔）\n"
    "• <code>/check &lt;UID&gt; &lt;天數&gt;</code> — 例：<code>/check 12345678 7</code>\n"
    "• <code>/check &lt;UID&gt; &lt;YYYY-MM-DD&gt; &lt;YYYY-MM-DD&gt;</code> — 自訂區間\n\n"
    "UID 為 8 位純數字。資產判定會自動顯示達到的最高門檻。"
)


class TGBot:
    def __init__(
        self,
        accounts: list[Account],
        allowed_chat_ids: set[int],
        asset_threshold_label: str | None,
        asset_high_threshold_label: str | None = None,
    ):
        assert accounts, "至少要有一個帳號"
        self.accounts = accounts
        self.allowed_chat_ids = allowed_chat_ids
        # 例 "200 USDT" / "1000 USDT"；純文字 label，無 API 行為
        self.asset_threshold_label = asset_threshold_label or ""
        self.asset_high_threshold_label = asset_high_threshold_label or ""

    def _is_allowed(self, update: Update) -> bool:
        chat = update.effective_chat
        if chat is None:
            return False
        return chat.id in self.allowed_chat_ids

    def _reject_log(self, update: Update, cmd: str) -> None:
        chat = update.effective_chat
        user = update.effective_user
        log.info(
            "rejected %s from chat_id=%s type=%s title=%r user=%s(%s)",
            cmd,
            chat.id if chat else None,
            chat.type if chat else None,
            chat.title if chat else None,
            user.username if user else None,
            user.id if user else None,
        )

    async def cmd_start(self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            self._reject_log(update, "/start")
            return
        await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)

    async def cmd_help(self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            self._reject_log(update, "/help")
            return
        await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------

    async def cmd_check(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            self._reject_log(update, "/check")
            return
        args = ctx.args or []
        if not args:
            await update.message.reply_text(
                "請輸入 UID。\n用法：/check <UID> [days] 或 /check <UID> <start> <end>"
            )
            return
        uid = args[0]
        if not UID_RE.match(uid):
            await update.message.reply_text("❌ UID 必須為 8 位純數字")
            return
        try:
            start, end, label = parse_date_args(args[1:])
        except ValueError as e:
            await update.message.reply_text(f"❌ {e}")
            return

        thinking = await update.message.reply_text(
            f"🔍 查詢 UID <code>{uid}</code>（{label}）…",
            parse_mode=ParseMode.HTML,
        )
        per_acct = await self._check_full_all(uid, start, end)
        await thinking.edit_text(
            format_full_result(
                uid, label, per_acct,
                self.asset_threshold_label,
                self.asset_high_threshold_label,
            ),
            parse_mode=ParseMode.HTML,
        )

    async def cmd_kol(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            self._reject_log(update, "/kol")
            return
        if not any(a.bot_id_kol for a in self.accounts):
            await update.message.reply_text(
                "❌ 沒有任何帳號設定 PIONEX_*_BOT_ID_KOL，無法使用 /kol"
            )
            return
        args = ctx.args or []
        if not args or not UID_RE.match(args[0]):
            await update.message.reply_text("用法：/kol <UID>（UID 為 8 位純數字）")
            return
        uid = args[0]
        thinking = await update.message.reply_text(
            f"🔍 KOL 檢查 <code>{uid}</code>…", parse_mode=ParseMode.HTML
        )
        per_acct = await self._check_qual_all(uid, "kol")
        await thinking.edit_text(
            format_short_qual_multi(uid, per_acct, mode="kol"),
            parse_mode=ParseMode.HTML,
        )

    async def cmd_asset(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            self._reject_log(update, "/asset")
            return
        has_any = any(
            a.bot_id_asset or a.bot_id_asset_high for a in self.accounts
        )
        if not has_any:
            await update.message.reply_text(
                "❌ 沒有任何帳號設定 PIONEX_*_BOT_ID_ASSET[_HIGH]，無法使用 /asset"
            )
            return
        args = ctx.args or []
        if not args or not UID_RE.match(args[0]):
            await update.message.reply_text("用法：/asset <UID>（UID 為 8 位純數字）")
            return
        uid = args[0]
        thinking = await update.message.reply_text(
            f"🔍 資產門檻檢查 <code>{uid}</code>…", parse_mode=ParseMode.HTML
        )
        low_results, high_results = await asyncio.gather(
            self._check_qual_all(uid, "asset_low"),
            self._check_qual_all(uid, "asset_high"),
        )
        await thinking.edit_text(
            format_asset_tier_multi(
                uid,
                low_results,
                high_results,
                self.asset_threshold_label,
                self.asset_high_threshold_label,
            ),
            parse_mode=ParseMode.HTML,
        )

    async def on_text(self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
        """Plain message handler: extract 8-digit UIDs and full-check (default 30d)."""
        if not self._is_allowed(update):
            return
        msg = update.message
        if not msg or not msg.text:
            return
        uids: list[str] = []
        seen: set[str] = set()
        for m in UID_TOKEN_RE.finditer(msg.text):
            u = m.group(0)
            if u not in seen:
                seen.add(u)
                uids.append(u)
        if not uids:
            return  # 不是 UID 訊息，安靜忽略
        if len(uids) > MAX_BATCH_UIDS:
            await msg.reply_text(
                f"⚠️ 一次最多 {MAX_BATCH_UIDS} 個 UID（這則訊息有 {len(uids)} 個）"
            )
            return

        start, end, label = parse_date_args([])  # default 30 days
        if len(uids) == 1:
            thinking = await msg.reply_text(
                f"🔍 查詢 UID <code>{uids[0]}</code>（{label}）…",
                parse_mode=ParseMode.HTML,
            )
            per_acct = await self._check_full_all(uids[0], start, end)
            await thinking.edit_text(
                format_full_result(
                    uids[0], label, per_acct,
                    self.asset_threshold_label,
                    self.asset_high_threshold_label,
                ),
                parse_mode=ParseMode.HTML,
            )
        else:
            thinking = await msg.reply_text(
                f"🔍 批次查詢 {len(uids)} 個 UID（{label}）…"
            )
            all_results = await asyncio.gather(
                *(self._check_full_all(u, start, end) for u in uids)
            )
            items = list(zip(uids, all_results))
            await thinking.edit_text(
                format_batch_multi(
                    label, items,
                    self.asset_threshold_label,
                    self.asset_high_threshold_label,
                ),
                parse_mode=ParseMode.HTML,
            )

    # ------------------------------------------------------------------
    # Helpers — fan-out across accounts
    # ------------------------------------------------------------------

    async def _check_qual_all(
        self, uid: str, mode: str
    ) -> list[tuple[Account, Any]]:
        """對所有帳號平行呼叫 checkQualification。

        mode = 'kol' | 'asset_low' | 'asset_high'
        """
        def pick_bot(a: Account) -> str | None:
            if mode == "kol":
                return a.bot_id_kol
            if mode == "asset_low":
                return a.bot_id_asset
            if mode == "asset_high":
                return a.bot_id_asset_high
            return None

        async def one(a: Account) -> tuple[Account, Any]:
            bot_id = pick_bot(a)
            if not bot_id:
                return a, {"_skip": True}
            try:
                qual = await a.client.check_qualification_async(uid, bot_id)
            except Exception as e:
                log.exception("checkQualification failed account=%s uid=%s", a.label, uid)
                qual = {"_error": str(e)}
            return a, qual
        return await asyncio.gather(*(one(a) for a in self.accounts))

    async def _check_full_all(
        self, uid: str, start: str, end: str
    ) -> list[tuple[Account, Any, Any, Any, Any]]:
        """對所有帳號做 KOL + 低門檻 + 高門檻 + 交易量。

        Returns list of (account, kol_qual, asset_low_qual, asset_high_qual, vol).
        每個 PionexClient 內部 semaphore 會自動節流，避免 burst 過大造成 rate limit。
        """

        async def call_qual(a: Account, bot_id: str | None) -> Any:
            if not bot_id:
                return {"_skip": True}
            try:
                return await a.client.check_qualification_async(uid, bot_id)
            except Exception as e:
                log.exception("checkQualification failed account=%s uid=%s", a.label, uid)
                return {"_error": str(e)}

        async def vol_call(a: Account) -> Any:
            try:
                return await a.client.trade_volume_for_uid_async(uid, start, end)
            except Exception as e:
                log.exception("inviteTradeStat failed account=%s", a.label)
                return {"_error": str(e)}

        async def one(a: Account) -> tuple[Account, Any, Any, Any, Any]:
            kol_q, low_q, high_q, vol = await asyncio.gather(
                call_qual(a, a.bot_id_kol),
                call_qual(a, a.bot_id_asset),
                call_qual(a, a.bot_id_asset_high),
                vol_call(a),
            )
            return a, kol_q, low_q, high_q, vol

        return await asyncio.gather(*(one(a) for a in self.accounts))


def _safe(s: str) -> str:
    return s.replace("<", "&lt;").replace(">", "&gt;")


def _qual_status(qual: Any) -> tuple[str, bool | None]:
    """回傳 (icon, qualified)；qualified=None 表示無法判斷 / 錯誤。"""
    if isinstance(qual, dict):
        if "_skip" in qual:
            return "—", None
        if "_error" in qual:
            return "⚠️", None
    qualified, _ = _extract_qualified(qual)
    if qualified is True:
        return "✅", True
    if qualified is False:
        return "❌", False
    return "⚠️", None


def _asset_tier_for(
    low_q: Any, high_q: Any
) -> str:
    """回傳 "high" / "low" / "none" / "unknown"，給單一帳號的兩個 qual 判斷。"""
    high_ok = _qual_status(high_q)[1] is True
    low_ok = _qual_status(low_q)[1] is True
    if high_ok:
        return "high"
    if low_ok:
        return "low"
    # 都是 False 才算 none；若都 unknown / skip 則回 unknown
    high_judged = _qual_status(high_q)[1] is not None
    low_judged = _qual_status(low_q)[1] is not None
    if high_judged or low_judged:
        return "none"
    return "unknown"


def _overall_asset_tier(
    pairs: list[tuple[Any, Any]],
) -> tuple[str, list[str]]:
    """跨帳號取最高 tier，回傳 (tier, hit_account_labels_for_that_tier).

    pairs = list of (low_q, high_q)；但因為要回 label，外層傳 (account, low, high) 比較順手——
    為了不重新設計，這裡只回 tier 字串，hit accounts 由呼叫端自己算。
    """
    tier_rank = {"high": 3, "low": 2, "none": 1, "unknown": 0}
    best = "unknown"
    for low_q, high_q in pairs:
        t = _asset_tier_for(low_q, high_q)
        if tier_rank[t] > tier_rank[best]:
            best = t
    return best, []


def format_asset_tier_multi(
    uid: str,
    low_results: list[tuple[Account, Any]],
    high_results: list[tuple[Account, Any]],
    low_label: str,
    high_label: str,
) -> str:
    """/asset 短回覆：顯示「達到的最高 tier」+ 各帳號明細。

    low_results / high_results 是同樣帳號順序，因此能 zip 配對。
    """
    # 對齊 by Account
    low_map = {id(a): q for a, q in low_results}
    high_map = {id(a): q for a, q in high_results}
    accounts = [a for a, _ in low_results]  # low/high 順序相同

    high_hits: list[str] = []
    low_hits: list[str] = []
    detail_lines: list[str] = []
    for a in accounts:
        lq = low_map.get(id(a), {"_skip": True})
        hq = high_map.get(id(a), {"_skip": True})
        tier = _asset_tier_for(lq, hq)
        if tier == "high":
            high_hits.append(a.label)
        elif tier == "low":
            low_hits.append(a.label)

        low_icon = _qual_status(lq)[0]
        high_icon = _qual_status(hq)[0]
        low_set = a.bot_id_asset is not None
        high_set = a.bot_id_asset_high is not None
        parts = []
        if low_set:
            parts.append(f"≥{low_label or '低門檻'} {low_icon}")
        if high_set:
            parts.append(f"≥{high_label or '高門檻'} {high_icon}")
        if not parts:
            parts.append("— (未設定 asset bot)")
        detail_lines.append(f"  • {a.label}：" + " / ".join(parts))

    # 整體判定（取最高 tier）
    if high_hits:
        header = f"✅ <b>達到高門檻</b>：資產 ≥ {high_label or '高門檻'}（{', '.join(high_hits)}）"
    elif low_hits:
        header = f"✅ <b>達到低門檻</b>：資產 ≥ {low_label or '低門檻'}（{', '.join(low_hits)}）"
    else:
        # 看是否至少有判讀
        any_judged = any(
            _qual_status(q)[1] is not None
            for _, q in (low_results + high_results)
        )
        if any_judged:
            header = "❌ <b>未達門檻</b>"
            if low_label:
                header += f"（資產 &lt; {low_label}）"
        else:
            header = "⚠️ 兩個帳號都無法判讀（請查 log）"

    return (
        f"<b>UID</b> <code>{uid}</code>\n"
        f"{header}\n\n"
        f"<i>各帳號明細</i>\n" + "\n".join(detail_lines)
    )


def format_short_qual_multi(
    uid: str,
    per_acct: list[tuple[Account, Any]],
    mode: str,
    threshold_label: str = "",
) -> str:
    """多帳號精簡 KOL/Asset 結果。"""
    # 整體判定：任一帳號 true → 整體 ✅
    overall_true = any(_qual_status(q)[1] is True for _, q in per_acct)
    has_any = any(
        _qual_status(q)[1] is not None for _, q in per_acct
    )

    if mode == "kol":
        if overall_true:
            hits = [a.label for a, q in per_acct if _qual_status(q)[1] is True]
            header = f"✅ <b>是</b> Bonnie 邀請的 KOL 用戶（{', '.join(hits)}）"
        elif has_any:
            header = "❌ <b>不是</b> Bonnie 任一帳號的 KOL 用戶"
        else:
            header = "⚠️ 兩個帳號都無法判讀，請查 log"
    else:  # asset
        th = f"資產 ≥ {threshold_label}" if threshold_label else "資產達標"
        if overall_true:
            hits = [a.label for a, q in per_acct if _qual_status(q)[1] is True]
            header = f"✅ <b>達標</b>：是 KOL 且{th}（{', '.join(hits)}）"
        elif has_any:
            header = (
                f"❌ <b>不達標</b>\n"
                f"<i>可能原因：非 KOL 用戶，或資產低於 {threshold_label or '門檻'}</i>"
            )
        else:
            header = "⚠️ 兩個帳號都無法判讀，請查 log"

    # 細項列
    detail_lines = []
    for a, q in per_acct:
        icon, _ = _qual_status(q)
        bot_id = a.bot_id_kol if mode == "kol" else a.bot_id_asset
        if not bot_id:
            detail_lines.append(f"  • {a.label}：— (未設定 bot)")
            continue
        reason = ""
        if isinstance(q, dict) and "_error" in q:
            reason = f" ({_safe(q['_error'])[:80]})"
        detail_lines.append(f"  • {a.label}：{icon}{reason}")

    return (
        f"<b>UID</b> <code>{uid}</code>\n"
        f"{header}\n\n"
        f"<i>各帳號明細</i>\n" + "\n".join(detail_lines)
    )


def _sum_vol(vols: list[Any]) -> dict[str, float] | None:
    """把多帳號的 vol 合計起來。全部都壞了就回 None。"""
    spot = perp = 0.0
    any_ok = False
    for v in vols:
        if isinstance(v, dict) and ("_error" in v or "_skip" in v):
            continue
        any_ok = True
        spot += _to_float(v.get("spot"))
        perp += _to_float(v.get("perp"))
    if not any_ok:
        return None
    return {"spot": spot, "perp": perp, "total": spot + perp}


def format_full_result(
    uid: str,
    label: str,
    per_acct: list[tuple[Account, Any, Any, Any, Any]],
    threshold_label: str = "",
    high_threshold_label: str = "",
) -> str:
    """多帳號完整報告：KOL + Asset Tier + 各帳號交易量 + 合計。

    per_acct = list of (account, kol_qual, asset_low_qual, asset_high_qual, vol)
    """
    # ----- KOL 整體判定 -----
    kol_true = any(_qual_status(kq)[1] is True for _, kq, _, _, _ in per_acct)
    kol_any = any(_qual_status(kq)[1] is not None for _, kq, _, _, _ in per_acct)
    if kol_true:
        hits = [a.label for a, kq, _, _, _ in per_acct if _qual_status(kq)[1] is True]
        kol_line = f"✅ 是 KOL 用戶（{', '.join(hits)}）"
    elif kol_any:
        kol_line = "❌ 非任一帳號的 KOL 用戶"
    else:
        kol_line = "⚠️ 無法判讀（請查 log）"

    # ----- Asset Tier 整體判定（取最高） -----
    high_hits: list[str] = []
    low_only_hits: list[str] = []
    any_asset_judged = False
    for a, _kq, lq, hq, _v in per_acct:
        tier = _asset_tier_for(lq, hq)
        if tier == "high":
            high_hits.append(a.label)
        elif tier == "low":
            low_only_hits.append(a.label)
        if tier != "unknown":
            any_asset_judged = True

    if high_hits:
        asset_line = (
            f"✅ <b>資產 ≥ {high_threshold_label or '高門檻'}</b>"
            f"（{', '.join(high_hits)}）"
        )
    elif low_only_hits:
        asset_line = (
            f"✅ <b>資產 ≥ {threshold_label or '低門檻'}</b>"
            f"（{', '.join(low_only_hits)}）"
        )
    elif any_asset_judged:
        asset_line = (
            f"❌ 未達門檻"
            + (f"（&lt; {threshold_label}）" if threshold_label else "")
        )
    else:
        asset_line = "⚠️ 無法判讀（請查 log）"

    # ----- 各帳號交易量明細 + 合計 -----
    detail_lines = []
    vols = []
    for a, kq, lq, hq, v in per_acct:
        kol_icon = _qual_status(kq)[0]
        # asset 顯示「達到的最高 tier icon」：以 high → low 順序找有設定且 ✓
        tier = _asset_tier_for(lq, hq)
        if tier == "high":
            asset_tag = f"Asset ✅ ≥{high_threshold_label or 'high'}"
        elif tier == "low":
            asset_tag = f"Asset ✅ ≥{threshold_label or 'low'}"
        elif tier == "none":
            asset_tag = "Asset ❌"
        else:
            asset_tag = "Asset ⚠️"

        if isinstance(v, dict) and "_skip" in v:
            detail_lines.append(
                f"  • {a.label}：KOL {kol_icon} / {asset_tag} ｜量: — (未設定 bot)"
            )
            vols.append(v)
            continue
        if isinstance(v, dict) and "_error" in v:
            detail_lines.append(
                f"  • {a.label}：KOL {kol_icon} / {asset_tag} ｜量: ⚠️ {_safe(v['_error'])[:60]}"
            )
            vols.append(v)
            continue
        in_list = v["appeared_in_invite_list"]
        marker = "✓" if in_list else "·"
        detail_lines.append(
            f"  • {a.label}：KOL {kol_icon} / {asset_tag} {marker}"
            f" ｜Spot <code>{v['spot']:,.0f}</code>"
            f" / Perp <code>{v['perp']:,.0f}</code>"
            f" / 合計 <code>{v['total']:,.0f}</code>"
        )
        vols.append(v)

    summed = _sum_vol(vols)
    if summed is None:
        sum_block = "⚠️ 無交易量資料"
    else:
        sum_block = (
            f"• 現貨 Spot：<code>{summed['spot']:,.2f}</code> USDT\n"
            f"• 合約 Perp：<code>{summed['perp']:,.2f}</code> USDT\n"
            f"• 合計 Total：<code>{summed['total']:,.2f}</code> USDT"
        )

    return (
        f"<b>UID</b> <code>{uid}</code>\n"
        f"<b>KOL</b>：{kol_line}\n"
        f"<b>Asset</b>：{asset_line}\n\n"
        f"<b>交易量（{label}）</b>\n"
        f"{sum_block}\n\n"
        f"<i>各帳號明細</i>\n" + "\n".join(detail_lines)
    )


def format_batch_multi(
    label: str,
    items: list[tuple[str, list[tuple[Account, Any, Any, Any, Any]]]],
    threshold_label: str = "",
    high_threshold_label: str = "",
) -> str:
    """批次查詢的精簡輸出：每個 UID 一行，含 KOL + Asset tier + 合計交易量。"""
    lines = [f"<b>區間</b>：{label}\n"]
    for uid, per_acct in items:
        kol_true = any(_qual_status(kq)[1] is True for _, kq, _, _, _ in per_acct)
        kol_any = any(_qual_status(kq)[1] is not None for _, kq, _, _, _ in per_acct)
        k_icon = "✅" if kol_true else ("❌" if kol_any else "⚠️")

        # asset tier across accounts
        best_tier = "unknown"
        tier_rank = {"high": 3, "low": 2, "none": 1, "unknown": 0}
        for _, _, lq, hq, _ in per_acct:
            t = _asset_tier_for(lq, hq)
            if tier_rank[t] > tier_rank[best_tier]:
                best_tier = t
        if best_tier == "high":
            a_str = f"Asset ✅≥{high_threshold_label or 'high'}"
        elif best_tier == "low":
            a_str = f"Asset ✅≥{threshold_label or 'low'}"
        elif best_tier == "none":
            a_str = "Asset ❌"
        else:
            a_str = "Asset ⚠️"

        summed = _sum_vol([v for _, _, _, _, v in per_acct])
        if summed is None:
            lines.append(f"<code>{uid}</code> — KOL {k_icon} {a_str} ｜量: ⚠️")
            continue
        lines.append(
            f"<code>{uid}</code> — KOL {k_icon} {a_str}"
            f" ｜Spot <code>{summed['spot']:,.0f}</code>"
            f" ｜Perp <code>{summed['perp']:,.0f}</code>"
            f" ｜合計 <code>{summed['total']:,.0f}</code>"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Interactive startup
# ---------------------------------------------------------------------------


def _load_env_file(path: Path) -> None:
    """簡易 .env 解析；不覆蓋已存在的環境變數。"""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = val


def _is_interactive() -> bool:
    """stdin 是否為終端機；雲端容器（Railway/Docker/systemd）通常為 False。"""
    try:
        return sys.stdin.isatty()
    except Exception:
        return False


def _ask(label: str, secret: bool = False) -> str:
    if not _is_interactive():
        env_hint = label.replace(" ", "_").upper()
        print(
            f"❌ 找不到必要設定『{label}』，且目前是無頭環境（無法互動式輸入）。\n"
            f"   請到 Railway / .env 把對應變數設好後再啟動。",
            file=sys.stderr,
        )
        sys.exit(1)
    while True:
        val = (getpass.getpass(f"{label}: ") if secret else input(f"{label}: ")).strip()
        if val:
            return val
        print(f"⚠️  {label} 不可空白，請重新輸入。")


def _parse_chat_ids(raw: str) -> set[int]:
    ids: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            raise ValueError(f"無法解析為整數：{part!r}")
    return ids


def _ask_chat_ids() -> set[int]:
    if not _is_interactive():
        print(
            "❌ 缺少 ALLOWED_CHAT_IDS，且目前是無頭環境（無法互動式輸入）。\n"
            "   請到 Railway 的 Variables 加上 ALLOWED_CHAT_IDS=<群組 chat_id>。",
            file=sys.stderr,
        )
        sys.exit(1)
    while True:
        raw = input(
            "允許的 TG 群組 chat_id（多個用逗號分隔；群組通常是負數，supergroup 以 -100 開頭）: "
        ).strip()
        try:
            ids = _parse_chat_ids(raw)
        except ValueError as e:
            print(f"⚠️  {e}，請重新輸入。")
            continue
        if not ids:
            print("⚠️  至少要提供一個 chat_id。")
            continue
        return ids


def _from_env_or_ask(env_key: str, label: str, secret: bool = False) -> str:
    val = os.environ.get(env_key, "").strip()
    if val:
        log.info("using %s from env/.env", env_key)
        return val
    return _ask(label, secret=secret)


# 從 .env 找所有 PIONEX_<SUFFIX>_API_KEY 形式的帳號設定
ACCOUNT_KEY_RE = re.compile(r"^PIONEX_([A-Za-z0-9]+)_API_KEY$")


def _discover_accounts() -> list[Account]:
    """掃 env 找所有 PIONEX_<SUFFIX>_API_KEY 帳號，組成 Account list。"""
    suffixes: list[str] = []
    for k in os.environ:
        m = ACCOUNT_KEY_RE.match(k)
        if m and os.environ.get(k, "").strip():
            suffixes.append(m.group(1))
    suffixes.sort()  # 穩定順序
    accounts: list[Account] = []
    for s in suffixes:
        key = os.environ.get(f"PIONEX_{s}_API_KEY", "").strip()
        sec = os.environ.get(f"PIONEX_{s}_API_SECRET", "").strip()
        if not key or not sec:
            log.warning("帳號 %s 缺少 API_KEY 或 API_SECRET，跳過", s)
            continue
        kol = os.environ.get(f"PIONEX_{s}_BOT_ID_KOL", "").strip() or None
        asset = os.environ.get(f"PIONEX_{s}_BOT_ID_ASSET", "").strip() or None
        asset_high = (
            os.environ.get(f"PIONEX_{s}_BOT_ID_ASSET_HIGH", "").strip() or None
        )
        label = os.environ.get(f"PIONEX_{s}_LABEL", "").strip() or f"帳號 {s}"
        if not (kol or asset or asset_high):
            log.warning(
                "帳號 %s 沒有 BOT_ID_KOL / _ASSET / _ASSET_HIGH，無法做任何查詢，跳過",
                s,
            )
            continue
        accounts.append(
            Account(
                label=label,
                client=PionexClient(PionexCreds(key, sec)),
                bot_id_kol=kol,
                bot_id_asset=asset,
                bot_id_asset_high=asset_high,
            )
        )
    return accounts


def main() -> None:
    _load_env_file(ENV_FILE)
    print("=" * 64)
    print("  Pionex Partner KOL 查詢 TG Bot（多帳號版）")
    print("=" * 64)
    if ENV_FILE.exists():
        print(f"📄 已讀取 {ENV_FILE.name}")

    accounts = _discover_accounts()
    if not accounts:
        print(
            "❌ 找不到任何 Pionex 帳號設定。\n"
            "   .env 至少要有一組 PIONEX_<SUFFIX>_API_KEY / _API_SECRET\n"
            "   並至少一個 _BOT_ID_KOL 或 _BOT_ID_ASSET。",
            file=sys.stderr,
        )
        sys.exit(1)

    tg_token = _from_env_or_ask("TELEGRAM_BOT_TOKEN", "Telegram Bot Token", secret=True)
    asset_threshold_label = os.environ.get("PIONEX_ASSET_THRESHOLD_LABEL", "").strip()
    asset_high_threshold_label = os.environ.get(
        "PIONEX_ASSET_HIGH_THRESHOLD_LABEL", ""
    ).strip()

    allowed_raw = os.environ.get("ALLOWED_CHAT_IDS", "").strip()
    if allowed_raw:
        try:
            allowed = _parse_chat_ids(allowed_raw)
        except ValueError as e:
            print(f"❌ ALLOWED_CHAT_IDS 格式錯誤：{e}", file=sys.stderr)
            sys.exit(1)
        if not allowed:
            print("❌ ALLOWED_CHAT_IDS 解析後為空", file=sys.stderr)
            sys.exit(1)
        log.info("using ALLOWED_CHAT_IDS from env/.env")
    else:
        allowed = _ask_chat_ids()

    print(f"\n✅ 載入 {len(accounts)} 個帳號：")
    for a in accounts:
        kol_tag = "KOL ✓" if a.bot_id_kol else "KOL ✗"
        low_tag = "ASSET ✓" if a.bot_id_asset else "ASSET ✗"
        high_tag = "ASSET_HIGH ✓" if a.bot_id_asset_high else "ASSET_HIGH ✗"
        print(f"   • {a.label}：{kol_tag} / {low_tag} / {high_tag}")
    print(f"   low threshold  = {asset_threshold_label or '(未設定)'}")
    print(f"   high threshold = {asset_high_threshold_label or '(未設定)'}")
    print(f"   監聽 {len(allowed)} 個 chat：{sorted(allowed)}")

    bot = TGBot(
        accounts=accounts,
        allowed_chat_ids=allowed,
        asset_threshold_label=asset_threshold_label,
        asset_high_threshold_label=asset_high_threshold_label,
    )

    app = Application.builder().token(tg_token).build()
    app.add_handler(CommandHandler("start", bot.cmd_start))
    app.add_handler(CommandHandler("help", bot.cmd_help))
    app.add_handler(CommandHandler("check", bot.cmd_check))
    app.add_handler(CommandHandler("kol", bot.cmd_kol))
    app.add_handler(CommandHandler("asset", bot.cmd_asset))
    # 群組裡直接打 UID 即可查詢（需在 BotFather /setprivacy 關閉 privacy，或將 bot 設為群組管理員）
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.on_text))

    print("\n🚀 Bot 啟動中（Ctrl+C 結束）…\n")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 已停止")
        sys.exit(0)
