#!/usr/bin/env python3
import asyncio
import base64
import dataclasses
import datetime as dt
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import websockets

# ============ ENV / CONFIG ============

def _clean(s: Optional[str]) -> str:
    return (s or "").strip().strip('"').strip("'")

# BloFin auth
BLOFIN_API_KEY = _clean(os.getenv("BLOFIN_API_KEY"))
BLOFIN_API_SECRET = _clean(os.getenv("BLOFIN_API_SECRET"))
BLOFIN_PASSPHRASE = _clean(os.getenv("BLOFIN_PASSPHRASE"))

# Host: prod vs demo (you can also hard-override with BLOFIN_WS_URL)
IS_DEMO = _clean(os.getenv("BLOFIN_IS_DEMO")).lower() == "true"
HOST = "demo-trading-openapi.blofin.com" if IS_DEMO else "openapi.blofin.com"
DEFAULT_WS = f"wss://{HOST}/ws/private"
WS_URL = _clean(os.getenv("BLOFIN_WS_URL")) or DEFAULT_WS

# Discord
DISCORD_WEBHOOK = _clean(os.getenv("DISCORD_WEBHOOK_URL"))
SEND_MIN_INTERVAL = float(_clean(os.getenv("SEND_MIN_INTERVAL")) or "300")  # seconds
PERIOD_HOURS = float(_clean(os.getenv("PERIOD_HOURS")) or "6")              # hours

TABLE_TITLE = os.getenv("TABLE_TITLE", "Active Positions")
SIZE_EPS = float(_clean(os.getenv("SIZE_EPS")) or "1e-8")

# Notion
NOTION_API_TOKEN = _clean(os.getenv("NOTION_API_TOKEN"))
NOTION_DATABASE_ID = _clean(os.getenv("NOTION_DATABASE_ID"))
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

# Notion property names (match your DB)
P_TITLE     = os.getenv("NOTION_PROP_TITLE", "Trade Name")
P_STATUS    = os.getenv("NOTION_PROP_STATUS", "Status")
P_DIRECTION = os.getenv("NOTION_PROP_DIRECTION", "Direction")
P_LEVERAGE  = os.getenv("NOTION_PROP_LEVERAGE", "Leverage")
P_QTY       = os.getenv("NOTION_PROP_QTY", "Qty")
P_ENTRY     = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
P_EXIT      = os.getenv("NOTION_PROP_EXIT", "Exit Price")
P_MARKET    = os.getenv("NOTION_PROP_MARKET", "Market Price")

OPEN_LABEL   = os.getenv("NOTION_STATUS_OPEN_LABEL", "1.Live")
CLOSED_LABEL = os.getenv("NOTION_STATUS_CLOSED_LABEL", "3.Closed")

# ============ UTILS ============

def now_ts() -> float:
    return time.time()

def short(x: float, dec: int) -> float:
    return float(f"{x:.{dec}f}")

def fmt_signed_pct(pct: float, dec: int = 2) -> str:
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.{dec}f}%"

def _f(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def compute_base_qty(ev: Dict[str, Any]) -> Optional[float]:
    """
    Return base-asset units regardless of whether feed reports contracts or notional.
    - Prefer explicit base fields if present
    - Else sz * ctVal
    - Else notional / price
    """
    if not ev:
        return None

    # common base-size style keys
    for k in ("qty", "baseSz", "base_size", "sizeBase", "fillBase", "accFillSzBase"):
        v = _f(ev.get(k))
        if v is not None:
            return v

    # contracts -> base (ETH fix: sz * ctVal, e.g., 22 * 0.001 = 0.022 ETH)
    sz = _f(ev.get("pos") or ev.get("positions") or ev.get("size") or ev.get("sz") or ev.get("fillSz"))
    ct = _f(ev.get("ctVal") or ev.get("ctValCcy") or ev.get("ctv"))
    if (sz is not None) and (ct is not None) and ct > 0:
        return sz * ct

    # notional / price fallback
    notional = _f(ev.get("notional") or ev.get("notionalUsd") or ev.get("value"))
    px = _f(ev.get("avgPx") or ev.get("avgPrice") or ev.get("entryPrice") or ev.get("fillPx") or ev.get("markPx"))
    if (notional is not None) and (px is not None) and px > 0:
        return notional / px

    # final fallback: sometimes "pos" IS base units already
    if sz is not None and ct is None:
        return sz
    return None

def norm_symbol(inst: str) -> str:
    # For Notion title (ETH/USDT), but keep internal symbol with dash
    inst = (inst or "").upper().replace("_", "-")
    parts = inst.split("-")
    return f"{parts[0]}/{parts[1]}" if len(parts) == 2 else inst.replace("-", "/")

def side_label(raw: str) -> str:
    r = (raw or "").lower()
    if "short" in r or r == "sell":
        return "Short"
    return "Long"

# ============ NOTION ============

N_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

class Notion:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)

    async def ensure_ok(self):
        if not (NOTION_API_TOKEN and NOTION_DATABASE_ID):
            print("Notion missing token or database id.")
            return
        r = await self.client.get(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}", headers=N_HEADERS)
        try:
            r.raise_for_status()
            print("Notion DB schema detected.")
        except httpx.HTTPStatusError:
            print("Notion DB schema error:", r.status_code, r.text)
            raise

    async def query_open_pages(self) -> Dict[str, str]:
        """Return { 'ETH/USDT': page_id } for rows with Status == OPEN_LABEL."""
        payload = {
            "filter": {"property": P_STATUS, "select": {"equals": OPEN_LABEL}},
            "page_size": 100
        }
        r = await self.client.post(
            f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
            headers=N_HEADERS, json=payload
        )
        r.raise_for_status()
        out = {}
        for row in r.json().get("results", []):
            props = row.get("properties", {})
            t = props.get(P_TITLE, {}).get("title", [])
            title = "".join([x.get("plain_text", "") for x in t]) if t else ""
            if title:
                out[title] = row["id"]
        return out

    async def create_open(self, symbol: str, side: str, lev: float, qty: float, entry: float, mark: float):
        props = {
            P_TITLE: {"title": [{"text": {"content": norm_symbol(symbol)}}]},
            P_STATUS: {"select": {"name": OPEN_LABEL}},
            P_DIRECTION: {"select": {"name": side}},
            P_LEVERAGE: {"number": short(lev, 2)},
            P_QTY: {"number": short(qty, 8)},
            P_ENTRY: {"number": short(entry, 6)},
            P_MARKET: {"number": short(mark, 6)},
        }
        r = await self.client.post("https://api.notion.com/v1/pages", headers=N_HEADERS,
                                   json={"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props})
        if r.status_code >= 400:
            print("Notion create error:", r.status_code, r.text)
        r.raise_for_status()

    async def update_open(self, page_id: str, qty: float, entry: float, mark: float):
        props = {
            P_QTY: {"number": short(qty, 8)},
            P_ENTRY: {"number": short(entry, 6)},
            P_MARKET: {"number": short(mark, 6)},
        }
        r = await self.client.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=N_HEADERS,
                                    json={"properties": props})
        if r.status_code >= 400:
            print("Notion update error:", r.status_code, r.text)
        r.raise_for_status()

    async def close(self, page_id: str, exit_price: float):
        props = {
            P_STATUS: {"select": {"name": CLOSED_LABEL}},
            P_EXIT: {"number": short(exit_price, 6)},
        }
        r = await self.client.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=N_HEADERS,
                                    json={"properties": props})
        if r.status_code >= 400:
            print("Notion close error:", r.status_code, r.text)
        r.raise_for_status()

# ============ DISCORD ============

class Discord:
    def __init__(self, webhook: Optional[str]):
        self.webhook = webhook
        self.client = httpx.AsyncClient(timeout=30.0)
        self.last_send = 0.0

    async def send(self, content: str):
        if not self.webhook:
            return
        # simple cooldown
        if now_ts() - self.last_send < SEND_MIN_INTERVAL:
            pass
        while True:
            r = await self.client.post(self.webhook, json={"content": content})
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", "2"))
                await asyncio.sleep(max(1.0, retry_after))
                continue
            if r.status_code >= 400:
                print("Discord error:", r.status_code, r.text)
            r.raise_for_status()
            self.last_send = now_ts()
            break

def _table(rows: List[List[str]]) -> str:
    w = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = ["```"]
    for r in rows:
        lines.append("  ".join(s.ljust(w[i]) for i, s in enumerate(r)))
    lines.append("```")
    return "\n".join(lines)

def new_trades_block(newps: List[Dict[str, Any]]) -> str:
    if not newps: return ""
    rows = [["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]]
    for p in newps:
        sign = "+" if p["pnl_pct"] >= 0 else ""
        rows.append([
            p["symbol"],
            f'{p["side"]} {int(p["lev"])}x',
            f'{p["entry"]:.6f}',
            f'{p["pnl"]:.6f}',
            f'{sign}{abs(p["pnl_pct"]):.2f}%',
        ])
    return "**New Trades**\n" + _table(rows)

def positions_block(title: str, cur: List[Dict[str, Any]]) -> str:
    rows = [["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]]
    for p in cur:
        sign = "+" if p["pnl_pct"] >= 0 else ""
        rows.append([
            p["symbol"],
            f'{p["side"]} {int(p["lev"])}x',
            f'{p["entry"]:.6f}',
            f'{p["pnl"]:.6f}',
            f'{sign}{abs(p["pnl_pct"]):.2f}%',
        ])
    return f"**{title}**\n" + _table(rows)

# ============ POSITION MODEL ============

@dataclasses.dataclass
class Position:
    symbol: str
    side: str          # "Long"/"Short"
    lev: float
    qty: float         # base units
    entry: float
    mark: float
    pnl: float
    pnl_pct: float
    notion_page_id: Optional[str] = None

    def digest(self) -> Tuple:
        return (
            self.symbol,
            self.side,
            round(self.lev, 2),
            round(self.qty, 8),
            round(self.entry, 6),
            round(self.mark, 6),
            round(self.pnl, 6),
            round(self.pnl_pct, 2),
        )

# ============ BLOFIN WS HELPERS ============

def _ws_sign(ts_ms: str, nonce: str) -> str:
    """
    BloFin WS login signature:
    message = path + method + timestamp + nonce
    path = "/users/self/verify", method = "GET"
    HMAC-SHA256(secret, message) -> hex -> base64
    """
    msg = "/users/self/verify" + "GET" + ts_ms + nonce
    hex_sig = hmac.new(BLOFIN_API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    return base64.b64encode(hex_sig).decode()

def login_frame() -> Dict[str, Any]:
    ts_ms = str(int(time.time() * 1000))
    nonce = ts_ms
    return {
        "op": "login",
        "args": [{
            "apiKey": BLOFIN_API_KEY,
            "passphrase": BLOFIN_PASSPHRASE,
            "timestamp": ts_ms,
            "nonce": nonce,
            "sign": _ws_sign(ts_ms, nonce),
        }]
    }

def sub_positions_frame() -> Dict[str, Any]:
    return {"op": "subscribe", "args": [{"channel": "positions"}]}

def sub_orders_frame() -> Dict[str, Any]:
    return {"op": "subscribe", "args": [{"channel": "orders"}]}

# ============ CORE RUNNER ============

class Runner:
    def __init__(self):
        self.notion = Notion()
        self.discord = Discord(DISCORD_WEBHOOK)
        self.open_pages: Dict[str, str] = {}   # Notion title -> page_id
        self.positions: Dict[str, Position] = {}
        self.prev_digest: Tuple = tuple()
        self.next_summary_at = now_ts() + PERIOD_HOURS * 3600
        self.buffer_new: List[Dict[str, Any]] = []

    async def init_notion(self):
        await self.notion.ensure_ok()
        # preload open ids so we don't dupe
        self.open_pages = await self.notion.query_open_pages()

    def snapshot_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for p in sorted(self.positions.values(), key=lambda x: x.symbol):
            rows.append({
                "symbol": p.symbol,
                "side": p.side,
                "lev": p.lev,
                "entry": p.entry,
                "pnl": p.pnl,
                "pnl_pct": p.pnl_pct,
            })
        return rows

    async def maybe_post(self, force: bool = False):
        tick = now_ts()
        digest = tuple(p.digest() for p in sorted(self.positions.values(), key=lambda x: x.symbol))
        changed = (digest != self.prev_digest)
        due = tick >= self.next_summary_at

        if force or changed or due:
            blocks = []
            if self.buffer_new:
                blocks.append(new_trades_block(self.buffer_new))
                self.buffer_new.clear()
            blocks.append(positions_block(TABLE_TITLE, self.snapshot_rows()))
            await self.discord.send("\n\n".join(blocks))
            self.prev_digest = digest
            self.next_summary_at = now_ts() + PERIOD_HOURS * 3600

    async def on_open_or_update(self, ev: Dict[str, Any]):
        symbol = (ev.get("instId") or ev.get("symbol") or "").upper().replace("_", "-")
        if not symbol:
            return
        side = side_label(ev.get("posSide") or ev.get("side") or "long")
        lev = _f(ev.get("lever") or ev.get("leverage") or 0) or 0.0
        entry = _f(ev.get("avgPx") or ev.get("avgPrice") or ev.get("entryPrice")) or 0.0
        mark  = _f(ev.get("markPx") or ev.get("markPrice") or ev.get("px") or ev.get("last")) or entry
        qty   = compute_base_qty(ev) or 0.0

        # If size is ~0, treat as close
        if qty <= SIZE_EPS:
            await self.on_close({"instId": symbol, "markPx": mark})
            return

        # PnL: use feed if present, else compute
        pnl = _f(ev.get("unrealizedPnl") or ev.get("uPnL") or ev.get("pnl"))
        pnl_ratio = _f(ev.get("unrealizedPnlRatio") or ev.get("uPnLRatio") or ev.get("pnlRatio"))
        if pnl is None or pnl_ratio is None:
            # approximate pnl in quote
            if side == "Long":
                pnl = (mark - entry) * qty
            else:
                pnl = (entry - mark) * qty
            notional = max(1e-12, entry * qty)
            pnl_ratio = (pnl / notional)

        pnl_pct = float(pnl_ratio) * 100.0 if abs(pnl_ratio) <= 1.0 else float(pnl_ratio)

        prev = self.positions.get(symbol)
        is_new = prev is None
        self.positions[symbol] = Position(
            symbol=symbol, side=side, lev=float(lev), qty=float(qty),
            entry=float(entry), mark=float(mark), pnl=float(pnl), pnl_pct=float(pnl_pct),
            notion_page_id=self.open_pages.get(norm_symbol(symbol))
        )

        # Notion
        title = norm_symbol(symbol)
        pid = self.open_pages.get(title)
        if pid:
            await self.notion.update_open(pid, qty=float(qty), entry=float(entry), mark=float(mark))
        else:
            await self.notion.create_open(symbol, side, float(lev), float(qty), float(entry), float(mark))
            # refresh cache so we can link it
            self.open_pages = await self.notion.query_open_pages()

        # Discord buffer for "New Trades"
        if is_new:
            self.buffer_new.append({
                "symbol": symbol, "side": side, "lev": float(lev),
                "entry": float(entry), "pnl": float(pnl), "pnl_pct": float(pnl_pct),
            })

    async def on_close(self, ev: Dict[str, Any]):
        symbol = (ev.get("instId") or ev.get("symbol") or "").upper().replace("_", "-")
        mark = _f(ev.get("markPx") or ev.get("markPrice") or ev.get("px") or ev.get("last"))
        if not symbol:
            return
        if symbol in self.positions:
            # Notion close
            title = norm_symbol(symbol)
            pid = self.open_pages.get(title)
            if pid:
                await self.notion.close(pid, float(mark or self.positions[symbol].mark or self.positions[symbol].entry))
            # remove locally
            self.positions.pop(symbol, None)
            # force a post (close + snapshot)
            await self.maybe_post(force=True)

    async def ws_loop(self):
        while True:
            try:
                print(f"Connecting to BloFin WS… ({WS_URL})")
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(login_frame()))
                    # (Optionally) verify login ack here if server echoes event
                    await ws.send(json.dumps(sub_positions_frame()))
                    await ws.send(json.dumps(sub_orders_frame()))
                    print("✅ Connected & subscribed (orders/positions)")

                    last_new_flush = 0.0

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue
                        arg = data.get("arg") or {}
                        ch = arg.get("channel")

                        if ch == "positions":
                            for ev in data.get("data", []):
                                await self.on_open_or_update(ev)
                            # when new trades buffered, coalesce briefly then post snapshot + new
                            if self.buffer_new and (now_ts() - last_new_flush) > 2.0:
                                await self.maybe_post(force=True)
                                last_new_flush = now_ts()
                            else:
                                await self.maybe_post()

                        elif ch == "orders":
                            # If you prefer, you can react to order-state closes here;
                            # we rely on positions=0 to avoid duplicates.
                            await self.maybe_post()

            except Exception as e:
                print("🔁 Reconnecting to BloFin WS in 2s…", repr(e))
                await asyncio.sleep(2.0)

# ============ ENTRYPOINT ============

async def run():
    # Basic sanity logs
    if not DISCORD_WEBHOOK:
        print("WARNING: DISCORD_WEBHOOK_URL not set.")
    if not (BLOFIN_API_KEY and BLOFIN_API_SECRET and BLOFIN_PASSPHRASE):
        print("WARNING: Missing BloFin credentials.")
    if not (NOTION_API_TOKEN and NOTION_DATABASE_ID):
        print("WARNING: Missing Notion creds / DB id.")

    r = Runner()
    await r.init_notion()
    await r.ws_loop()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
