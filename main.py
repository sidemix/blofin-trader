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

# =========================
# ======= CONFIG ==========
# =========================

def _clean(s: Optional[str]) -> Optional[str]:
    return s.strip() if isinstance(s, str) else None

# --- BloFin API auth (same keys you used before) ---
BLOFIN_API_KEY = _clean(os.getenv("BLOFIN_API_KEY"))
BLOFIN_API_SECRET = _clean(os.getenv("BLOFIN_API_SECRET"))
BLOFIN_PASSPHRASE = _clean(os.getenv("BLOFIN_PASSPHRASE"))

# WS URL fallback order (we’ll try your env first)
_ENV_WS = _clean(os.getenv("BLOFIN_WS_URL"))
WS_CANDIDATES = [u for u in [
    _ENV_WS,
    "wss://ws.blofin.com/ws/v5/private",
    "wss://ws.blofin.com/ws/private",
    "wss://wsp.blofin.com/ws/v5/private",
] if u]

# --- Discord (Webhook) ---
DISCORD_WEBHOOK = _clean(os.getenv("DISCORD_WEBHOOK_URL"))
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "300"))  # seconds
PERIOD_HOURS = float(os.getenv("PERIOD_HOURS", "6"))              # summary interval

# --- Table title in Discord ---
TABLE_TITLE = os.getenv("TABLE_TITLE", "Active BloFin Positions")

# --- Number formatting ---
SIZE_EPS = float(os.getenv("SIZE_EPS", "0.0000001"))

# --- Notion ---
NOTION_API_TOKEN = _clean(os.getenv("NOTION_API_TOKEN"))
# Either set NOTION_DATABASE_ID directly (32-char ID) or paste the full view link
NOTION_DATABASE_ID = _clean(os.getenv("NOTION_DATABASE_ID"))
NOTION_VIEW_LINK = _clean(os.getenv("NOTION_VIEW_LINK"))  # optional; we’ll derive the ID

# Property names (defaults match screenshots you shared)
P_TITLE = os.getenv("NOTION_PROP_TITLE", "Trade Name")      # title
P_STATUS = os.getenv("NOTION_PROP_STATUS", "Status")        # select: 1.Live / 3.Closed
P_DIRECTION = os.getenv("NOTION_PROP_DIRECTION", "Direction")  # select: Long/Short
P_LEVERAGE = os.getenv("NOTION_PROP_LEVERAGE", "Leverage")     # number
P_QTY = os.getenv("NOTION_PROP_QTY", "Qty")                    # number
P_ENTRY = os.getenv("NOTION_PROP_ENTRY", "Entry Price")        # number
P_EXIT = os.getenv("NOTION_PROP_EXIT", "Exit Price")           # number
P_MARKET = os.getenv("NOTION_PROP_MARKET", "Market Price")     # number
P_PAIR = os.getenv("NOTION_PROP_PAIR", "Pair/Market")          # relation or text (we won’t write if relation)
STATUS_OPEN = os.getenv("NOTION_STATUS_OPEN_LABEL", "1.Live")
STATUS_CLOSED = os.getenv("NOTION_STATUS_CLOSED_LABEL", "3.Closed")

NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

# =========================
# ===== UTILITIES =========
# =========================

def now_ts() -> float:
    return time.time()

def now_iso() -> str:
    return dt.datetime.utcnow().isoformat() + "Z"

def short_num(x: float, places: int = 6) -> float:
    # constrain decimals; avoid scientific notation in tables/Notion
    fmt = "{:." + str(places) + "f}"
    return float(fmt.format(x))

def fmt_pct(x: float, places: int = 2) -> str:
    sign = "+" if x >= 0 else "-"
    return f"{sign}{abs(x):.{places}f}%"

def fmt_money(x: float, places: int = 6) -> str:
    sign = "" if x >= 0 else "-"
    return f"{sign}{abs(x):.{places}f}"

def normalize_symbol(inst: str) -> str:
    # BloFin sends "ETH-USDT" (or similar). Keep that for Discord; for Notion title show "ETH/USDT"
    inst = inst.upper().replace("_", "-")
    parts = inst.split("-")
    if len(parts) == 2:
        return f"{parts[0]}/{parts[1]}"
    return inst.replace("-", "/")

def okx_style_sign(ts: str) -> str:
    # Many OKX-style exchanges (BloFin included) expect sign on this literal path
    msg = f"{ts}GET/users/self/verify".encode()
    key = BLOFIN_API_SECRET.encode()
    return base64.b64encode(hmac.new(key, msg, hashlib.sha256).digest()).decode()

def make_login_frame() -> Dict[str, Any]:
    ts = str(int(time.time()))
    return {
        "op": "login",
        "args": [{
            "apiKey": BLOFIN_API_KEY,
            "passphrase": BLOFIN_PASSPHRASE,
            "timestamp": ts,
            "sign": okx_style_sign(ts),
        }],
    }

def sub_frame(channel: str) -> Dict[str, Any]:
    # private: orders / positions / account; OKX-style “op=subscribe” shape
    return {"op": "subscribe", "args": [{"channel": channel}]}

# =========================
# ======= MODELS ==========
# =========================

@dataclasses.dataclass
class Position:
    symbol: str           # "ETH-USDT"
    side: str             # "long" or "short"
    lev: float            # leverage
    entry: float          # average entry price
    mark: float           # mark price
    qty: float            # position size in base units (no custom scaling)
    pnl: float            # running PnL in quote
    pnl_pct: float        # % return on notional (approx)

    def digest(self) -> Tuple:
        """Used to detect meaningful changes for Discord posting."""
        return (
            self.symbol,
            self.side,
            round(self.lev, 2),
            round(self.entry, 6),
            round(self.mark, 6),
            round(self.qty, 8),
            round(self.pnl, 6),
            round(self.pnl_pct, 2),
        )

# =========================
# ===== NOTION I/O ========
# =========================

class Notion:
    def __init__(self, token: Optional[str], db_id: Optional[str], view_link: Optional[str]):
        self.token = token
        self.db_id = db_id or self._id_from_view(view_link)
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }
        self.client = httpx.AsyncClient(timeout=30.0)

    def _id_from_view(self, link: Optional[str]) -> Optional[str]:
        if not link:
            return None
        # Extract the 32-char id after the last slash and before '?'
        # e.g. https://www.notion.so/.../<dbid>?v=...
        try:
            core = link.split("/")[-1]
            core = core.split("?")[0]
            core = core.replace("-", "")
            return core if len(core) >= 32 else None
        except Exception:
            return None

    async def _raise_if(self, r: httpx.Response):
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            print("Notion error:", e.response.text)
            raise

    async def ensure_ok(self):
        if not (self.token and self.db_id):
            return
        # Touch schema (also serves as auth/access check)
        r = await self.client.get(f"https://api.notion.com/v1/databases/{self.db_id}", headers=self.headers)
        await self._raise_if(r)
        # Not printing full schema to keep noisy logs clean
        print("Notion DB schema detected.")

    async def query_open_pages(self) -> Dict[str, str]:
        """
        Returns a map: symbol_title -> page_id for open positions.
        We use title as "ETH/USDT" etc.
        """
        if not (self.token and self.db_id):
            return {}
        payload = {
            "filter": {
                "property": P_STATUS,
                "select": {"equals": STATUS_OPEN}
            }
        }
        r = await self.client.post(
            f"https://api.notion.com/v1/databases/{self.db_id}/query",
            headers=self.headers, json=payload
        )
        await self._raise_if(r)
        out = {}
        data = r.json()
        for row in data.get("results", []):
            props = row.get("properties", {})
            title = ""
            if P_TITLE in props and props[P_TITLE].get("title"):
                title = "".join([t.get("plain_text","") for t in props[P_TITLE]["title"]])
            if title:
                out[title] = row["id"]
        return out

    def _title(self, text: str) -> Dict[str, Any]:
        return {"title": [{"type": "text", "text": {"content": text}}]}

    def _number(self, val: Optional[float]) -> Dict[str, Any]:
        return {"number": None if val is None else float(val)}

    def _select(self, name: str) -> Dict[str, Any]:
        return {"select": {"name": name}}

    def _date_now(self) -> Dict[str, Any]:
        return {"date": {"start": dt.datetime.utcnow().isoformat()}}

    async def create_open(self, pos: Position):
        if not (self.token and self.db_id):
            return
        props = {
            P_TITLE: self._title(normalize_symbol(pos.symbol)),
            P_STATUS: self._select(STATUS_OPEN),
            P_DIRECTION: self._select("Long" if pos.side.lower()=="long" else "Short"),
            P_LEVERAGE: self._number(short_num(pos.lev, 2)),
            P_QTY: self._number(short_num(pos.qty, 8)),
            P_ENTRY: self._number(short_num(pos.entry, 6)),
            P_MARKET: self._number(short_num(pos.mark, 6)),
        }
        payload = {"parent": {"database_id": self.db_id}, "properties": props}
        r = await self.client.post("https://api.notion.com/v1/pages", headers=self.headers, json=payload)
        await self._raise_if(r)

    async def update_open(self, page_id: str, pos: Position):
        if not (self.token and page_id):
            return
        props = {
            P_QTY: self._number(short_num(pos.qty, 8)),
            P_ENTRY: self._number(short_num(pos.entry, 6)),
            P_MARKET: self._number(short_num(pos.mark, 6)),
        }
        r = await self.client.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=self.headers, json={"properties": props}
        )
        await self._raise_if(r)

    async def close_page(self, page_id: str, exit_price: Optional[float]):
        if not (self.token and page_id):
            return
        props = {P_STATUS: self._select(STATUS_CLOSED)}
        if exit_price is not None:
            props[P_EXIT] = self._number(short_num(exit_price, 6))
        r = await self.client.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=self.headers, json={"properties": props}
        )
        await self._raise_if(r)

# =========================
# ===== DISCORD I/O =======
# =========================

class Discord:
    def __init__(self, webhook: Optional[str]):
        self.webhook = webhook
        self.client = httpx.AsyncClient(timeout=30.0)
        self.last_send = 0.0

    async def send(self, content: str):
        if not self.webhook:
            return
        # rate-limit guard
        delta = now_ts() - self.last_send
        if delta < SEND_MIN_INTERVAL:
            # still allow in “new trade” case if it's batched once; caller should group
            pass

        while True:
            r = await self.client.post(self.webhook, json={"content": content})
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", "2.0"))
                await asyncio.sleep(max(1.0, retry_after))
                continue
            r.raise_for_status()
            self.last_send = now_ts()
            break

def render_positions_block(title: str, positions: List[Position]) -> str:
    # monospace table
    lines = []
    lines.append(f"**{title}**")
    lines.append("```")
    lines.append(f"{'SYMBOL':<10} {'SIDE ×LEV':<11} {'AVG Entry Price':<15} {'PNL':<12} {'PNL%':<8}")
    lines.append(f"{'-'*10} {'-'*11} {'-'*15} {'-'*12} {'-'*8}")
    for p in positions:
        side = "Buy" if p.side.lower()=="long" else "Short"
        lev = f"{int(round(p.lev))}x" if p.lev else "—"
        pnl_pct = f"{'+' if p.pnl_pct>=0 else ''}{p.pnl_pct:.2f}%"
        lines.append(
            f"{p.symbol:<10} {side:<3} {lev:<7} "
            f"{p.entry:>14.6f}  "
            f"{p.pnl:>11.6f}  "
            f"{pnl_pct:>7}"
        )
    lines.append("```")
    return "\n".join(lines)

def render_new_trades_block(new_positions: List[Position]) -> str:
    if not new_positions:
        return ""
    lines = []
    lines.append("**New Trades**")
    lines.append("```")
    lines.append(f"{'SYMBOL':<10} {'SIDE ×LEV':<11} {'AVG Entry Price':<15} {'PNL':<12} {'PNL%':<8}")
    lines.append(f"{'-'*10} {'-'*11} {'-'*15} {'-'*12} {'-'*8}")
    for p in new_positions:
        side = "Buy" if p.side.lower()=="long" else "Short"
        lev = f"{int(round(p.lev))}x" if p.lev else "—"
        pnl_pct = f"{'+' if p.pnl_pct>=0 else ''}{p.pnl_pct:.2f}%"
        lines.append(
            f"{p.symbol:<10} {side:<3} {lev:<7} "
            f"{p.entry:>14.6f}  "
            f"{p.pnl:>11.6f}  "
            f"{pnl_pct:>7}"
        )
    lines.append("```")
    return "\n".join(lines)

# =========================
# ====== ENGINE ===========
# =========================

def compute_pnl(side: str, entry: float, mark: float, qty: float) -> Tuple[float, float]:
    """
    Very conservative pnl approximation in quote currency:
    PnL = (mark - entry) * qty  (long)
    PnL = (entry - mark) * qty  (short)
    PnL% ~ PnL / (entry * qty) * 100
    """
    if entry is None or mark is None or qty is None:
        return 0.0, 0.0
    raw = (mark - entry) * qty if side.lower()=="long" else (entry - mark) * qty
    notional = max(1e-12, entry * qty)
    pct = (raw / notional) * 100.0
    return float(raw), float(pct)

def parse_position_event(ev: Dict[str, Any]) -> Optional[Position]:
    """
    Try to normalize BloFin "positions" payload into Position.
    We avoid arbitrary scaling; only round for display.
    Fields vary by exchange; this handles the common ones we saw in your logs.
    """
    try:
        inst = ev.get("instId") or ev.get("symbol") or ev.get("pair") or ""
        symbol = inst.replace("/", "-").upper()
        pos_side = (ev.get("posSide") or ev.get("side") or ev.get("direction") or "long").lower()
        side = "long" if "long" in pos_side else ("short" if "short" in pos_side else "long")

        lev = float(ev.get("lever", ev.get("leveraged", ev.get("leverage", 0)) ) or 0)

        entry = ev.get("avgPx") or ev.get("avgEntryPrice") or ev.get("entryPrice") or ev.get("avgPrice")
        mark = ev.get("markPx") or ev.get("markPrice") or ev.get("last") or ev.get("px")

        qty = ev.get("pos") or ev.get("size") or ev.get("positionAmt") or ev.get("quantity") or 0
        entry = float(entry or 0)
        mark = float(mark or 0)
        qty = float(qty or 0)

        # avoid negative qty; use side for direction
        qty = abs(qty)

        pnl_val = ev.get("upl") or ev.get("pnl") or ev.get("unrealizedPnl")
        pnl_pct_val = ev.get("uplRatio") or ev.get("pnlRatio") or ev.get("unrealizedPnlRatio")

        if pnl_val is None or pnl_pct_val is None:
            pnl_val, pnl_pct_val = compute_pnl(side, entry, mark, qty)
        else:
            pnl_val = float(pnl_val)
            pnl_pct_val = float(pnl_pct_val) * (100.0 if abs(pnl_pct_val) <= 1.0 else 1.0)

        return Position(
            symbol=symbol,
            side=side,
            lev=lev,
            entry=entry,
            mark=mark,
            qty=qty,
            pnl=pnl_val,
            pnl_pct=pnl_pct_val,
        )
    except Exception:
        return None

class Runner:
    def __init__(self):
        self.discord = Discord(DISCORD_WEBHOOK)
        self.notion = Notion(NOTION_API_TOKEN, NOTION_DATABASE_ID, NOTION_VIEW_LINK)

        self.positions: Dict[str, Position] = {}
        self.prev_digest: Tuple = tuple()
        self.next_summary_at = now_ts() + PERIOD_HOURS * 3600
        self.open_pages: Dict[str, str] = {}   # Notion title -> page_id

        self._new_trade_buffer: List[Position] = []

    # ----- Notion helpers -----
    async def notion_sync_open_map(self):
        try:
            await self.notion.ensure_ok()
            self.open_pages = await self.notion.query_open_pages()
        except Exception as e:
            print("Notion init/sync error:", repr(e))

    async def notion_on_open_or_update(self, p: Position):
        """Create a page if missing; otherwise update it."""
        if not self.notion.db_id:
            return
        title = normalize_symbol(p.symbol)
        page_id = self.open_pages.get(title)
        if page_id:
            try:
                await self.notion.update_open(page_id, p)
            except Exception as e:
                print("Notion update error:", repr(e))
        else:
            try:
                await self.notion.create_open(p)
                # refresh cache so we don't duplicate
                self.open_pages = await self.notion.query_open_pages()
            except Exception as e:
                print("Notion create error:", repr(e))

    async def notion_on_close(self, inst: str, exit_price: Optional[float]):
        if not self.notion.db_id:
            return
        title = normalize_symbol(inst)
        page_id = self.open_pages.get(title)
        if not page_id:
            return
        try:
            await self.notion.close_page(page_id, exit_price)
            # keep it in cache; it won’t return from query_open_pages next run
        except Exception as e:
            print("Notion close error:", repr(e))

    # ----- Discord posting -----
    def _positions_digest(self) -> Tuple:
        keys = sorted(self.positions.keys())
        cols = []
        for k in keys:
            cols.append(self.positions[k].digest())
        return tuple(cols)

    async def maybe_post_summary(self):
        tick = now_ts()
        digest = self._positions_digest()

        # periodic timer or material change
        material_change = digest != self.prev_digest
        time_due = tick >= self.next_summary_at

        # We only post summary table if (a) changed OR (b) timer expired
        if material_change or time_due:
            # Compose message: New trades (if any) + Active table
            blocks = []
            if self._new_trade_buffer:
                blocks.append(render_new_trades_block(self._new_trade_buffer))
                self._new_trade_buffer.clear()

            if self.positions:
                ordered = sorted(self.positions.values(), key=lambda p: p.symbol)
                blocks.append(render_positions_block(TABLE_TITLE, ordered))

            if blocks:
                await self.discord.send("\n\n".join([b for b in blocks if b.strip()]))

            # advance pointers
            self.prev_digest = digest
            self.next_summary_at = now_ts() + PERIOD_HOURS * 3600

    # ----- Websocket consumer -----
    async def ws_loop(self):
        # We try endpoints until one actually works and remains stable
        while True:
            for candidate in WS_CANDIDATES:
                try:
                    print(f"Connecting to BloFin WS… ({candidate})")
                    async with websockets.connect(candidate, ping_interval=20, ping_timeout=20) as ws:
                        # login
                        await ws.send(json.dumps(make_login_frame()))
                        # subscribe
                        for ch in ("orders", "positions", "account"):
                            await ws.send(json.dumps(sub_frame(ch)))
                        print("✅ Connected to BloFin WS and subscribed (orders/positions/account).")

                        # flow
                        while True:
                            raw = await ws.recv()
                            data = json.loads(raw)
                            if not isinstance(data, dict):
                                continue

                            arg = data.get("arg", {})
                            channel = arg.get("channel")

                            # positions feed
                            if channel == "positions":
                                for ev in data.get("data", []):
                                    pos = parse_position_event(ev)
                                    if not pos:
                                        continue

                                    # Detect open/close via qty; if qty ~ 0: closed
                                    if pos.qty <= SIZE_EPS:
                                        # close
                                        if pos.symbol in self.positions:
                                            # mark Notion closed with exit=mark
                                            await self.notion_on_close(pos.symbol, pos.mark)
                                            del self.positions[pos.symbol]
                                        continue

                                    # open/update
                                    prior = self.positions.get(pos.symbol)
                                    is_open = prior is None
                                    self.positions[pos.symbol] = pos
                                    await self.notion_on_open_or_update(pos)
                                    if is_open:
                                        # buffer for next post
                                        self._new_trade_buffer.append(pos)

                            # orders feed (optional; use it to catch closes by order)
                            if channel == "orders":
                                for ev in data.get("data", []):
                                    state = str(ev.get("state") or ev.get("status") or "").lower()
                                    if "closed" in state or "filled" in state or "canceled" in state:
                                        inst = ev.get("instId") or ev.get("symbol") or ""
                                        # we'll verify via positions soon anyway

                            # push out if due
                            await self.maybe_post_summary()

                except Exception as e:
                    print("WS candidate failed:", candidate, repr(e))
                    await asyncio.sleep(2.0)
                    continue

            # If all endpoints failed in the loop, wait and retry the full list
            print("No WS endpoints reachable. Retrying in 5s…")
            await asyncio.sleep(5.0)

    async def run(self):
        await self.notion_sync_open_map()
        await self.ws_loop()

# =========================
# ========= MAIN ==========
# =========================

if __name__ == "__main__":
    missing = []
    for k in ("BLOFIN_API_KEY", "BLOFIN_API_SECRET", "BLOFIN_PASSPHRASE", "DISCORD_WEBHOOK_URL"):
        if not _clean(os.getenv(k)):
            missing.append(k)
    if missing:
        print("WARNING: missing env vars:", ", ".join(missing))

    try:
        asyncio.run(Runner().run())
    except KeyboardInterrupt:
        pass
