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

# --------------------------
# Environment & configuration
# --------------------------

def _clean(s: Optional[str]) -> str:
    return (s or "").strip().strip('"').strip("'")

BLOFIN_API_KEY = _clean(os.getenv("BLOFIN_API_KEY"))
BLOFIN_API_SECRET = _clean(os.getenv("BLOFIN_API_SECRET"))
BLOFIN_PASSPHRASE = _clean(os.getenv("BLOFIN_PASSPHRASE"))

BLOFIN_WS_URL = _clean(os.getenv("BLOFIN_WS_URL")) or "wss://ws.blofin.com/ws/private"

DISCORD_WEBHOOK_URL = _clean(os.getenv("DISCORD_WEBHOOK_URL"))

NOTION_API_TOKEN = _clean(os.getenv("NOTION_API_TOKEN"))
NOTION_DATABASE_ID = _clean(os.getenv("NOTION_DATABASE_ID"))

# Notion property names (match your DB screenshot)
PROP_TITLE = _clean(os.getenv("NOTION_PROP_TITLE")) or "Trade Name"
PROP_STATUS = _clean(os.getenv("NOTION_PROP_STATUS")) or "Status"
PROP_DIRECTION = _clean(os.getenv("NOTION_PROP_DIRECTION")) or "Direction"
PROP_LEVERAGE = _clean(os.getenv("NOTION_PROP_LEVERAGE")) or "Leverage"
PROP_QTY = _clean(os.getenv("NOTION_PROP_QTY")) or "Qty"
PROP_ENTRY = _clean(os.getenv("NOTION_PROP_ENTRY")) or "Entry Price"
PROP_EXIT = _clean(os.getenv("NOTION_PROP_EXIT")) or "Exit Price"
PROP_DATE_OPEN = _clean(os.getenv("NOTION_PROP_DATE_OPEN")) or "Date Opened"
PROP_DATE_CLOSE = _clean(os.getenv("NOTION_PROP_DATE_CLOSE")) or "Date Closed"
PROP_MKT_PRICE = _clean(os.getenv("NOTION_PROP_MARKET_PRICE")) or "Market Price"

OPEN_LABEL = _clean(os.getenv("NOTION_STATUS_OPEN_LABEL")) or "1.Open"
CLOSED_LABEL = _clean(os.getenv("NOTION_STATUS_CLOSED_LABEL")) or "3.Closed"

# Posting cadence
PERIOD_HOURS = int(_clean(os.getenv("PERIOD_HOURS")) or "6")
SEND_MIN_INTERVAL = int(_clean(os.getenv("SEND_MIN_INTERVAL")) or "5")  # minutes
TABLE_TITLE = _clean(os.getenv("TABLE_TITLE")) or "Active Positions"

# Small epsilon for comparing sizes
SIZE_EPS = float(_clean(os.getenv("SIZE_EPS")) or "1e-8")

STATE_PATH = "/mnt/data/state.json"

# -------------
# Utils
# -------------

def _now_ts() -> float:
    return time.time()

def _utc_iso(ts: Optional[float] = None) -> str:
    return dt.datetime.utcfromtimestamp(ts or _now_ts()).replace(microsecond=0).isoformat() + "Z"

def _f(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def _fmt_dec(x: float, max_dec: int = 6) -> str:
    s = f"{x:.{max_dec}f}"
    return s.rstrip("0").rstrip(".")

def _pos_side_name(raw: str) -> str:
    r = (raw or "").lower()
    if r in ("long", "buy", "net", "net_long"):
        return "Buy"
    if r in ("short", "sell", "net_short"):
        return "Short"
    # fall back
    return raw or "Buy"

def compute_base_qty(ev: Dict[str, Any]) -> Optional[float]:
    """
    Normalize size to BASE units regardless of whether event uses
    contracts (sz, ctVal), notional, or already base qty.
    """
    if not ev:
        return None

    # already-provided base style keys
    for k in ("qty", "baseSz", "base_size", "sizeBase", "fillBase", "accFillSzBase"):
        q = _f(ev.get(k))
        if q is not None:
            return q

    # contracts → base
    sz = _f(ev.get("sz") or ev.get("size") or ev.get("fillSz") or ev.get("accFillSz"))
    ct = _f(ev.get("ctVal") or ev.get("ctValCcy") or ev.get("ctv"))
    if (sz is not None) and (ct is not None) and ct > 0:
        return sz * ct

    # notional / price
    notional = _f(ev.get("notional") or ev.get("notionalUsd") or ev.get("value"))
    px = _f(ev.get("avgPx") or ev.get("px") or ev.get("fillPx") or ev.get("markPx") or ev.get("entryPrice"))
    if (notional is not None) and (px is not None) and px > 0:
        return notional / px

    return None

def first_float(*keys, src: Dict[str, Any]):
    for k in keys:
        v = _f(src.get(k))
        if v is not None:
            return v
    return None

def first_str(*keys, src: Dict[str, Any]):
    for k in keys:
        v = src.get(k)
        if isinstance(v, str) and v:
            return v
    return None

# -------------
# Notion client
# -------------

N_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

async def notion_query_open(client: httpx.AsyncClient) -> Dict[str, str]:
    """
    Return {symbol: page_id} for open trades found in Notion DB.
    We key by Trade Name (title), which you’re using as SYMBOL like 'ETH-USDT'.
    """
    try:
        payload = {
            "filter": {
                "and": [
                    {"property": PROP_STATUS, "select": {"equals": OPEN_LABEL}},
                ]
            },
            "page_size": 100,
        }
        r = await client.post(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                              headers=N_HEADERS, json=payload, timeout=30)
        r.raise_for_status()
        res = r.json()
        mapping = {}
        for page in res.get("results", []):
            props = page.get("properties", {})
            title = props.get(PROP_TITLE, {}).get("title", [])
            name = title[0]["plain_text"] if title else ""
            if name:
                mapping[name] = page["id"]
        return mapping
    except Exception as e:
        print("Notion query error:", repr(e))
        return {}

async def notion_create_open(client: httpx.AsyncClient, symbol: str, side: str, lev: float,
                             qty_base: float, entry_px: float, opened_ts: float) -> Optional[str]:
    try:
        payload = {
            "parent": {"database_id": NOTION_DATABASE_ID},
            "properties": {
                PROP_TITLE: {"title": [{"text": {"content": symbol}}]},
                PROP_STATUS: {"select": {"name": OPEN_LABEL}},
                PROP_DIRECTION: {"select": {"name": "Long" if side == "Buy" else "Short"}},
                PROP_LEVERAGE: {"number": lev},
                PROP_QTY: {"number": qty_base},
                PROP_ENTRY: {"number": entry_px},
                PROP_DATE_OPEN: {"date": {"start": dt.datetime.utcfromtimestamp(opened_ts).isoformat() + "Z"}},
            },
        }
        r = await client.post("https://api.notion.com/v1/pages", headers=N_HEADERS, json=payload, timeout=30)
        r.raise_for_status()
        pid = r.json()["id"]
        return pid
    except Exception as e:
        try:
            print("Notion create error:", r.status_code, r.text)  # type: ignore
        except Exception:
            pass
        print("Notion create exception:", repr(e))
        return None

async def notion_close_trade(client: httpx.AsyncClient, page_id: str, exit_px: float, closed_ts: float):
    try:
        payload = {
            "properties": {
                PROP_STATUS: {"select": {"name": CLOSED_LABEL}},
                PROP_EXIT: {"number": exit_px},
                PROP_DATE_CLOSE: {"date": {"start": dt.datetime.utcfromtimestamp(closed_ts).isoformat() + "Z"}},
            }
        }
        r = await client.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=N_HEADERS, json=payload, timeout=30)
        r.raise_for_status()
    except Exception as e:
        try:
            print("Notion close error:", r.status_code, r.text)  # type: ignore
        except Exception:
            pass
        print("Notion close exception:", repr(e))

async def notion_update_mark(client: httpx.AsyncClient, page_id: str, mark_px: float):
    try:
        payload = {"properties": {PROP_MKT_PRICE: {"number": mark_px}}}
        r = await client.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=N_HEADERS, json=payload, timeout=30)
        r.raise_for_status()
    except Exception as e:
        # non-fatal
        pass

# -------------
# Discord
# -------------

def fmt_table(rows: List[List[str]]) -> str:
    # monospace table for Discord
    col_widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    out = []
    for r in rows:
        out.append("  ".join(w.ljust(col_widths[i]) for i, w in enumerate(r)))
    return "```\n" + "\n".join(out) + "\n```"

async def post_discord(text: str):
    if not DISCORD_WEBHOOK_URL:
        return
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(DISCORD_WEBHOOK_URL, json={"content": text})
        if r.status_code >= 400:
            print("Discord post error:", r.status_code, r.text)

def build_discord_message(new_trades: List[Dict[str, Any]], positions: List[Dict[str, Any]]) -> str:
    blocks = []
    # New trades
    if new_trades:
        hdr = [["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]]
        for nt in new_trades:
            pnl = _fmt_dec(nt.get("uPnL", 0.0), 6)
            pnlp = nt.get("uPnLRatio", 0.0)
            sign = "+" if pnlp >= 0 else ""
            hdr.append([
                nt["symbol"],
                f'{nt["side"]} {int(nt.get("lever", 0))}x',
                _fmt_dec(nt.get("entry", 0.0), 6),
                pnl,
                f"{sign}{_fmt_dec(100*pnlp, 2)}%"
            ])
        blocks.append("**New Trades**\n" + fmt_table(hdr))

    # Active positions
    tab = [["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]]
    for p in positions:
        pnlp = p.get("uPnLRatio", 0.0)
        sign = "+" if pnlp >= 0 else ""
        tab.append([
            p["symbol"],
            f'{p["side"]} {int(p.get("lever", 0))}x',
            _fmt_dec(p.get("entry", 0.0), 6),
            _fmt_dec(p.get("uPnL", 0.0), 6),
            f"{sign}{_fmt_dec(100*p.get('uPnLRatio', 0.0), 2)}%"
        ])
    blocks.append(f"**{TABLE_TITLE}**\n" + fmt_table(tab))

    return "\n".join(blocks)

# -----------------
# State management
# -----------------

@dataclasses.dataclass
class Position:
    symbol: str
    side: str  # Buy/Short
    lever: float
    qty_base: float
    entry: float
    mark: float
    uPnL: float
    uPnLRatio: float
    notion_page_id: Optional[str] = None
    opened_ts: float = dataclasses.field(default_factory=_now_ts)

    def fingerprint(self) -> Tuple:
        return (self.symbol, self.side, round(self.qty_base, 10), round(self.entry, 8))

class BotState:
    def __init__(self):
        self.positions: Dict[str, Position] = {}  # symbol -> Position (net)
        self.last_snapshot_posted_at: float = 0.0
        self.next_summary_at: float = _now_ts() + PERIOD_HOURS * 3600
        self.last_post_ts: float = 0.0
        self.open_pages: Dict[str, str] = {}  # symbol -> page id

    def to_dict(self):
        return {
            "positions": {k: dataclasses.asdict(v) for k, v in self.positions.items()},
            "last_snapshot_posted_at": self.last_snapshot_posted_at,
            "next_summary_at": self.next_summary_at,
            "last_post_ts": self.last_post_ts,
            "open_pages": self.open_pages,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BotState":
        s = BotState()
        for k, v in d.get("positions", {}).items():
            s.positions[k] = Position(**v)
        s.last_snapshot_posted_at = d.get("last_snapshot_posted_at", 0.0)
        s.next_summary_at = d.get("next_summary_at", _now_ts() + PERIOD_HOURS * 3600)
        s.last_post_ts = d.get("last_post_ts", 0.0)
        s.open_pages = d.get("open_pages", {})
        return s

def load_state() -> BotState:
    try:
        with open(STATE_PATH, "r") as f:
            return BotState.from_dict(json.load(f))
    except Exception:
        return BotState()

def save_state(state: BotState):
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(state.to_dict(), f)
    except Exception:
        pass

STATE = load_state()

# -----------------
# Blofin WS helpers
# -----------------

def blofin_sign(ts: str) -> str:
    # Blofin follows OKX-style signing
    msg = f"{ts}GET/users/self/verify"
    mac = hmac.new(BLOFIN_API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def make_login_frame() -> Dict[str, Any]:
    ts = str(time.time())
    return {
        "op": "login",
        "args": [{
            "apiKey": BLOFIN_API_KEY,
            "passphrase": BLOFIN_PASSPHRASE,
            "timestamp": ts,
            "sign": blofin_sign(ts),
        }]
    }

def sub_frame(channel: str) -> Dict[str, Any]:
    return {"op": "subscribe", "args": [{"channel": channel}]}

def norm_symbol(ev: Dict[str, Any]) -> Optional[str]:
    return first_str("instId", "symbol", "sym", src=ev)

def extract_side(ev: Dict[str, Any]) -> str:
    raw = first_str("posSide", "side", src=ev) or "Buy"
    return _pos_side_name(raw)

def extract_leverage(ev: Dict[str, Any]) -> float:
    return first_float("lever", "leverage", src=ev) or 0.0

def extract_entry(ev: Dict[str, Any]) -> float:
    return first_float("avgPx", "avg", "entryPrice", src=ev) or 0.0

def extract_mark(ev: Dict[str, Any]) -> float:
    return first_float("markPx", "mark", "markPrice", src=ev) or 0.0

def extract_upnl(ev: Dict[str, Any]) -> float:
    return first_float("uPnL", "upnl", "unrealizedPnl", src=ev) or 0.0

def extract_upnl_ratio(ev: Dict[str, Any]) -> float:
    return first_float("uPnLRatio", "uPnlRatio", "pnlRatio", src=ev) or 0.0

# -----------------
# Core logic
# -----------------

async def handle_new_position(client_notion: httpx.AsyncClient, p: Position) -> None:
    # create or link Notion page
    page_id = STATE.open_pages.get(p.symbol)
    if not page_id:
        page_id = await notion_create_open(client_notion, p.symbol, p.side, p.lever, p.qty_base, p.entry, p.opened_ts)
        if page_id:
            STATE.open_pages[p.symbol] = page_id
            p.notion_page_id = page_id
            save_state(STATE)

async def handle_close_position(client_notion: httpx.AsyncClient, symbol: str, exit_px: float):
    page_id = STATE.open_pages.get(symbol)
    if page_id:
        await notion_close_trade(client_notion, page_id, exit_px, _now_ts())
        STATE.open_pages.pop(symbol, None)
        save_state(STATE)

def positions_snapshot() -> List[Dict[str, Any]]:
    out = []
    for pos in STATE.positions.values():
        out.append({
            "symbol": pos.symbol,
            "side": pos.side,
            "lever": pos.lever,
            "entry": pos.entry,
            "mark": pos.mark,
            "uPnL": pos.uPnL,
            "uPnLRatio": pos.uPnLRatio,
        })
    # sort by symbol
    out.sort(key=lambda x: x["symbol"])
    return out

async def maybe_post_summary(force: bool = False, also_new: Optional[List[Dict[str, Any]]] = None):
    now = _now_ts()
    allow_regular = (now - STATE.last_post_ts) >= SEND_MIN_INTERVAL * 60
    due = now >= STATE.next_summary_at
    if force or (due and allow_regular):
        text = build_discord_message(also_new or [], positions_snapshot())
        await post_discord(text)
        STATE.last_post_ts = now
        STATE.last_snapshot_posted_at = now
        if due:
            STATE.next_summary_at = now + PERIOD_HOURS * 3600
        save_state(STATE)

async def ws_loop():
    # Notion http client (reuse)
    async with httpx.AsyncClient(timeout=30) as notion_client:
        # bootstrap open pages map from Notion
        STATE.open_pages.update(await notion_query_open(notion_client))
        save_state(STATE)

        while True:
            try:
                print("Connecting to BloFin WS…")
                async with websockets.connect(BLOFIN_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    # login
                    await ws.send(json.dumps(make_login_frame()))
                    # subscribe
                    for ch in ("orders", "positions", "account"):
                        await ws.send(json.dumps(sub_frame(ch)))
                    print("✅ Connected to BloFin WS and subscribed (orders/positions/account).")

                    new_trades_to_announce: List[Dict[str, Any]] = []
                    last_emit = 0.0

                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)

                        if isinstance(data, dict) and "event" in data and data.get("event") != "subscribe":
                            # server ack or ping events—we can ignore
                            pass

                        # Uniform events with "arg" + "data" (OKX-style)
                        if not isinstance(data, dict) or "arg" not in data:
                            continue

                        ch = data["arg"].get("channel")
                        for ev in data.get("data", []):
                            sym = norm_symbol(ev)
                            if not sym:
                                continue

                            if ch == "positions":
                                side = extract_side(ev)
                                lev = extract_leverage(ev)
                                entry = extract_entry(ev) or 0.0
                                mark = extract_mark(ev) or 0.0
                                qty = compute_base_qty(ev) or 0.0
                                upnl = extract_upnl(ev)
                                upnlr = extract_upnl_ratio(ev)

                                # detect open/close/modify
                                prev = STATE.positions.get(sym)
                                if qty <= SIZE_EPS:
                                    # Closed
                                    if prev is not None:
                                        await handle_close_position(notion_client, sym, exit_px=mark or prev.entry)
                                        STATE.positions.pop(sym, None)
                                        save_state(STATE)
                                        # Announce closed immediately with snapshot
                                        await maybe_post_summary(force=True, also_new=[{
                                            "symbol": sym, "side": prev.side, "lever": prev.lever,
                                            "entry": prev.entry, "uPnL": prev.uPnL, "uPnLRatio": prev.uPnLRatio
                                        }])
                                    continue

                                # Open or changed
                                p = Position(
                                    symbol=sym, side=side, lever=lev, qty_base=qty,
                                    entry=entry, mark=mark, uPnL=upnl, uPnLRatio=upnlr,
                                    notion_page_id=STATE.open_pages.get(sym),
                                    opened_ts=_now_ts()
                                )
                                if prev is None:
                                    # New position
                                    STATE.positions[sym] = p
                                    save_state(STATE)
                                    await handle_new_position(notion_client, p)
                                    new_trades_to_announce.append({
                                        "symbol": sym, "side": side, "lever": lev,
                                        "entry": entry, "uPnL": upnl, "uPnLRatio": upnlr
                                    })
                                else:
                                    # update existing
                                    prev.lever = lev
                                    prev.qty_base = qty
                                    prev.entry = entry or prev.entry
                                    prev.mark = mark or prev.mark
                                    prev.uPnL = upnl
                                    prev.uPnLRatio = upnlr
                                    STATE.positions[sym] = prev
                                    # keep Notion mark updated quietly
                                    if prev.notion_page_id and mark:
                                        await notion_update_mark(notion_client, prev.notion_page_id, mark)

                                # Debounce announcement for burst of opens; always also show current snapshot
                                if new_trades_to_announce and (_now_ts() - last_emit) > 2.0:
                                    await maybe_post_summary(force=True, also_new=new_trades_to_announce[:])
                                    new_trades_to_announce.clear()
                                    last_emit = _now_ts()

                        # Periodic summary
                        await maybe_post_summary()

            except Exception as e:
                print("🔁 Reconnecting to BloFin WS in 2s…", repr(e))
                await asyncio.sleep(2.0)

# -------------
# Entrypoint
# -------------

async def run():
    # Basic checks
    if not (BLOFIN_API_KEY and BLOFIN_API_SECRET and BLOFIN_PASSPHRASE):
        print("Missing Blofin creds.")
    if not DISCORD_WEBHOOK_URL:
        print("Missing DISCORD_WEBHOOK_URL.")
    if not (NOTION_API_TOKEN and NOTION_DATABASE_ID):
        print("Missing Notion configuration.")

    await ws_loop()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
