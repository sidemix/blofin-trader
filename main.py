# main.py
# BloFin → (Discord alerts) + Notion database
# - Detect OPEN/CLOSE from BloFin WS 'positions'
# - On OPEN: upsert a Notion page (Status=Open, fill entry fields)
# - On CLOSE: update same page (Status=Closed, fill exit/PnL fields)
# - Discord alerts are optional (set DISCORD_WEBHOOK_URL to enable)

import os
import json
import time
import hmac
import base64
import hashlib
import asyncio
from typing import Dict, Tuple, List, Optional, Set

import websockets
import httpx

# ---------- ENV ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# BloFin
API_KEY = os.environ["BLOFIN_API_KEY"]
API_SECRET = os.environ["BLOFIN_API_SECRET"]
PASSPHRASE = os.environ["BLOFIN_PASSPHRASE"]
IS_DEMO = os.getenv("BLOFIN_IS_DEMO", "false").lower() == "true"
WS_URL = (
    "wss://demo-trading-openapi.blofin.com/ws/private"
    if IS_DEMO
    else "wss://openapi.blofin.com/ws/private"
)

# Discord (optional)
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", "")

# Notion
NOTION_API_TOKEN = os.environ["NOTION_API_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

# Notion property names (override via env if yours differ)
P_TITLE   = os.getenv("NOTION_PROP_TITLE", "Name")
P_STATUS  = os.getenv("NOTION_PROP_STATUS", "Status")
P_DIR     = os.getenv("NOTION_PROP_DIRECTION", "Direction")
P_LEV     = os.getenv("NOTION_PROP_LEVERAGE", "Leverage")
P_QTY     = os.getenv("NOTION_PROP_QTY", "Qty")
P_ENTRY   = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
P_OPENED  = os.getenv("NOTION_PROP_DATE_OPEN", "Date Opened")
P_EXIT    = os.getenv("NOTION_PROP_EXIT", "Exit Price")
P_CLOSED  = os.getenv("NOTION_PROP_DATE_CLOSE", "Date Closed")
P_UPNL    = os.getenv("NOTION_PROP_UPNL", "Unrealized P&L")
P_CPNL    = os.getenv("NOTION_PROP_CPNL", "Closed P&L")
P_PCT     = os.getenv("NOTION_PROP_PCT", "P&L %")
P_NET     = os.getenv("NOTION_PROP_NET", "Net Profit / Loss")
P_ACCT    = os.getenv("NOTION_PROP_ACCOUNT", "Account")
P_TRADEID = os.getenv("NOTION_PROP_TRADE_ID", "Trade ID")

# Behavior
SIZE_EPS = float(os.getenv("SIZE_EPS", "1e-8"))
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5"))

# ---------- Utils ----------
def fnum(x, d=6):
    try:
        return f"{float(x):.{d}f}"
    except Exception:
        return str(x or "")

def now_iso():
    # ISO-like, fine for Notion date
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")

def parse_size(row: dict) -> float:
    s = row.get("positions") or row.get("pos") or row.get("size") or 0
    try:
        return float(s)
    except Exception:
        return 0.0

def side_of(row: dict) -> str:
    s = str(row.get("positionSide") or "").lower()
    if s in ("long", "buy"):
        return "Buy"
    if s in ("short", "sell"):
        return "Short"
    sz = parse_size(row)
    return "Buy" if sz >= 0 else "Short"

def pct_signed(ratio) -> Optional[float]:
    try:
        return float(ratio) * 100.0
    except Exception:
        return None

# ---------- State ----------
OpenKey = Tuple[str, str]  # (instId, "Buy"/"Short")

LATEST_ROW: Dict[OpenKey, dict] = {}
LAST_OPEN_KEYS: Set[OpenKey] = set()
LAST_POST_TIME: float = 0.0

# mapping open position -> Notion page id
OPEN_PAGE_IDS: Dict[OpenKey, str] = {}

# ---------- Discord sender ----------
SEND_Q: asyncio.Queue = asyncio.Queue()

async def discord_sender():
    if not DISCORD_WEBHOOK:
        return
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            content = await SEND_Q.get()
            try:
                r = await client.post(DISCORD_WEBHOOK, json={"content": content})
                if r.status_code == 429:
                    try:
                        retry_after = float(r.json().get("retry_after", 2.0))
                    except Exception:
                        retry_after = 2.0
                    await asyncio.sleep(retry_after)
                    await SEND_Q.put(content)
                else:
                    r.raise_for_status()
                    await asyncio.sleep(1.1)
            except Exception:
                await asyncio.sleep(2.0)
            finally:
                SEND_Q.task_done()

async def send_discord(msg: str):
    if DISCORD_WEBHOOK:
        await SEND_Q.put(msg)

# ---------- Notion client ----------
N_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

async def notion_query_open_page(client: httpx.AsyncClient, symbol: str, direction: str) -> Optional[str]:
    """Return an existing OPEN page id for (symbol, direction), else None."""
    payload = {
        "filter": {
            "and": [
                {"property": P_TITLE,  "title":  {"equals": symbol}},
                {"property": P_DIR,    "select": {"equals": direction}},
                {"property": P_STATUS, "status": {"equals": "Open"}}
            ]
        },
        "page_size": 1
    }
    r = await client.post(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
        headers=N_HEADERS,
        json=payload,
    )
    r.raise_for_status()
    res = r.json()
    results = res.get("results", [])
    return results[0]["id"] if results else None

async def notion_create_open_page(client: httpx.AsyncClient, key: OpenKey, row: dict) -> str:
    symbol, direction = key
    lev  = row.get("leverage")
    qty  = parse_size(row)
    avg  = row.get("entryPrice") or row.get("avgPrice") or row.get("averagePrice")
    upnl = row.get("unrealizedPnl")
    pct  = pct_signed(row.get("unrealizedPnlRatio"))

    # Stable Trade ID (symbol|direction|first-seen-ts)
    open_ts = str(row.get("updateTime") or row.get("createTime") or int(time.time() * 1000))
    trade_id = f"{symbol}|{direction}|{open_ts}"

    data = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            P_TITLE:   {"title": [{"text": {"content": symbol}}]},
            P_STATUS:  {"status": {"name": "Open"}},
            P_DIR:     {"select": {"name": direction}},
            P_LEV:     {"number": float(lev) if lev not in (None, "") else None},
            P_QTY:     {"number": qty},
            P_ENTRY:   {"number": float(avg) if avg not in (None, "") else None},
            P_OPENED:  {"date": {"start": now_iso()}},
            P_UPNL:    {"number": float(upnl) if upnl not in (None, "") else None},
            P_PCT:     {"number": float(pct) if pct is not None else None},
            P_ACCT:    {"select": {"name": "Blofin"}},
            P_TRADEID: {"rich_text": [{"text": {"content": trade_id}}]},
        }
    }
    r = await client.post("https://api.notion.com/v1/pages", headers=N_HEADERS, json=data)
    r.raise_for_status()
    return r.json()["id"]

async def notion_update_close_page(client: httpx.AsyncClient, page_id: str, key: OpenKey, row: dict):
    """Mark Closed and fill exit numbers."""
    # Best-effort values from last snapshot we saw
    exit_px = row.get("markPrice") or row.get("lastPrice") or row.get("price")
    cpnl    = row.get("unrealizedPnl")  # last seen before disappearance
    pct     = pct_signed(row.get("unrealizedPnlRatio"))
    net     = cpnl

    props = {
        P_STATUS: {"status": {"name": "Closed"}},
        P_EXIT:   {"number": float(exit_px) if exit_px not in (None, "") else None},
        P_CLOSED: {"date": {"start": now_iso()}},
        P_CPNL:   {"number": float(cpnl) if cpnl not in (None, "") else None},
        P_PCT:    {"number": float(pct) if pct is not None else None},
        P_NET:    {"number": float(net) if net not in (None, "") else None},
        P_UPNL:   {"number": None},  # clear live uPnL
    }
    r = await client.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=N_HEADERS,
        json={"properties": props},
    )
    r.raise_for_status()

async def notion_load_existing_opens(client: httpx.AsyncClient):
    """On startup: prefill OPEN_PAGE_IDS from Notion (so restarts don't duplicate)."""
    start_cursor = None
    while True:
        payload = {
            "filter": {"property": P_STATUS, "status": {"equals": "Open"}},
            "page_size": 100
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        r = await client.post(
            f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
            headers=N_HEADERS,
            json=payload,
        )
        r.raise_for_status()
        res = r.json()

        for page in res.get("results", []):
            props = page.get("properties", {})
            # Rebuild key = (Name, Direction)
            try:
                name = "".join([t["plain_text"] for t in props[P_TITLE]["title"]])
            except Exception:
                name = ""
            direction = props.get(P_DIR, {}).get("select", {}).get("name", "")
            if name and direction:
                OPEN_PAGE_IDS[(name, direction)] = page["id"]

        if not res.get("has_more"):
            break
        start_cursor = res.get("next_cursor")

# ---------- BloFin WS helpers ----------
def ws_login_payload() -> dict:
    ts = str(int(time.time() * 1000))
    nonce = ts
    msg = "/users/self/verify" + "GET" + ts + nonce
    hex_sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    sign_b64 = base64.b64encode(hex_sig).decode()
    return {
        "op": "login",
        "args": [{
            "apiKey": API_KEY,
            "passphrase": PASSPHRASE,
            "timestamp": ts,
            "nonce": nonce,
            "sign": sign_b64
        }]
    }

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

def sub_payloads() -> List[dict]:
    return [{"op": "subscribe", "args": [{"channel": "positions"}]}]

def keys_from_rows(rows: List[dict]) -> Set[OpenKey]:
    keys: Set[OpenKey] = set()
    for r in rows:
        if abs(parse_size(r)) <= SIZE_EPS:
            continue
        key = (str(r.get("instId")), side_of(r))
        keys.add(key)
        LATEST_ROW[key] = r  # keep the freshest row for display on alerts/updates
    return keys

# ---------- Main loop ----------
async def run():
    # Start Discord worker (no-op if webhook not set)
    asyncio.create_task(discord_sender())

    async with httpx.AsyncClient(timeout=15) as notion_client:
        # Prefill open pages mapping from Notion (handles restarts)
        await notion_load_existing_opens(notion_client)

        backoff = 1
        first_snapshot = True

        while True:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=20, ping_timeout=20, max_size=5_000_000
                ) as ws:
                    await ws.send(json.dumps(ws_login_payload()))
                    login_ack = json.loads(await ws.recv())
                    if login_ack.get("event") == "error":
                        raise RuntimeError(f"Login failed: {login_ack}")

                    for sub in sub_payloads():
                        await ws.send(json.dumps(sub))

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("event") in ("subscribe", "unsubscribe", "info", "pong", "welcome"):
                            continue
                        if not is_push(msg) or msg.get("arg", {}).get("channel") != "positions":
                            continue

                        rows = msg.get("data", [])
                        keys_now = keys_from_rows(rows)

                        added = sorted(list(keys_now - LAST_OPEN_KEYS))
                        removed = sorted(list(LAST_OPEN_KEYS - keys_now))

                        # Skip alerts on the very first snapshot (they would be historical)
                        if not first_snapshot:
                            # --- NEW TRADES ---
                            for key in added:
                                symbol, direction = key
                                page_id = await notion_query_open_page(notion_client, symbol, direction)
                                if not page_id:
                                    page_id = await notion_create_open_page(notion_client, key, LATEST_ROW[key])
                                OPEN_PAGE_IDS[key] = page_id
                                avg = (
                                    LATEST_ROW[key].get("entryPrice")
                                    or LATEST_ROW[key].get("avgPrice")
                                    or LATEST_ROW[key].get("averagePrice")
                                )
                                await send_discord(f"🟢 New trade: **{symbol}** {direction} @ avg {fnum(avg, 6)}")

                            # --- CLOSED TRADES ---
                            for key in removed:
                                last_row = LATEST_ROW.get(key, {})
                                page_id = OPEN_PAGE_IDS.get(key)
                                if not page_id:
                                    # fallback query if mapping missing
                                    page_id = await notion_query_open_page(notion_client, key[0], key[1])
                                if page_id:
                                    await notion_update_close_page(notion_client, page_id, key, last_row)
                                OPEN_PAGE_IDS.pop(key, None)
                                await send_discord(f"🔴 Closed trade: **{key[0]}** {key[1]}")

                        LAST_OPEN_KEYS = keys_now
                        first_snapshot = False

            except Exception:
                await asyncio.sleep(min(backoff, 30))
                backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
