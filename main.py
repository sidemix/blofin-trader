# main.py
# BloFin → Notion (plus optional Discord alerts)
# - Detects OPEN/CLOSE from BloFin WS 'positions'
# - On OPEN: upsert a Notion page (Status=<OPEN_LABEL>, fill entry fields)
# - On CLOSE: update same page (Status=<CLOSED_LABEL>, fill exit/PnL fields)
# - Discord alerts are optional (set DISCORD_WEBHOOK_URL to enable)
# - Auto-detects Notion property types (Status: status vs select) to avoid 400 errors.

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
WS_URL = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

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

# Your label text in Notion for open/closed
STATUS_OPEN_LABEL   = os.getenv("NOTION_STATUS_OPEN_LABEL", "Open")      # e.g., "1.Open"
STATUS_CLOSED_LABEL = os.getenv("NOTION_STATUS_CLOSED_LABEL", "Closed")  # e.g., "3.Closed"

# Behavior
SIZE_EPS = float(os.getenv("SIZE_EPS", "1e-8"))

# ---------- Utils ----------
def fnum(x, d=6):
    try:
        return f"{float(x):.{d}f}"
    except Exception:
        return str(x or "")

def now_iso():
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

# ---------- Notion client & schema detection ----------
N_BASE = "https://api.notion.com/v1"
N_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# Will be set at runtime after we read DB schema
_STATUS_FILTER_KIND = "status"  # or "select"

async def notion_get_db_schema(client: httpx.AsyncClient) -> dict:
    r = await client.get(f"{N_BASE}/databases/{NOTION_DATABASE_ID}", headers=N_HEADERS)
    # Helpful error print if misconfigured
    if r.status_code >= 400:
        print("Notion DB schema error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

async def init_schema_and_labels(client: httpx.AsyncClient):
    """Detect whether P_STATUS is 'status' or 'select' and configure filters accordingly."""
    global _STATUS_FILTER_KIND
    schema = await notion_get_db_schema(client)
    props = schema.get("properties", {})

    status_prop = props.get(P_STATUS)
    if not status_prop:
        raise RuntimeError(f"Notion DB missing property '{P_STATUS}'. Check NOTION_PROP_STATUS.")

    ptype = status_prop.get("type")
    if ptype == "status":
        _STATUS_FILTER_KIND = "status"
    elif ptype == "select":
        _STATUS_FILTER_KIND = "select"
    else:
        # Fallback: treat like select
        _STATUS_FILTER_KIND = "select"

def status_filter_equals(label: str) -> dict:
    # Returns the correct filter snippet depending on property type
    if _STATUS_FILTER_KIND == "status":
        return {"property": P_STATUS, "status": {"equals": label}}
    else:
        return {"property": P_STATUS, "select": {"equals": label}}

def direction_filter_equals(label: str) -> dict:
    # Direction is expected to be a select
    return {"property": P_DIR, "select": {"equals": label}}

def title_filter_equals(symbol: str) -> dict:
    return {"property": P_TITLE, "title": {"equals": symbol}}

async def notion_query_open_page(client: httpx.AsyncClient, symbol: str, direction: str) -> Optional[str]:
    """Return an existing OPEN page id for (symbol, direction), else None."""
    payload = {
        "filter": {
            "and": [
                title_filter_equals(symbol),
                direction_filter_equals(direction),
                status_filter_equals(STATUS_OPEN_LABEL),
            ]
        },
        "page_size": 1
    }
    r = await client.post(
        f"{N_BASE}/databases/{NOTION_DATABASE_ID}/query",
        headers=N_HEADERS,
        json=payload,
    )
    if r.status_code == 400:
        print("Notion 400 in notion_query_open_page:", r.text)
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

    open_ts = str(row.get("updateTime") or row.get("createTime") or int(time.time() * 1000))
    trade_id = f"{symbol}|{direction}|{open_ts}"

    data = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            P_TITLE:   {"title": [{"text": {"content": symbol}}]},
            P_STATUS:  ({_STATUS_FILTER_KIND: {"name": STATUS_OPEN_LABEL}} if _STATUS_FILTER_KIND == "status"
                        else {"select": {"name": STATUS_OPEN_LABEL}}),
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
    r = await client.post(f"{N_BASE}/pages", headers=N_HEADERS, json=data)
    if r.status_code >= 400:
        print("Notion create error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()["id"]

async def notion_update_close_page(client: httpx.AsyncClient, page_id: str, key: OpenKey, row: dict):
    exit_px = row.get("markPrice") or row.get("lastPrice") or row.get("price")
    cpnl    = row.get("unrealizedPnl")      # last seen before disappearance
    pct     = pct_signed(row.get("unrealizedPnlRatio"))
    net     = cpnl

    props = {
        P_STATUS:  ({_STATUS_FILTER_KIND: {"name": STATUS_CLOSED_LABEL}} if _STATUS_FILTER_KIND == "status"
                    else {"select": {"name": STATUS_CLOSED_LABEL}}),
        P_EXIT:    {"number": float(exit_px) if exit_px not in (None, "") else None},
        P_CLOSED:  {"date": {"start": now_iso()}},
        P_CPNL:    {"number": float(cpnl) if cpnl not in (None, "") else None},
        P_PCT:     {"number": float(pct) if pct is not None else None},
        P_NET:     {"number": float(net) if net not in (None, "") else None},
        P_UPNL:    {"number": None},
    }
    r = await client.patch(f"{N_BASE}/pages/{page_id}", headers=N_HEADERS, json={"properties": props})
    if r.status_code >= 400:
        print("Notion update(close) error:", r.status_code, r.text)
    r.raise_for_status()

async def notion_load_existing_opens(client: httpx.AsyncClient):
    """Prefill OPEN_PAGE_IDS from Notion (handles restarts)."""
    start_cursor = None
    while True:
        payload = {
            "filter": status_filter_equals(STATUS_OPEN_LABEL),
            "page_size": 100
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        r = await client.post(
            f"{N_BASE}/databases/{NOTION_DATABASE_ID}/query",
            headers=N_HEADERS,
            json=payload,
        )
        if r.status_code == 400:
            print("Notion 400 in notion_load_existing_opens:", r.text)
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
        LATEST_ROW[key] = r  # keep freshest row
    return keys

# ---------- Main loop ----------
async def run():
    # Start Discord worker (no-op if webhook not set)
    asyncio.create_task(discord_sender())

    async with httpx.AsyncClient(timeout=15) as notion_client:
        # 1) Detect schema (sets _STATUS_FILTER_KIND)
        await init_schema_and_labels(notion_client)
        # 2) Prefill any open pages (so restarts don't duplicate)
        await notion_load_existing_opens(notion_client)

        backoff = 1
        first_snapshot = True
        global LAST_OPEN_KEYS

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

                        # Skip alerts on first snapshot (they'd be historical)
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
                                    page_id = await notion_query_open_page(notion_client, key[0], key[1])
                                if page_id:
                                    await notion_update_close_page(notion_client, page_id, key, last_row)
                                OPEN_PAGE_IDS.pop(key, None)
                                await send_discord(f"🔴 Closed trade: **{key[0]}** {key[1]}")

                        LAST_OPEN_KEYS = keys_now
                        first_snapshot = False

            except Exception as e:
                # helpful log for debugging Notion/BloFin issues
                print("Loop error:", repr(e))
                await asyncio.sleep(min(backoff, 30))
                backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
