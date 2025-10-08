# main.py
# BloFin → Notion (plus optional Discord alerts)
# - Detect OPEN/CLOSE from BloFin WS 'positions'
# - On OPEN: upsert a Notion page (Status=<OPEN_LABEL>, fill entry fields)
# - On CLOSE: update that page (Status=<CLOSED_LABEL>, exit/PnL)
# - Discord alerts are optional (set DISCORD_WEBHOOK_URL)
# - Robust Notion setup:
#     * Accepts real database ID
#     * If given a PAGE ID and NOTION_DATABASE_NAME, auto-finds the database by name
#     * Detects Status property type (status vs select) to avoid 400s.

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
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]  # can be a real DB ID or (by mistake) a PAGE ID
NOTION_DATABASE_NAME = os.getenv("NOTION_DATABASE_NAME", "")  # optional fallback search string
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

# Your labels for open/closed
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
    if s in ("long", "buy"):  return "Buy"
    if s in ("short", "sell"): return "Short"
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

# ---------- Notion client & schema ----------
N_BASE = "https://api.notion.com/v1"
N_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

_STATUS_FILTER_KIND = "status"  # will be set to "status" or "select"
_DB_ID: str = NOTION_DATABASE_ID  # resolved database id

async def try_get_database(client: httpx.AsyncClient, dbid: str) -> Optional[dict]:
    r = await client.get(f"{N_BASE}/databases/{dbid}", headers=N_HEADERS)
    if r.status_code == 200:
        return r.json()
    return None

async def search_database_by_name(client: httpx.AsyncClient, name: str) -> Optional[str]:
    """Search for a database by name and return its id."""
    payload = {
        "query": name,
        "filter": {"property": "object", "value": "database"},
        "page_size": 10,
    }
    r = await client.post(f"{N_BASE}/search", headers=N_HEADERS, json=payload)
    if r.status_code >= 400:
        print("Notion search error:", r.status_code, r.text)
        r.raise_for_status()
    res = r.json()
    for item in res.get("results", []):
        title_parts = item.get("title", [])
        title = "".join([t.get("plain_text", "") for t in title_parts]) if title_parts else ""
        if title.strip().lower() == name.strip().lower():
            return item["id"]
    # fallback: return the first database result if exact name not found
    if res.get("results"):
        return res["results"][0]["id"]
    return None

async def resolve_database_id(client: httpx.AsyncClient) -> str:
    """Ensure we have a valid database id; if a page id was provided, try to resolve via search."""
    global _DB_ID
    # First: try using provided ID as a database
    db = await try_get_database(client, NOTION_DATABASE_ID)
    if db:
        _DB_ID = NOTION_DATABASE_ID
        return _DB_ID

    # If it failed (likely a page id), try to find DB by name if provided
    if NOTION_DATABASE_NAME:
        found = await search_database_by_name(client, NOTION_DATABASE_NAME)
        if not found:
            raise RuntimeError(
                "The NOTION_DATABASE_ID you provided is not a database, and NOTION_DATABASE_NAME "
                "did not match any shared database. Please provide a valid database ID or a correct name."
            )
        # Validate found id
        db = await try_get_database(client, found)
        if not db:
            raise RuntimeError(
                f"Found a candidate database '{NOTION_DATABASE_NAME}', but Notion refused it."
            )
        _DB_ID = found
        return _DB_ID

    # No name fallback available
    raise RuntimeError(
        "Provided NOTION_DATABASE_ID appears to be a PAGE, not a DATABASE. "
        "Fix: open your Notion database → ••• → Copy link to view, and use the 32-char ID after the last '-' "
        "(or set NOTION_DATABASE_NAME so I can search for it)."
    )

async def notion_get_db_schema(client: httpx.AsyncClient) -> dict:
    r = await client.get(f"{N_BASE}/databases/{_DB_ID}", headers=N_HEADERS)
    if r.status_code >= 400:
        print("Notion DB schema error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

async def init_schema_and_labels(client: httpx.AsyncClient):
    """Resolve DB id and detect whether Status is 'status' or 'select'."""
    global _STATUS_FILTER_KIND
    await resolve_database_id(client)
    schema = await notion_get_db_schema(client)
    props = schema.get("properties", {})
    status_prop = props.get(P_STATUS)
    if not status_prop:
        raise RuntimeError(f"Notion DB missing property '{P_STATUS}'. Check NOTION_PROP_STATUS.")
    ptype = status_prop.get("type")
    _STATUS_FILTER_KIND = "status" if ptype == "status" else "select"

def status_filter_equals(label: str) -> dict:
    return {"property": P_STATUS, _STATUS_FILTER_KIND: {"equals": label}} if _STATUS_FILTER_KIND == "status" \
        else {"property": P_STATUS, "select": {"equals": label}}

def status_set_value(label: str) -> dict:
    return {_STATUS_FILTER_KIND: {"name": label}} if _STATUS_FILTER_KIND == "status" \
        else {"select": {"name": label}}

def direction_filter_equals(label: str) -> dict:
    # Direction is expected to be a select
    return {"property": P_DIR, "select": {"equals": label}}

def title_filter_equals(symbol: str) -> dict:
    return {"property": P_TITLE, "title": {"equals": symbol}}

async def notion_query_open_page(client: httpx.AsyncClient, symbol: str, direction: str) -> Optional[str]:
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
        f"{N_BASE}/databases/{_DB_ID}/query",
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
        "parent": {"database_id": _DB_ID},
        "properties": {
            P_TITLE:   {"title": [{"text": {"content": symbol}}]},
            P_STATUS:  status_set_value(STATUS_OPEN_LABEL),
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
    cpnl    = row.get("unrealizedPnl")
    pct     = pct_signed(row.get("unrealizedPnlRatio"))
    net     = cpnl

    props = {
        P_STATUS:  status_set_value(STATUS_CLOSED_LABEL),
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
            f"{N_BASE}/databases/{_DB_ID}/query",
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
        LATEST_ROW[key] = r
    return keys

# ---------- Main loop ----------
async def run():
    # Start Discord worker
    asyncio.create_task(discord_sender())

    async with httpx.AsyncClient(timeout=15) as notion_client:
        # Resolve db id & schema, then pre-load open pages
        await init_schema_and_labels(notion_client)
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

                        if not first_snapshot:
                            # New trades
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

                            # Closed trades
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
                print("Loop error:", repr(e))
                await asyncio.sleep(min(backoff, 30))
                backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
