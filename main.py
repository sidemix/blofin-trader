# main.py
# BloFin → Notion (plus optional Discord alerts)
# - Detect OPEN/CLOSE from BloFin WS 'positions'
# - On OPEN: upsert a Notion page (fills only properties that exist & match type)
# - On CLOSE: update the same page
# - Handles Notion Status as status or select; Account as relation or select

import os, json, time, hmac, base64, hashlib, asyncio
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
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]  # real DB id
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

# Notion property names (override to match your DB)
P_TITLE   = os.getenv("NOTION_PROP_TITLE", "Trade Name")
P_STATUS  = os.getenv("NOTION_PROP_STATUS", "Status")
P_DIR     = os.getenv("NOTION_PROP_DIRECTION", "Direction")
P_LEV     = os.getenv("NOTION_PROP_LEVERAGE", "Leverage")
P_QTY     = os.getenv("NOTION_PROP_QTY", "Qty")
P_ENTRY   = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
P_OPENED  = os.getenv("NOTION_PROP_DATE_OPEN", "Date Opened")
P_EXIT    = os.getenv("NOTION_PROP_EXIT", "Exit Price")
P_CLOSED  = os.getenv("NOTION_PROP_DATE_CLOSE", "Date Closed")
P_UPNL    = os.getenv("NOTION_PROP_UPNL", "Unrealized P&L")   # optional; skip if missing
P_CPNL    = os.getenv("NOTION_PROP_CPNL", "Closed P&L")       # optional
P_PCT     = os.getenv("NOTION_PROP_PCT", "P&L %")             # optional
P_NET     = os.getenv("NOTION_PROP_NET", "Net Profit / Loss") # optional
P_ACCT    = os.getenv("NOTION_PROP_ACCOUNT", "Account")       # relation or select
P_TRADEID = os.getenv("NOTION_PROP_TRADE_ID", "Trade ID")     # optional rich_text

# Status labels
STATUS_OPEN_LABEL   = os.getenv("NOTION_STATUS_OPEN_LABEL", "Open")      # e.g. "1.Open"
STATUS_CLOSED_LABEL = os.getenv("NOTION_STATUS_CLOSED_LABEL", "Closed")  # e.g. "3.Closed"

# If Account is a relation, you can provide a page id to link (optional)
NOTION_ACCOUNT_PAGE_ID = os.getenv("NOTION_ACCOUNT_PAGE_ID", "")

# Behavior
SIZE_EPS = float(os.getenv("SIZE_EPS", "1e-8"))

# ---------- Utils ----------
def fnum(x, d=6):
    try: return f"{float(x):.{d}f}"
    except Exception: return str(x or "")

def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")

def parse_size(row: dict) -> float:
    s = row.get("positions") or row.get("pos") or row.get("size") or 0
    try: return float(s)
    except Exception: return 0.0

def dir_long_short(row: dict) -> str:
    # Notion column uses "Long"/"Short" in your DB
    s = str(row.get("positionSide") or "").lower()
    if s in ("long", "buy"):  return "Long"
    if s in ("short", "sell"): return "Short"
    sz = parse_size(row)
    return "Long" if sz >= 0 else "Short"

def pct_signed(ratio) -> Optional[float]:
    try: return float(ratio) * 100.0
    except Exception: return None

# ---------- State ----------
OpenKey = Tuple[str, str]  # (instId, "Long"/"Short")
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
                    retry = 2.0
                    try: retry = float(r.json().get("retry_after", 2.0))
                    except Exception: pass
                    await asyncio.sleep(retry)
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

# ---------- Notion helpers ----------
N_BASE = "https://api.notion.com/v1"
N_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

DB_PROPS: Dict[str, dict] = {}     # property schema
STATUS_KIND = "status"             # "status" or "select" (detected)

def prop_exists(name: str) -> bool:
    return name in DB_PROPS

def prop_type(name: str) -> Optional[str]:
    return DB_PROPS.get(name, {}).get("type")

async def load_db_schema(client: httpx.AsyncClient):
    r = await client.get(f"{N_BASE}/databases/{NOTION_DATABASE_ID}", headers=N_HEADERS)
    if r.status_code >= 400:
        print("Notion DB schema error:", r.status_code, r.text)
    r.raise_for_status()
    schema = r.json()
    props = schema.get("properties", {})
    DB_PROPS.clear()
    DB_PROPS.update(props)
    global STATUS_KIND
    pt = prop_type(P_STATUS)
    STATUS_KIND = "status" if pt == "status" else "select"

def status_set(label: str) -> dict:
    return {"status": {"name": label}} if STATUS_KIND == "status" else {"select": {"name": label}}

def status_filter_equals(label: str) -> dict:
    return {"property": P_STATUS, "status": {"equals": label}} if STATUS_KIND == "status" \
        else {"property": P_STATUS, "select": {"equals": label}}

def direction_select(label: str) -> Optional[dict]:
    if prop_type(P_DIR) == "select":
        return {"select": {"name": label}}
    return None

# Build properties dicts safely (only include what exists and matches expected type)
def build_open_properties(symbol: str, direction: str, row: dict) -> dict:
    props = {}

    # Title
    if prop_type(P_TITLE) == "title":
        props[P_TITLE] = {"title": [{"text": {"content": symbol}}]}

    # Status
    if prop_exists(P_STATUS) and prop_type(P_STATUS) in ("status", "select"):
        props[P_STATUS] = status_set(STATUS_OPEN_LABEL)

    # Direction
    ds = direction_select(direction)
    if ds: props[P_DIR] = ds

    # Numbers
    mapping_num = [
        (P_LEV, row.get("leverage")),
        (P_QTY, parse_size(row)),
        (P_ENTRY, row.get("entryPrice") or row.get("avgPrice") or row.get("averagePrice")),
    ]
    for name, val in mapping_num:
        if prop_type(name) == "number" and val not in (None, ""):
            try: props[name] = {"number": float(val)}
            except Exception: pass

    # Optional: Date Opened
    if prop_type(P_OPENED) == "date":
        props[P_OPENED] = {"date": {"start": now_iso()}}

    # Optional: Unrealized P&L (number)
    if prop_type(P_UPNL) == "number":
        upnl = row.get("unrealizedPnl")
        if upnl not in (None, ""):
            try: props[P_UPNL] = {"number": float(upnl)}
            except Exception: pass

    # Optional: P&L % (number)
    if prop_type(P_PCT) == "number":
        pct = pct_signed(row.get("unrealizedPnlRatio"))
        if pct is not None:
            props[P_PCT] = {"number": pct}

    # Account: select or relation
    pt = prop_type(P_ACCT)
    if pt == "select":
        props[P_ACCT] = {"select": {"name": "Blofin"}}
    elif pt == "relation" and NOTION_ACCOUNT_PAGE_ID:
        props[P_ACCT] = {"relation": [{"id": NOTION_ACCOUNT_PAGE_ID}]}
    # else: skip to avoid type errors

    # Optional: Trade ID (rich_text)
    if prop_type(P_TRADEID) == "rich_text":
        open_ts = str(row.get("updateTime") or row.get("createTime") or int(time.time() * 1000))
        trade_id = f"{symbol}|{direction}|{open_ts}"
        props[P_TRADEID] = {"rich_text": [{"text": {"content": trade_id}}]}

    return props

def build_close_properties(row: dict) -> dict:
    props = {}
    # Status
    if prop_exists(P_STATUS) and prop_type(P_STATUS) in ("status", "select"):
        props[P_STATUS] = status_set(STATUS_CLOSED_LABEL)
    # Exit price
    if prop_type(P_EXIT) == "number":
        exit_px = row.get("markPrice") or row.get("lastPrice") or row.get("price")
        if exit_px not in (None, ""):
            try: props[P_EXIT] = {"number": float(exit_px)}
            except Exception: pass
    # Date Closed
    if prop_type(P_CLOSED) == "date":
        props[P_CLOSED] = {"date": {"start": now_iso()}}
    # Closed P&L
    if prop_type(P_CPNL) == "number":
        cpnl = row.get("unrealizedPnl")
        if cpnl not in (None, ""):
            try: props[P_CPNL] = {"number": float(cpnl)}
            except Exception: pass
    # P&L %
    if prop_type(P_PCT) == "number":
        pct = pct_signed(row.get("unrealizedPnlRatio"))
        if pct is not None:
            props[P_PCT] = {"number": pct}
    # Net Profit/Loss
    if prop_type(P_NET) == "number":
        net = row.get("unrealizedPnl")
        if net not in (None, ""):
            try: props[P_NET] = {"number": float(net)}
            except Exception: pass
    # Clear Unrealized P&L if present
    if prop_type(P_UPNL) == "number":
        props[P_UPNL] = {"number": None}
    return props

async def notion_query_open_page(client: httpx.AsyncClient, symbol: str, direction: str) -> Optional[str]:
    payload = {
        "filter": {
            "and": [
                {"property": P_TITLE, "title": {"equals": symbol}},
                {"property": P_DIR, "select": {"equals": direction}} if prop_type(P_DIR)=="select" else {"property": P_DIR, "rich_text": {"equals": direction}},
                status_filter_equals(STATUS_OPEN_LABEL)
            ]
        },
        "page_size": 1
    }
    r = await client.post(f"{N_BASE}/databases/{NOTION_DATABASE_ID}/query", headers=N_HEADERS, json=payload)
    if r.status_code == 400:
        print("Notion 400 in query:", r.text)
    r.raise_for_status()
    res = r.json()
    return res.get("results", [{}])[0].get("id") if res.get("results") else None

async def notion_create_open_page(client: httpx.AsyncClient, symbol: str, direction: str, row: dict) -> str:
    data = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": build_open_properties(symbol, direction, row)
    }
    r = await client.post(f"{N_BASE}/pages", headers=N_HEADERS, json=data)
    if r.status_code >= 400:
        print("Notion create error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()["id"]

async def notion_update_close_page(client: httpx.AsyncClient, page_id: str, row: dict):
    props = build_close_properties(row)
    r = await client.patch(f"{N_BASE}/pages/{page_id}", headers=N_HEADERS, json={"properties": props})
    if r.status_code >= 400:
        print("Notion update(close) error:", r.status_code, r.text)
    r.raise_for_status()

# ---------- BloFin WS ----------
def ws_login_payload() -> dict:
    ts = str(int(time.time() * 1000))
    nonce = ts
    msg = "/users/self/verify" + "GET" + ts + nonce
    hex_sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    sign_b64 = base64.b64encode(hex_sig).decode()
    return {"op": "login", "args": [{
        "apiKey": API_KEY,
        "passphrase": PASSPHRASE,
        "timestamp": ts,
        "nonce": nonce,
        "sign": sign_b64
    }]}

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

def sub_payloads() -> List[dict]:
    return [{"op": "subscribe", "args": [{"channel": "positions"}]}]

def keys_from_rows(rows: List[dict]) -> Set[OpenKey]:
    keys: Set[OpenKey] = set()
    for r in rows:
        if abs(parse_size(r)) <= SIZE_EPS:
            continue
        key = (str(r.get("instId")), dir_long_short(r))
        keys.add(key)
        LATEST_ROW[key] = r
    return keys

# ---------- Main ----------
async def run():
    asyncio.create_task(discord_sender())

    async with httpx.AsyncClient(timeout=20) as notion_client:
        # Read schema so we know what exists/types
        await load_db_schema(notion_client)

        backoff = 1
        first_snapshot = True
        global LAST_OPEN_KEYS

        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=5_000_000) as ws:
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
                        if msg.get("event") in ("subscribe","unsubscribe","info","pong","welcome"):
                            continue
                        if not is_push(msg) or msg.get("arg", {}).get("channel") != "positions":
                            continue

                        rows = msg.get("data", [])
                        keys_now = keys_from_rows(rows)

                        added = sorted(list(keys_now - LAST_OPEN_KEYS))
                        removed = sorted(list(LAST_OPEN_KEYS - keys_now))

                        if not first_snapshot:
                            # Opened
                            for (symbol, direction) in added:
                                page_id = await notion_query_open_page(notion_client, symbol, direction)
                                if not page_id:
                                    page_id = await notion_create_open_page(notion_client, symbol, direction, LATEST_ROW[(symbol, direction)])
                                OPEN_PAGE_IDS[(symbol, direction)] = page_id
                                avg = LATEST_ROW[(symbol, direction)].get("entryPrice") or LATEST_ROW[(symbol, direction)].get("avgPrice") or LATEST_ROW[(symbol, direction)].get("averagePrice")
                                await send_discord(f"🟢 New trade: **{symbol}** {direction} @ avg {fnum(avg,6)}")

                            # Closed
                            for key in removed:
                                last_row = LATEST_ROW.get(key, {})
                                page_id = OPEN_PAGE_IDS.get(key)
                                if not page_id:
                                    page_id = await notion_query_open_page(notion_client, key[0], key[1])
                                if page_id:
                                    await notion_update_close_page(notion_client, page_id, last_row)
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
