# main.py
# BloFin → Discord + Notion
# - Discord: compact positions table, plus New/Closed alert blocks
# - Notion: upsert "Open" trades and update the same page to "Closed" using Trade Key
# - Periodic table refresh every PERIOD_HOURS (default 6h)

import os, json, time, hmac, base64, hashlib, asyncio
from typing import Dict, Tuple, List, Optional

import websockets
import httpx

# ========== ENV ==========
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---- BloFin
API_KEY    = os.environ["BLOFIN_API_KEY"]
API_SECRET = os.environ["BLOFIN_API_SECRET"]
PASSPHRASE = os.environ["BLOFIN_PASSPHRASE"]
IS_DEMO    = os.getenv("BLOFIN_IS_DEMO", "false").lower() == "true"
WS_URL     = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

# ---- Discord
WEBHOOK_TABLE  = os.getenv("DISCORD_WEBHOOK_TABLE")  or os.environ["DISCORD_WEBHOOK_URL"]
WEBHOOK_ALERTS = os.getenv("DISCORD_WEBHOOK_ALERTS") or os.environ["DISCORD_WEBHOOK_URL"]
TABLE_TITLE    = os.getenv("TABLE_TITLE", "Active BloFin Positions")
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5"))  # seconds min between tables
PERIOD_HOURS   = float(os.getenv("PERIOD_HOURS", "6"))          # 0 disables

# ---- Notion
NOTION_TOKEN   = os.getenv("NOTION_API_TOKEN", "")
NOTION_DB_ID   = os.getenv("NOTION_DATABASE_ID", "")
N_VER          = os.getenv("NOTION_VERSION", "2022-06-28")

# Property names (defaults match your DB)
N_TITLE        = os.getenv("NOTION_PROP_TITLE", "Trade Name")
N_STATUS       = os.getenv("NOTION_PROP_STATUS", "Status")
N_STATUS_OPEN  = os.getenv("NOTION_STATUS_OPEN_LABEL", "1.Open")
N_STATUS_CLOSED= os.getenv("NOTION_STATUS_CLOSED_LABEL", "3.Closed")
N_DIR          = os.getenv("NOTION_PROP_DIRECTION", "Direction")
N_LEV          = os.getenv("NOTION_PROP_LEVERAGE", "Leverage")
N_QTY          = os.getenv("NOTION_PROP_QTY", "Qty")
N_ENTRY        = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
N_EXIT         = os.getenv("NOTION_PROP_EXIT", "Exit Price")
N_MARK         = os.getenv("NOTION_PROP_MARK", "Market Price")
N_CPNL         = os.getenv("NOTION_PROP_CPNL", "Closed P&L")
N_PCT          = os.getenv("NOTION_PROP_PCT", "P&L %")
N_DATE_OPENED  = os.getenv("NOTION_PROP_DATE_OPENED", "Date Opened")
N_DATE_CLOSED  = os.getenv("NOTION_PROP_DATE_CLOSED", "Date Closed")
N_TRADE_KEY    = os.getenv("NOTION_PROP_TRADE_KEY", "Trade Key")     # NEW (Text)

# ---- Parsing / sizing
SIZE_EPS       = float(os.getenv("SIZE_EPS", "0.00000001"))  # size <= EPS == closed

# ========== Discord sender (queue + 429) ==========
SEND_Q: asyncio.Queue = asyncio.Queue()

async def discord_sender():
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            url, content = await SEND_Q.get()
            try:
                r = await client.post(url, json={"content": content})
                if r.status_code == 429:
                    try:
                        retry_after = float(r.json().get("retry_after", 2.0))
                    except Exception:
                        retry_after = 2.0
                    await asyncio.sleep(retry_after)
                    await SEND_Q.put((url, content))
                else:
                    r.raise_for_status()
                    await asyncio.sleep(1.1)
            except Exception:
                # swallow & keep going
                await asyncio.sleep(2.0)
            finally:
                SEND_Q.task_done()

async def send_to(url: str, text: str):
    await SEND_Q.put((url, text))

async def send_table(text: str):   return await send_to(WEBHOOK_TABLE, text)
async def send_alert(text: str):   return await send_to(WEBHOOK_ALERTS, text)

# ========== BloFin WS auth/sub ==========
def ws_login_payload() -> dict:
    # Works with current BloFin gateway
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

def sub_payloads() -> List[dict]:
    return [{"op": "subscribe", "args": [{"channel": "positions"}]}]

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

# ========== Parsing helpers ==========
def fnum(x, d=6):
    try: return f"{float(x):.{d}f}"
    except: return str(x or "")

def parse_size(row: dict) -> float:
    for k in ("positions","pos","size"):
        if k in row:
            try: return float(row[k])
            except: pass
    return 0.0

def row_inst(row: dict) -> str:
    return str(row.get("instId") or row.get("symbol") or "")

def row_leverage(row: dict) -> Optional[float]:
    v = row.get("leverage")
    try: return float(v) if v is not None else None
    except: return None

def row_entry_price(row: dict) -> Optional[float]:
    for k in ("avgPx","avgPrice","averagePrice","entryPrice","avgEntryPx"):
        if k in row:
            try: return float(row[k])
            except: pass
    return None

def row_mark(row: dict) -> Optional[float]:
    for k in ("markPx","mark","markPrice"):
        if k in row:
            try: return float(row[k])
            except: pass
    return None

def row_qty(row: dict) -> Optional[float]:
    for k in ("qty","quantity","pos","positions","size"):
        if k in row:
            try: return float(row[k])
            except: pass
    return None

def row_unreal_pnl(row: dict) -> Optional[float]:
    for k in ("uPnl","unrealizedPnl"):
        if k in row:
            try: return float(row[k])
            except: pass
    return None

def row_unreal_ratio(row: dict) -> Optional[float]:
    for k in ("uPnlRatio","unrealizedPnlRatio"):
        if k in row:
            try: return float(row[k])
            except: pass
    return None

def infer_side(row: dict) -> str:
    s = str(row.get("positionSide") or "").lower()
    if s in ("long","buy"):  return "Buy"
    if s in ("short","sell"): return "Short"
    sz = parse_size(row)
    if sz > 0: return "Buy"
    if sz < 0: return "Short"
    return "Buy"

def fmt_signed_pct(ratio: Optional[float]) -> str:
    try:
        v = float(ratio) * 100.0
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.2f}%"
    except:
        return ""

# ========== State ==========
OpenKey = Tuple[str, str]  # (instId, "Buy"/"Short")
LATEST_ROW: Dict[OpenKey, dict] = {}     # cache latest row for display
LAST_OPEN_KEYS: set[OpenKey] = set()     # set of open pos keys
LAST_POST_TIME: float = 0.0

# Notion schema + open-page cache
N_SCHEMA_PROPS: set[str] = set()
OPEN_PAGE_IDS: Dict[OpenKey, str] = {}   # { (inst, side) : page_id }

def keys_from_rows(rows: List[dict]) -> set[OpenKey]:
    keys: set[OpenKey] = set()
    for r in rows:
        if abs(parse_size(r)) <= SIZE_EPS:
            continue
        k = (row_inst(r), infer_side(r))
        keys.add(k)
        LATEST_ROW[k] = r
    return keys

# ========== Discord formatting ==========
def table_for_keys(keys: set[OpenKey]) -> str:
    headers = ["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]
    rows: List[List[str]] = []
    for k in sorted(keys):
        r = LATEST_ROW.get(k, {})
        inst, side = k
        lev  = row_leverage(r) or 0
        avg  = row_entry_price(r)
        pnl  = row_unreal_pnl(r)
        pnlr = row_unreal_ratio(r)
        sidelev = f"{side} {int(lev)}x" if lev else side
        rows.append([inst, sidelev, fnum(avg,6), fnum(pnl,6), fmt_signed_pct(pnlr)])

    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]
    def fmt_row(r): return "  ".join(str(v).ljust(w) for v, w in zip(r, widths))

    lines = [f"**{TABLE_TITLE}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if rows:
        for r in rows: lines.append(fmt_row(r))
    else:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

def alert_block(title: str, keys: List[OpenKey]) -> str:
    headers = ["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]
    rows: List[List[str]] = []
    for k in keys:
        r = LATEST_ROW.get(k, {})
        inst, side = k
        lev  = row_leverage(r) or 0
        avg  = row_entry_price(r)
        pnl  = row_unreal_pnl(r)
        pnlr = row_unreal_ratio(r)
        sidelev = f"{side} {int(lev)}x" if lev else side
        rows.append([inst, sidelev, fnum(avg,6), fnum(pnl,6), fmt_signed_pct(pnlr)])

    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]
    def fmt_row(r): return "  ".join(str(v).ljust(w) for v, w in zip(r, widths))

    lines = [f"**{title}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if rows:
        for r in rows: lines.append(fmt_row(r))
    else:
        lines.append("(none)")
    lines.append("```")
    return "\n".join(lines)

async def post_table(keys: set[OpenKey], force: bool = False):
    global LAST_POST_TIME
    now = time.time()
    if not force and (now - LAST_POST_TIME) < SEND_MIN_INTERVAL:
        return
    await send_table(table_for_keys(keys))
    LAST_POST_TIME = now

# ========== Notion helpers ==========
def n_title(v: str): return {"title": [{"type": "text","text": {"content": v}}]}
def n_select(name: str): return {"select": {"name": name}} if name else None
def n_num(v: Optional[float]): return {"number": float(v)} if v is not None else {"number": None}
def n_date_ts(ts: Optional[float]):
    if ts is None: return {"date": None}
    iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
    return {"date": {"start": iso}}
def n_rich(v: str): return {"rich_text": [{"type":"text","text":{"content": v}}]}

def pick(props: dict, name: str, value: Optional[dict]):
    if name in N_SCHEMA_PROPS and value is not None:
        props[name] = value

def position_key(k: OpenKey) -> str:
    inst, side = k
    return f"{inst}|{side}"

async def load_db_schema(client: httpx.AsyncClient):
    """Load Notion DB schema and cache property names."""
    global N_SCHEMA_PROPS
    if not (NOTION_TOKEN and NOTION_DB_ID):
        return
    r = await client.get(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
    )
    r.raise_for_status()
    props = r.json().get("properties", {})
    N_SCHEMA_PROPS = set(props.keys())

async def notion_find_by_key(client: httpx.AsyncClient, key: str):
    """Return (page_id, status_name or None) for the first page with Trade Key == key."""
    if N_TRADE_KEY not in N_SCHEMA_PROPS:
        return None, None
    r = await client.post(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
        json={"filter": {"property": N_TRADE_KEY, "rich_text": {"equals": key}}, "page_size": 1}
    )
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        return None, None
    page = results[0]
    status_name = None
    try:
        status_name = page["properties"][N_STATUS]["select"]["name"]
    except Exception:
        pass
    return page["id"], status_name

async def notion_query_open_page(client: httpx.AsyncClient, inst: str, side: str) -> Optional[str]:
    """Legacy fallback lookup: Title + Status(Open) + Direction."""
    if not all(x in N_SCHEMA_PROPS for x in (N_TITLE, N_STATUS, N_DIR)):
        return None
    payload = {
        "filter": {
            "and": [
                {"property": N_TITLE,  "title":    {"equals": inst}},
                {"property": N_STATUS, "select":   {"equals": N_STATUS_OPEN}},
                {"property": N_DIR,    "select":   {"equals": "Long" if side == "Buy" else "Short"}},
            ]
        },
        "page_size": 1
    }
    r = await client.post(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
        json=payload
    )
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0]["id"] if results else None

async def notion_upsert_open(client: httpx.AsyncClient, k: OpenKey, rrow: dict) -> Optional[str]:
    """Create or update OPEN trade row in Notion; store Trade Key to link the future close."""
    inst, side = k
    key  = position_key(k)
    entry = row_entry_price(rrow)
    qty   = row_qty(rrow)
    lev   = row_leverage(rrow)
    mark  = row_mark(rrow)

    page_id = OPEN_PAGE_IDS.get(k)

    if not page_id:
        # Prefer Trade Key
        pid, status_name = await notion_find_by_key(client, key)
        if pid and status_name != N_STATUS_OPEN:
            # same key but already closed => treat as a new cycle, create fresh page
            page_id = None
        else:
            page_id = pid

        # Fallback
        if not page_id:
            page_id = await notion_query_open_page(client, inst, side)

    # Build properties
    props = {}
    pick(props, N_TITLE,   n_title(inst))
    pick(props, N_STATUS,  n_select(N_STATUS_OPEN))
    pick(props, N_DIR,     n_select("Long" if side == "Buy" else "Short"))
    pick(props, N_LEV,     n_num(lev))
    pick(props, N_QTY,     n_num(abs(qty) if qty is not None else None))
    pick(props, N_ENTRY,   n_num(entry))
    pick(props, N_MARK,    n_num(mark))
    pick(props, N_DATE_OPENED, n_date_ts(time.time()))
    if N_TRADE_KEY in N_SCHEMA_PROPS:
        pick(props, N_TRADE_KEY, n_rich(key))

    if page_id:
        r = await client.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
            json={"properties": props},
        )
        r.raise_for_status()
        OPEN_PAGE_IDS[k] = page_id
        return page_id
    else:
        r = await client.post(
            "https://api.notion.com/v1/pages",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
            json={"parent": {"database_id": NOTION_DB_ID}, "properties": props},
        )
        r.raise_for_status()
        page_id = r.json()["id"]
        OPEN_PAGE_IDS[k] = page_id
        return page_id

async def notion_close_page(client: httpx.AsyncClient, k: OpenKey, last_row: dict):
    """Update the existing open page to Closed (same Trade Key)."""
    inst, side = k
    key  = position_key(k)

    page_id = OPEN_PAGE_IDS.get(k)
    if not page_id:
        # Prefer Trade Key
        page_id, _ = await notion_find_by_key(client, key)
        # Fallback
        if not page_id:
            page_id = await notion_query_open_page(client, inst, side)
        if not page_id:
            return

    # Compute realized-ish pnl if derivable; otherwise leave to your formulas
    mark = row_mark(last_row)  # just clear mark on close
    props = {}
    pick(props, N_STATUS, n_select(N_STATUS_CLOSED))
    pick(props, N_MARK, n_num(None))
    pick(props, N_EXIT, n_num(mark))  # optional: if you want, pass last mark as proxy
    pick(props, N_DATE_CLOSED, n_date_ts(time.time()))

    r = await client.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
        json={"properties": props},
    )
    r.raise_for_status()
    # Remove from cache
    if k in OPEN_PAGE_IDS:
        del OPEN_PAGE_IDS[k]

# ========== Periodic Discord table ==========
async def periodic_refresh():
    if PERIOD_HOURS <= 0:
        return
    interval = PERIOD_HOURS * 3600.0
    await asyncio.sleep(interval)  # avoid double post at boot
    while True:
        if time.time() - LAST_POST_TIME >= 60:
            await post_table(set(LATEST_ROW.keys()), force=True)
        await asyncio.sleep(interval)

# ========== Main loop ==========
async def run():
    # background senders
    asyncio.create_task(discord_sender())
    asyncio.create_task(periodic_refresh())

    backoff = 1
    first_snapshot_done = False

    # Notion client
    notion_client = httpx.AsyncClient(timeout=20)
    if NOTION_TOKEN and NOTION_DB_ID:
        try:
            await load_db_schema(notion_client)
            print("Notion DB schema detected.")
        except Exception as e:
            print("Notion schema load failed:", e)

    global LAST_OPEN_KEYS

    while True:
        try:
            print(f"Connecting to BloFin WS… ({WS_URL})")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=5_000_000) as ws:
                # login
                await ws.send(json.dumps(ws_login_payload()))
                login_ack = json.loads(await ws.recv())
                if login_ack.get("event") == "error":
                    raise RuntimeError(f"Login failed: {login_ack}")

                # subscribe
                for sub in sub_payloads():
                    await ws.send(json.dumps(sub))
                print("✅ Connected & subscribed (orders/positions).")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("event") in ("subscribe","unsubscribe","info","pong","welcome"):
                        continue
                    if not is_push(msg):
                        continue
                    if msg.get("arg", {}).get("channel") != "positions":
                        continue

                    rows = msg.get("data", [])
                    keys_now = keys_from_rows(rows)

                    added   = sorted(list(keys_now - LAST_OPEN_KEYS))
                    removed = sorted(list(LAST_OPEN_KEYS - keys_now))

                    # Notion upserts/updates on *changes only*
                    if NOTION_TOKEN and NOTION_DB_ID:
                        for k in added:
                            rrow = LATEST_ROW.get(k, {})
                            try:
                                await notion_upsert_open(notion_client, k, rrow)
                            except Exception as e:
                                print("Notion open upsert error:", e)
                        for k in removed:
                            rrow = LATEST_ROW.get(k, {})
                            try:
                                await notion_close_page(notion_client, k, rrow)
                            except Exception as e:
                                print("Notion close update error:", e)

                    # Discord alerts (skip on the very first snapshot so we don't alert legacy positions)
                    if first_snapshot_done:
                        if added:
                            await send_alert(alert_block("🟢 New Trades", added))
                        if removed:
                            await send_alert(alert_block("🔴 Closed Trades", removed))
                            # purge cache for removed keys (kept long enough to render close alert)
                            for k in removed:
                                if k in LATEST_ROW:
                                    del LATEST_ROW[k]

                    # Post the table whenever the open/close set changes (or first snapshot)
                    if (not first_snapshot_done) or added or removed:
                        await post_table(keys_now, force=True)
                        LAST_OPEN_KEYS = keys_now
                        first_snapshot_done = True

        except websockets.exceptions.ConnectionClosedOK as e:
            print("WS closed OK:", e)
            await asyncio.sleep(2)
        except Exception as e:
            print("WS error:", e)
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
