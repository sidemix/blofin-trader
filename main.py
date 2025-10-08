# main.py
# BloFin → Discord (+ Notion)
# - Discord: table + 🟢 New / 🔴 Closed alerts (same format you liked)
# - Notion: upsert on open, close on closed, periodic mark updates
# - NEW: BloFin app-level heartbeat {"op":"ping"} to prevent 1000 closes

import os, json, time, hmac, base64, hashlib, asyncio
from typing import Dict, Tuple, List, Optional

import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
import httpx

# ========== ENV ==========
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

API_KEY = os.environ["BLOFIN_API_KEY"]
API_SECRET = os.environ["BLOFIN_API_SECRET"]
PASSPHRASE = os.environ["BLOFIN_PASSPHRASE"]

# Discord webhooks
WEBHOOK_TABLE  = os.getenv("DISCORD_WEBHOOK_TABLE")  or os.environ["DISCORD_WEBHOOK_URL"]
WEBHOOK_ALERTS = os.getenv("DISCORD_WEBHOOK_ALERTS") or os.environ["DISCORD_WEBHOOK_URL"]

IS_DEMO      = os.getenv("BLOFIN_IS_DEMO", "false").lower() == "true"
TABLE_TITLE  = os.getenv("TABLE_TITLE", "Active BloFin Positions")
SIZE_EPS     = float(os.getenv("SIZE_EPS", "0.00000001"))
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5"))
PERIOD_HOURS = float(os.getenv("PERIOD_HOURS", "6"))  # 0 = disable periodic

# NEW: app-level WS heartbeat (seconds)
HEARTBEAT_SEC = float(os.getenv("HEARTBEAT_SEC", "15"))

WS_URL = (
    "wss://demo-trading-openapi.blofin.com/ws/private"
    if IS_DEMO else
    "wss://openapi.blofin.com/ws/private"
)

# ---------- Notion ENV ----------
NOTION_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DATABASE_ID")
N_VER = os.getenv("NOTION_VERSION", "2022-06-28")

# property names (map to your DB)
N_TITLE       = os.getenv("NOTION_PROP_TITLE", "Trade Name")
N_STATUS      = os.getenv("NOTION_PROP_STATUS", "Status")
N_DIR         = os.getenv("NOTION_PROP_DIRECTION", "Direction")
N_LEV         = os.getenv("NOTION_PROP_LEVERAGE", "Leverage")
N_QTY         = os.getenv("NOTION_PROP_QTY", "Qty")
N_ENTRY       = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
N_EXIT        = os.getenv("NOTION_PROP_EXIT", "Exit Price")
N_MARK        = os.getenv("NOTION_PROP_MARK", "Market Price")
N_DATE_OPEN   = os.getenv("NOTION_PROP_DATE_OPEN", "Date Opened")
N_DATE_CLOSE  = os.getenv("NOTION_PROP_DATE_CLOSE", "Date Closed")

N_STATUS_OPEN   = os.getenv("NOTION_STATUS_OPEN_LABEL", "1.Open")
N_STATUS_CLOSED = os.getenv("NOTION_STATUS_CLOSED_LABEL", "3.Closed")

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
                await asyncio.sleep(2.0)
            finally:
                SEND_Q.task_done()

async def send_to(url: str, text: str):
    await SEND_Q.put((url, text))

async def send_table(text: str):   return await send_to(WEBHOOK_TABLE, text)
async def send_alert(text: str):   return await send_to(WEBHOOK_ALERTS, text)

# ========== WS auth/sub ==========
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

def sub_payloads() -> List[dict]:
    return [{"op": "subscribe", "args": [{"channel": "positions"}]}]

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

# ========== Helpers ==========
def fnum(x, d=6):
    try:
        return f"{float(x):.{d}f}"
    except:
        return str(x or "")

def parse_size(row: dict) -> float:
    s = row.get("positions") or row.get("pos") or row.get("size") or 0
    try:
        return float(s)
    except:
        return 0.0

def infer_side(row: dict) -> str:
    s = str(row.get("positionSide") or row.get("posSide") or "").lower()
    if s in ("long", "buy"):  return "Buy"
    if s in ("short", "sell"): return "Short"
    sz = parse_size(row)
    if sz > 0: return "Buy"
    if sz < 0: return "Short"
    return "Buy"

def fmt_signed_pct(ratio) -> str:
    try:
        v = float(ratio) * 100.0
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.2f}%"
    except:
        return ""

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")

# ========== State ==========
OpenKey = Tuple[str, str]  # (instId, "Buy"/"Short")
LATEST_ROW: Dict[OpenKey, dict] = {}    # freshest row for each open key
LAST_OPEN_KEYS: set[OpenKey] = set()    # for open/close detection
LAST_POST_TIME: float = 0.0

# cache: Notion page id per open position
OPEN_PAGE_IDS: Dict[OpenKey, str] = {}

def keys_from_rows(rows: List[dict]) -> set[OpenKey]:
    keys: set[OpenKey] = set()
    for r in rows:
        if abs(parse_size(r)) <= SIZE_EPS:
            continue
        k = (str(r.get("instId")), infer_side(r))
        keys.add(k)
        LATEST_ROW[k] = r
    return keys

def row_entry_price(r: dict) -> Optional[float]:
    for k in ("entryPrice", "avgPrice", "averagePrice", "avgEntryPrice"):
        v = r.get(k)
        if v is not None:
            try: return float(v)
            except: pass
    return None

def row_mark(r: dict) -> Optional[float]:
    for k in ("markPrice", "markPx", "mark", "last"):
        v = r.get(k)
        if v is not None:
            try: return float(v)
            except: pass
    return None

def row_leverage(r: dict) -> Optional[float]:
    v = r.get("leverage") or r.get("lev")
    try: return float(v) if v is not None else None
    except: return None

def row_qty(r: dict) -> Optional[float]:
    try:
        return float(r.get("positions") or r.get("pos") or r.get("size") or 0.0)
    except:
        return None

# ========== Formatting (Discord) ==========
def table_for_keys(keys: set[OpenKey]) -> str:
    headers = ["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]
    rows: List[List[str]] = []
    for k in sorted(keys):
        r = LATEST_ROW.get(k, {})
        inst, side = k
        lev  = row_leverage(r) or ""
        avg  = row_entry_price(r) or ""
        pnl  = r.get("unrealizedPnl")
        pnlr = r.get("unrealizedPnlRatio")
        sidelev = f"{side} {fnum(lev,0)}x" if str(lev) not in ("", "0") else side
        rows.append([inst, sidelev, fnum(avg,6), fnum(pnl,6), fmt_signed_pct(pnlr)])

    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]
    def fmt_row(r): return "  ".join(str(v).ljust(w) for v, w in zip(r, widths))

    lines = [f"**{TABLE_TITLE}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if rows:
        for r in rows:
            lines.append(fmt_row(r))
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
        lev  = row_leverage(r) or ""
        avg  = row_entry_price(r) or ""
        pnl  = r.get("unrealizedPnl")
        pnlr = r.get("unrealizedPnlRatio")
        sidelev = f"{side} {fnum(lev,0)}x" if str(lev) not in ("", "0") else side
        rows.append([inst, sidelev, fnum(avg,6), fnum(pnl,6), fmt_signed_pct(pnlr)])

    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]
    def fmt_row(r): return "  ".join(str(v).ljust(w) for v, w in zip(r, widths))

    lines = [f"**{title}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if rows:
        for r in rows:
            lines.append(fmt_row(r))
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
def have_notion() -> bool:
    return bool(NOTION_TOKEN and NOTION_DB_ID)

N_SCHEMA_PROPS: set[str] = set()

def n_title(v: str): return {"title": [{"text": {"content": v}}]}
def n_num(v: Optional[float]): return {"number": (float(v) if v is not None else None)}
def n_select(name: Optional[str]): return {"select": ({"name": name} if name else None)}
def n_date_iso(iso: Optional[str]): return {"date": ({"start": iso} if iso else None)}

def pick(props: dict, name: str, val: dict):
    if name in N_SCHEMA_PROPS:
        props[name] = val

async def load_db_schema(client: httpx.AsyncClient):
    if not have_notion(): return
    r = await client.get(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER}
    )
    r.raise_for_status()
    data = r.json()
    N_SCHEMA_PROPS.clear()
    N_SCHEMA_PROPS.update(list(data.get("properties", {}).keys()))

async def notion_query_open_page(client: httpx.AsyncClient, inst: str, side: str) -> Optional[str]:
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    filt = {
        "and": [
            {"property": N_TITLE,  "title": {"equals": inst}},
            {"property": N_STATUS, "select": {"equals": N_STATUS_OPEN}},
            {"property": N_DIR,    "select": {"equals": side}},
        ]
    }
    r = await client.post(
        url,
        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
        json={"filter": filt, "page_size": 1},
    )
    r.raise_for_status()
    res = r.json()
    results = res.get("results", [])
    if results:
        return results[0]["id"]
    return None

async def notion_upsert_open(client: httpx.AsyncClient, k: OpenKey, r: dict) -> str:
    inst, side = k
    entry = row_entry_price(r)
    qty   = row_qty(r)
    lev   = row_leverage(r)
    mark  = row_mark(r)

    page_id = OPEN_PAGE_IDS.get(k)
    if not page_id:
        page_id = await notion_query_open_page(client, inst, side)

    props = {}
    pick(props, N_TITLE, n_title(inst))
    pick(props, N_STATUS, n_select(N_STATUS_OPEN))
    pick(props, N_DIR, n_select("Long" if side == "Buy" else "Short"))
    pick(props, N_LEV, n_num(lev))
    pick(props, N_QTY, n_num(abs(qty) if qty is not None else None))
    pick(props, N_ENTRY, n_num(entry))
    pick(props, N_MARK, n_num(mark))
    pick(props, N_DATE_OPEN, n_date_iso(now_iso()))

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
    inst, side = k
    page_id = OPEN_PAGE_IDS.get(k)
    if not page_id:
        page_id = await notion_query_open_page(client, inst, side)
        if not page_id:
            return

    exitp = row_mark(last_row) or row_entry_price(last_row)
    props = {}
    pick(props, N_STATUS, n_select(N_STATUS_CLOSED))
    pick(props, N_EXIT, n_num(exitp))
    pick(props, N_MARK, n_num(None))
    pick(props, N_DATE_CLOSE, n_date_iso(now_iso()))

    r = await client.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
        json={"properties": props},
    )
    r.raise_for_status()
    if k in OPEN_PAGE_IDS:
        del OPEN_PAGE_IDS[k]

async def notion_refresh_marks(client: httpx.AsyncClient, keys: set[OpenKey]):
    if not have_notion(): return
    for k in keys:
        page_id = OPEN_PAGE_IDS.get(k) or await notion_query_open_page(client, k[0], k[1])
        if not page_id:
            continue
        r = LATEST_ROW.get(k, {})
        mark = row_mark(r)
        if mark is None:
            continue
        props = {}
        pick(props, N_MARK, n_num(mark))
        try:
            resp = await client.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": N_VER},
                json={"properties": props},
            )
            resp.raise_for_status()
        except Exception:
            await asyncio.sleep(0.2)

# ========== Periodic table refresh ==========
async def periodic_refresh(notion_client: Optional[httpx.AsyncClient]):
    if PERIOD_HOURS <= 0:
        return
    interval = PERIOD_HOURS * 3600.0
    await asyncio.sleep(interval)

    while True:
        keys_now = set(LATEST_ROW.keys())
        if time.time() - LAST_POST_TIME >= 60:
            await post_table(keys_now, force=True)
        if have_notion() and notion_client is not None:
            await notion_refresh_marks(notion_client, keys_now)
        await asyncio.sleep(interval)

# ========== Heartbeat ==========
async def ws_heartbeat(ws: websockets.WebSocketClientProtocol, stop_evt: asyncio.Event):
    # BloFin expects an application-level heartbeat like {"op":"ping"}
    # Send every HEARTBEAT_SEC seconds until stop_evt is set
    while not stop_evt.is_set():
        try:
            await ws.send(json.dumps({"op": "ping"}))
        except Exception:
            # let the main loop handle reconnect
            return
        await asyncio.sleep(HEARTBEAT_SEC)

# ========== Main ==========
async def run():
    notion_client: Optional[httpx.AsyncClient] = None
    if have_notion():
        notion_client = httpx.AsyncClient(timeout=20)
        try:
            await load_db_schema(notion_client)
            print("Notion DB schema detected.")
        except Exception as e:
            print(f"Notion schema load failed: {e}")

    asyncio.create_task(discord_sender())
    asyncio.create_task(periodic_refresh(notion_client))

    global LAST_OPEN_KEYS
    backoff = 1
    first_sent = False

    while True:
        try:
            print(f"Connecting to BloFin WS… ({WS_URL})")
            # IMPORTANT: disable library-level pings; we do app-level ping
            async with websockets.connect(
                WS_URL, ping_interval=None, ping_timeout=None, max_size=5_000_000
            ) as ws:
                # login
                await ws.send(json.dumps(ws_login_payload()))
                login_ack = json.loads(await ws.recv())
                if login_ack.get("event") == "error":
                    raise RuntimeError(f"Login failed: {login_ack}")

                # subscribe
                for sub in sub_payloads():
                    await ws.send(json.dumps(sub))
                print("✅ Connected & subscribed (orders/positions).")

                # start heartbeat
                stop_hb = asyncio.Event()
                hb_task = asyncio.create_task(ws_heartbeat(ws, stop_hb))

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # ignore non-data frames
                        if msg.get("event") in ("subscribe","unsubscribe","info","pong","welcome"):
                            continue
                        if msg.get("op") in ("pong",):  # some servers reply as {"op":"pong"}
                            continue

                        if not is_push(msg):
                            continue
                        if msg.get("arg", {}).get("channel") != "positions":
                            continue

                        rows = msg.get("data", [])
                        keys_now = keys_from_rows(rows)

                        added = sorted(list(keys_now - LAST_OPEN_KEYS))
                        removed = sorted(list(LAST_OPEN_KEYS - keys_now))

                        # Notion updates
                        if have_notion() and notion_client is not None:
                            for k in added:
                                try:
                                    await notion_upsert_open(notion_client, k, LATEST_ROW.get(k, {}))
                                except Exception as e:
                                    print(f"Notion upsert open failed {k}: {e}")
                            for k in removed:
                                last_row = LATEST_ROW.get(k, {})
                                try:
                                    await notion_close_page(notion_client, k, last_row)
                                except Exception as e:
                                    print(f"Notion close failed {k}: {e}")

                        # Discord alerts
                        if first_sent:
                            if added:
                                await send_alert(alert_block("🟢 New Trades", added))
                            if removed:
                                await send_alert(alert_block("🔴 Closed Trades", removed))
                                for k in removed:
                                    if k in LATEST_ROW:
                                        del LATEST_ROW[k]

                        # Table when set changes or first snapshot
                        if (not first_sent) or added or removed:
                            await post_table(keys_now, force=True)
                            LAST_OPEN_KEYS = keys_now
                            first_sent = True

                finally:
                    # stop heartbeat when leaving connection context
                    stop_hb.set()
                    try:
                        hb_task.cancel()
                    except Exception:
                        pass

        except (ConnectionClosedOK, ConnectionClosedError) as e:
            # 1000 OK or other closure — reconnect quickly
            print(f"WS closed: {e}. Reconnecting…")
            await asyncio.sleep(2)
            backoff = 1
            continue
        except Exception as e:
            print(f"WS loop error: {e}")
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

    # (never reached in a worker)
    if notion_client is not None:
        await notion_client.aclose()

if __name__ == "__main__":
    asyncio.run(run())
