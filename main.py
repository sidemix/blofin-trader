# main.py
# BloFin -> Discord + Notion (no duplicates)
# - Discord: "New Trades", "Closed Trades", and compact table of Active Positions (+ periodic refresh)
# - Notion: upsert by UID "<SYMBOL>|<SIDE>", create on open, update same page on close

import os, json, time, hmac, base64, hashlib, asyncio
from typing import Dict, Tuple, List, Optional

import websockets
import httpx

# =========================
# ENV / CONFIG
# =========================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- BloFin ---
API_KEY     = os.environ["BLOFIN_API_KEY"]
API_SECRET  = os.environ["BLOFIN_API_SECRET"]
PASSPHRASE  = os.environ["BLOFIN_PASSPHRASE"]
IS_DEMO     = os.getenv("BLOFIN_IS_DEMO", "false").lower() == "true"
WS_URL      = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

# --- Discord ---
WEBHOOK_TABLE  = os.getenv("DISCORD_WEBHOOK_TABLE")  or os.environ["DISCORD_WEBHOOK_URL"]
WEBHOOK_ALERTS = os.getenv("DISCORD_WEBHOOK_ALERTS") or os.environ["DISCORD_WEBHOOK_URL"]
TABLE_TITLE    = os.getenv("TABLE_TITLE", "Active BloFin Positions")

# --- Posting cadence ---
SIZE_EPS          = float(os.getenv("SIZE_EPS", "0.00000001"))  # <= EPS treated as closed
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5"))  # min seconds between table posts
PERIOD_HOURS      = float(os.getenv("PERIOD_HOURS", "6"))       # periodic table (0=off)

# --- Notion ---
NOTION_TOKEN       = os.getenv("NOTION_API_TOKEN") or os.getenv("NOTION_TOKEN")
NOTION_DB_ID       = os.environ["NOTION_DATABASE_ID"]
NOTION_VERSION     = os.getenv("NOTION_VERSION", "2022-06-28")

# Property names in your DB (match your Notion)
PROP_TITLE         = os.getenv("NOTION_PROP_TITLE", "Trade Name")
PROP_STATUS        = os.getenv("NOTION_PROP_STATUS", "Status")
OPEN_LABEL         = os.getenv("NOTION_STATUS_OPEN_LABEL", "1.Open")
CLOSED_LABEL       = os.getenv("NOTION_STATUS_CLOSED_LABEL", "3.Closed")

PROP_DIRECTION     = os.getenv("NOTION_PROP_DIRECTION", "Direction")
PROP_LEVERAGE      = os.getenv("NOTION_PROP_LEVERAGE", "Leverage")
PROP_QTY           = os.getenv("NOTION_PROP_QTY", "Qty")
PROP_ENTRY         = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
PROP_EXIT          = os.getenv("NOTION_PROP_EXIT", "Exit Price")
PROP_DATE_OPEN     = os.getenv("NOTION_PROP_DATE_OPEN", "Date Opened")
PROP_DATE_CLOSE    = os.getenv("NOTION_PROP_DATE_CLOSE", "Date Closed")
PROP_UID           = os.getenv("NOTION_PROP_UID", "UID")  # text (recommended)

# =========================
# Discord sender (queued)
# =========================
SEND_Q: asyncio.Queue = asyncio.Queue()

async def discord_sender():
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            url, content = await SEND_Q.get()
            try:
                r = await client.post(url, json={"content": content})
                if r.status_code == 429:
                    retry_after = 2.0
                    try:
                        retry_after = float(r.json().get("retry_after", 2.0))
                    except Exception:
                        pass
                    await asyncio.sleep(retry_after)
                    await SEND_Q.put((url, content))
                else:
                    r.raise_for_status()
                    await asyncio.sleep(1.0)
            except Exception:
                await asyncio.sleep(2.0)
            finally:
                SEND_Q.task_done()

async def send_to(url: str, text: str):
    await SEND_Q.put((url, text))

async def send_table(text: str): return await send_to(WEBHOOK_TABLE, text)
async def send_alert(text: str): return await send_to(WEBHOOK_ALERTS, text)

# =========================
# BloFin WS helpers
# =========================
def ws_login_payload() -> dict:
    ts = str(int(time.time() * 1000))
    nonce = ts
    msg = "/users/self/verify" + "GET" + ts + nonce
    hex_sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    sign_b64 = base64.b64encode(hex_sig).decode()
    return {"op": "login", "args": [{
        "apiKey": API_KEY, "passphrase": PASSPHRASE,
        "timestamp": ts, "nonce": nonce, "sign": sign_b64
    }]}

def sub_payloads() -> List[dict]:
    # positions stream; orders stream is optional here
    return [{"op": "subscribe", "args": [{"channel": "positions"}]}]

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

# =========================
# Format helpers
# =========================
def fnum(x, d=6):
    try:
        return f"{float(x):.{d}f}"
    except:
        return str(x or "")

def parse_float(x, default=0.0):
    try:
        return float(x)
    except:
        return default

def parse_size(row: dict) -> float:
    # BloFin payloads vary; try common names
    for k in ("positions", "pos", "size", "qty", "position"):
        if k in row:
            return parse_float(row.get(k))
    return 0.0

def infer_side(row: dict) -> str:
    s = str(row.get("positionSide") or row.get("side") or "").lower()
    if s in ("long","buy"):  return "Buy"
    if s in ("short","sell"): return "Short"
    sz = parse_size(row)
    return "Buy" if sz >= 0 else "Short"

def get_symbol(row: dict) -> str:
    return str(row.get("instId") or row.get("symbol") or row.get("instrument") or "").replace("/", "-")

def get_leverage(row: dict) -> float:
    return parse_float(row.get("leverage") or row.get("lever") or 0)

def get_avg_entry(row: dict) -> float:
    for k in ("entryPrice","avgPrice","averagePrice","avg_entry"):
        if k in row: return parse_float(row[k])
    return 0.0

def get_mark_price(row: dict) -> float:
    for k in ("markPrice","marketPrice","last","price"):
        if k in row: return parse_float(row[k])
    return 0.0

def get_unreal_pnl(row: dict) -> float:
    for k in ("unrealizedPnl","unrealPnl","pnl"):
        if k in row: return parse_float(row[k])
    return 0.0

def get_unreal_pnl_ratio(row: dict) -> float:
    for k in ("unrealizedPnlRatio","pnlRatio","roe"):
        if k in row: return parse_float(row[k])
    return 0.0

def fmt_pct_from_ratio(ratio: float) -> str:
    sign = "+" if ratio >= 0 else ""
    return f"{sign}{ratio*100.0:.2f}%"

# =========================
# State (for Discord flow)
# =========================
OpenKey = Tuple[str, str]  # (symbol, "Buy"/"Short")

LATEST_ROW: Dict[OpenKey, dict] = {}     # freshest row for display & Notion close exit
LAST_OPEN_KEYS: set[OpenKey] = set()
LAST_POST_TIME: float = 0.0

def keys_from_rows(rows: List[dict]) -> set[OpenKey]:
    keys: set[OpenKey] = set()
    for r in rows:
        if abs(parse_size(r)) <= SIZE_EPS:
            continue
        k = (get_symbol(r), infer_side(r))
        keys.add(k)
        LATEST_ROW[k] = r
    return keys

# =========================
# Discord formatting
# =========================
def table_for_keys(keys: set[OpenKey]) -> str:
    headers = ["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]
    rows: List[List[str]] = []
    for k in sorted(keys):
        r = LATEST_ROW.get(k, {})
        symbol, side = k
        lev  = get_leverage(r)
        avg  = get_avg_entry(r)
        pnl  = get_unreal_pnl(r)
        pnlr = get_unreal_pnl_ratio(r)
        sidelev = f"{side} {fnum(lev,0)}x" if lev else side
        rows.append([symbol, sidelev, fnum(avg,6), fnum(pnl,6), fmt_pct_from_ratio(pnlr)])

    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]
    def fmt_row(r): return "  ".join(str(v).ljust(w) for v, w in zip(r, widths))

    lines = [f"**{TABLE_TITLE}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if rows:
        for rr in rows: lines.append(fmt_row(rr))
    else:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

def alert_block(title: str, keys: List[OpenKey]) -> str:
    headers = ["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]
    rows: List[List[str]] = []
    for k in keys:
        r = LATEST_ROW.get(k, {})
        symbol, side = k
        lev  = get_leverage(r)
        avg  = get_avg_entry(r)
        pnl  = get_unreal_pnl(r)
        pnlr = get_unreal_pnl_ratio(r)
        sidelev = f"{side} {fnum(lev,0)}x" if lev else side
        rows.append([symbol, sidelev, fnum(avg,6), fnum(pnl,6), fmt_pct_from_ratio(pnlr)])

    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]
    def fmt_row(r): return "  ".join(str(v).ljust(w) for v, w in zip(r, widths))

    lines = [f"**{title}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if rows:
        for rr in rows: lines.append(fmt_row(rr))
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

async def periodic_refresh():
    if PERIOD_HOURS <= 0:
        return
    interval = PERIOD_HOURS * 3600.0
    await asyncio.sleep(interval)
    while True:
        if time.time() - LAST_POST_TIME >= 60:
            await post_table(set(LATEST_ROW.keys()), force=True)
        await asyncio.sleep(interval)

# =========================
# Notion client
# =========================
class Notion:
    def __init__(self, token: str, version: str):
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": version,
            "Content-Type": "application/json",
        }
        self.client = httpx.AsyncClient(timeout=20)

        # open UID -> page_id
        self.open_pages: Dict[str, str] = {}
        self.has_uid_prop: bool = True  # assume; validate on schema load

    async def close(self):
        await self.client.aclose()

    async def _get_db_schema(self, db_id: str) -> dict:
        r = await self.client.get(f"https://api.notion.com/v1/databases/{db_id}", headers=self.headers)
        r.raise_for_status()
        return r.json()

    async def load_schema_and_opens(self, db_id: str):
        # schema
        try:
            schema = await self._get_db_schema(db_id)
            props = schema.get("properties", {})
            self.has_uid_prop = (PROP_UID in props and props[PROP_UID]["type"] in ("rich_text","title","url","email","phone_number"))
        except Exception:
            self.has_uid_prop = True  # be permissive

        # existing opens -> map UID -> page_id
        start = None
        self.open_pages.clear()

        while True:
            body = {
                "filter": {
                    "and": [
                        {"property": PROP_STATUS, "select": {"equals": OPEN_LABEL}},
                    ]
                }
            }
            if start: body["start_cursor"] = start
            r = await self.client.post(
                f"https://api.notion.com/v1/databases/{db_id}/query",
                headers=self.headers, json=body
            )
            r.raise_for_status()
            data = r.json()
            for page in data.get("results", []):
                props = page.get("properties", {})
                uid_val = ""
                if self.has_uid_prop and PROP_UID in props and "rich_text" in props[PROP_UID]:
                    arr = props[PROP_UID]["rich_text"]
                    if arr and "plain_text" in arr[0]:
                        uid_val = arr[0]["plain_text"]

                if not uid_val:
                    # fallback: Title + Direction
                    title = ""
                    if PROP_TITLE in props and "title" in props[PROP_TITLE] and props[PROP_TITLE]["title"]:
                        title = props[PROP_TITLE]["title"][0].get("plain_text","")
                    direction = ""
                    if PROP_DIRECTION in props and "select" in props[PROP_DIRECTION] and props[PROP_DIRECTION]["select"]:
                        direction = props[PROP_DIRECTION]["select"].get("name","")
                    uid_val = f"{title}|{direction or 'Long'}"

                self.open_pages[uid_val] = page["id"]

            if data.get("has_more"):
                start = data.get("next_cursor")
            else:
                break

    def uid_for(self, symbol: str, side: str) -> str:
        # side is "Buy"/"Short" -> convert to "Long"/"Short" for Notion
        notion_side = "Long" if side == "Buy" else "Short"
        return f"{symbol}|{notion_side}"

    async def create_open(self, db_id: str, uid: str, symbol: str, side: str, lev: float, qty: float, entry: float):
        props = {
            PROP_TITLE: {"title": [{"text": {"content": symbol}}]},
            PROP_STATUS: {"select": {"name": OPEN_LABEL}},
            PROP_DIRECTION: {"select": {"name": ("Long" if side == "Buy" else "Short")}},
            PROP_LEVERAGE: {"number": float(lev) if lev else None},
            PROP_QTY: {"number": float(qty) if qty else None},
            PROP_ENTRY: {"number": float(entry) if entry else None},
            PROP_DATE_OPEN: {"date": {"start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}},
        }
        if self.has_uid_prop:
            props[PROP_UID] = {"rich_text": [{"text": {"content": uid}}]}

        r = await self.client.post(
            "https://api.notion.com/v1/pages",
            headers=self.headers,
            json={"parent": {"database_id": db_id}, "properties": props}
        )
        r.raise_for_status()
        page_id = r.json()["id"]
        self.open_pages[uid] = page_id
        return page_id

    async def close_trade(self, db_id: str, uid: str, exit_price: float):
        page_id = self.open_pages.get(uid)
        if not page_id:
            return  # nothing to close (e.g., we restarted after the close)

        props = {
            PROP_STATUS: {"select": {"name": CLOSED_LABEL}},
            PROP_EXIT: {"number": float(exit_price) if exit_price else None},
            PROP_DATE_CLOSE: {"date": {"start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}},
        }
        r = await self.client.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=self.headers, json={"properties": props}
        )
        r.raise_for_status()
        # remove from open map after closing
        self.open_pages.pop(uid, None)

# =========================
# Notion wiring helpers
# =========================
notion = Notion(NOTION_TOKEN, NOTION_VERSION)

async def notion_bootstrap():
    # Load schema & all open pages so closes hit the right page
    await notion.load_schema_and_opens(NOTION_DB_ID)

async def notion_on_open(symbol: str, side: str, lev: float, qty: float, entry: float):
    uid = notion.uid_for(symbol, side)
    if uid in notion.open_pages:
        return  # already present
    try:
        await notion.create_open(NOTION_DB_ID, uid, symbol, side, lev, qty, entry)
    except Exception as e:
        # non-fatal
        pass

async def notion_on_close(symbol: str, side: str, exit_price: float):
    uid = notion.uid_for(symbol, side)
    try:
        await notion.close_trade(NOTION_DB_ID, uid, exit_price)
    except Exception:
        pass

# =========================
# Main loop
# =========================
async def run():
    # background senders
    asyncio.create_task(discord_sender())
    asyncio.create_task(periodic_refresh())

    # prepare Notion maps
    await notion_bootstrap()

    global LAST_OPEN_KEYS
    first_snapshot = True
    backoff = 1

    while True:
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=20, max_size=5_000_000
            ) as ws:
                await ws.send(json.dumps(ws_login_payload()))
                login_ack = json.loads(await ws.recv())
                if login_ack.get("event") == "error":
                    raise RuntimeError(f"BloFin login failed: {login_ack}")

                for sub in sub_payloads():
                    await ws.send(json.dumps(sub))

                print("✅ Connected & subscribed (orders/positions).")
                backoff = 1

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

                    added = sorted(list(keys_now - LAST_OPEN_KEYS))
                    removed = sorted(list(LAST_OPEN_KEYS - keys_now))

                    # --- Notion + Discord new
                    if not first_snapshot and added:
                        # Notion create open per added
                        for (symbol, side) in added:
                            r = LATEST_ROW.get((symbol, side), {})
                            await notion_on_open(
                                symbol, side,
                                get_leverage(r), parse_size(r), get_avg_entry(r)
                            )
                        await send_alert(alert_block("🟢 New Trades", added))

                    # --- Notion + Discord closed
                    if not first_snapshot and removed:
                        # Use last cached mark as exit
                        for (symbol, side) in removed:
                            r = LATEST_ROW.get((symbol, side), {})
                            exit_price = get_mark_price(r) or get_avg_entry(r)
                            await notion_on_close(symbol, side, exit_price)
                            # keep cache row for the Discord message block formatting
                        await send_alert(alert_block("🔴 Closed Trades", removed))
                        # finally purge local cache for closed keys
                        for k in removed:
                            if k in LATEST_ROW: del LATEST_ROW[k]

                    # Table on any set change (or first snapshot)
                    if first_snapshot or added or removed:
                        await post_table(keys_now, force=True)
                        LAST_OPEN_KEYS = keys_now
                        first_snapshot = False

        except websockets.exceptions.ConnectionClosedOK:
            print("WS closed (OK). Reconnecting...")
        except Exception as e:
            print(f"WS error: {e}")
        # reconnect backoff
        await asyncio.sleep(min(backoff, 30))
        backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    finally:
        try:
            asyncio.run(notion.close())
        except Exception:
            pass
