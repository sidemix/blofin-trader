# main.py
# BloFin → Discord (Render Worker)
# - Table columns: SYMBOL | SIDE xLEV | AVG Entry Price | PNL | PNL%
# - Triggers:
#     • immediate on open/close (set change only)
#     • periodic refresh every PERIOD_HOURS (default 6h)
# - Ignores add/reduce, mark/liq/entry wiggles between updates.

import os, json, time, hmac, base64, hashlib, asyncio
from typing import Dict, Tuple, List

import websockets
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
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]

IS_DEMO = os.getenv("BLOFIN_IS_DEMO", "false").lower() == "true"
TABLE_TITLE = os.getenv("TABLE_TITLE", "Active BloFin Positions")
SIZE_EPS = float(os.getenv("SIZE_EPS", "0.00000001"))      # treat <= EPS as closed
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5"))
PERIOD_HOURS = float(os.getenv("PERIOD_HOURS", "6"))       # set to 0 to disable periodic posts

WS_URL = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

# ========== Discord sender (queue + 429) ==========
SEND_Q: asyncio.Queue = asyncio.Queue()

async def discord_sender():
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
                    await SEND_Q.put(content)  # retry same message
                else:
                    r.raise_for_status()
                    await asyncio.sleep(1.1)
            except Exception:
                await asyncio.sleep(2.0)
            finally:
                SEND_Q.task_done()

async def send_text(text: str):
    await SEND_Q.put(text)

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
    s = str(row.get("positionSide") or "").lower()
    if s in ("long", "buy"): return "Buy"
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

# ========== State ==========
OpenKey = Tuple[str, str]  # (instId, "Buy"/"Short")
LATEST_ROW: Dict[OpenKey, dict] = {}    # freshest row for display
LAST_OPEN_KEYS: set[OpenKey] = set()    # set used to detect open/close
LAST_POST_TIME: float = 0.0

def update_cache_and_keys(rows: List[dict]) -> set[OpenKey]:
    current: set[OpenKey] = set()
    for r in rows:
        if abs(parse_size(r)) <= SIZE_EPS:
            continue
        key = (str(r.get("instId")), infer_side(r))
        current.add(key)
        LATEST_ROW[key] = r
    # purge closed keys from cache
    for k in list(LATEST_ROW.keys()):
        if k not in current:
            del LATEST_ROW[k]
    return current

def format_table(keys: set[OpenKey]) -> str:
    headers = ["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]
    rows: List[List[str]] = []

    for key in sorted(keys):
        row = LATEST_ROW.get(key, {})
        inst, side = key
        lev  = row.get("leverage") or ""
        avg  = row.get("entryPrice") or row.get("avgPrice") or row.get("averagePrice") or ""
        pnl  = row.get("unrealizedPnl")
        pnlr = row.get("unrealizedPnlRatio")

        sidelev = f"{side} {fnum(lev,0)}x" if str(lev) not in ("", "0") else side
        rows.append([inst, sidelev, fnum(avg,6), fnum(pnl,6), fmt_signed_pct(pnlr)])

    # widths
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

async def post_table(keys: set[OpenKey], force: bool = False):
    global LAST_POST_TIME
    now = time.time()
    if not force and (now - LAST_POST_TIME) < SEND_MIN_INTERVAL:
        return
    await send_text(format_table(keys))
    LAST_POST_TIME = now

# ========== Periodic task (every PERIOD_HOURS) ==========

async def periodic_refresh():
    if PERIOD_HOURS <= 0:
        return
    interval = PERIOD_HOURS * 3600.0

    # Wait a full period before the first periodic post
    # so startup doesn't immediately double-post.
    await asyncio.sleep(interval)

    while True:
        keys_now = set(LATEST_ROW.keys())

        # Debounce: skip if we posted in the last 60s (e.g., an open/close just happened)
        if time.time() - LAST_POST_TIME >= 60:
            await post_table(keys_now, force=True)

        await asyncio.sleep(interval)


# ========== Main ==========
async def run():
    asyncio.create_task(discord_sender())
    asyncio.create_task(periodic_refresh())

    global LAST_OPEN_KEYS
    backoff = 1
    first_sent = False

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=5_000_000) as ws:
                # login
                await ws.send(json.dumps(ws_login_payload()))
                login_ack = json.loads(await ws.recv())
                if login_ack.get("event") == "error":
                    raise RuntimeError(f"Login failed: {login_ack}")

                # subscribe
                for sub in sub_payloads():
                    await ws.send(json.dumps(sub))

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
                    keys_now = update_cache_and_keys(rows)

                    # Only open/close triggers an immediate post
                    if (not first_sent) or (keys_now != LAST_OPEN_KEYS):
                        await post_table(keys_now, force=True)
                        LAST_OPEN_KEYS = keys_now
                        first_sent = True

        except Exception:
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
