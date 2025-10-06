# main.py
# BloFin → Discord (Render Worker)
# Posts ONE compact "Active BloFin Positions" table.
# Triggers:
#   • Immediate when a position is opened or closed (set change)
#   • Every PERIOD_HOURS (default 6h) to show ΔPNL since last periodic post
# Columns:
#   SYMBOL | SIDE xLEV | AVG Entry Price | PNL | PNL% | (ΔPNL(6h) on periodic posts)

import os, json, time, hmac, base64, hashlib, asyncio
from typing import Optional, Dict, Tuple, List

import websockets
import httpx

# ---------- ENV ----------
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

# Treat <= EPS size as closed (prevents float jitter)
SIZE_EPS = float(os.getenv("SIZE_EPS", "0.00000001"))
# Minimum seconds between two back-to-back posts (anti-burst)
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5"))
# Periodic refresh interval in hours (for ΔPNL)
PERIOD_HOURS = float(os.getenv("PERIOD_HOURS", "6"))

WS_URL = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

# ---------- Discord sender (queue + 429) ----------
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
                    await SEND_Q.put(content)
                else:
                    r.raise_for_status()
                    await asyncio.sleep(1.1)   # gentle pace ~1 msg/sec
            except Exception:
                await asyncio.sleep(2.0)
            finally:
                SEND_Q.task_done()

async def send_text(text: str):
    await SEND_Q.put(text)

# ---------- WS auth/sub ----------
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

# ---------- Helpers ----------
def fnum(x, d=6):
    try: return f"{float(x):.{d}f}"
    except: return str(x or "")

def parse_size(row: dict) -> float:
    s = row.get("positions") or row.get("pos") or row.get("size") or 0
    try: return float(s)
    except: return 0.0

def infer_side(row: dict) -> str:
    s = str(row.get("positionSide") or "").lower()
    if s in ("long", "buy"): return "Buy"
    if s in ("short", "sell"): return "Short"
    sz = parse_size(row)
    if sz > 0: return "Buy"
    if sz < 0: return "Short"
    return "Buy"

# ---------- State we maintain ----------
OpenKey = Tuple[str, str]  # (instId, "Buy"/"Short")

# Latest row we’ve seen per open key (for display)
LATEST_ROW: Dict[OpenKey, dict] = {}
# Set of open keys (for structural change detection)
LAST_OPEN_KEYS: set[OpenKey] = set()
# PNL baseline for ΔPNL on periodic posts
PNL_BASELINE: Dict[OpenKey, float] = {}

LAST_POST_TIME: float = 0.0

def extract_open_keys_and_update_cache(rows: List[dict]) -> set[OpenKey]:
    """Update LATEST_ROW for open positions and return the set of open keys right now."""
    current: set[OpenKey] = set()
    for r in rows:
        if abs(parse_size(r)) <= SIZE_EPS:
            continue
        key = (str(r.get("instId")), infer_side(r))
        current.add(key)
        LATEST_ROW[key] = r  # keep the freshest row for display
    # purge cache entries for keys that are no longer open
    for k in list(LATEST_ROW.keys()):
        if k not in current:
            del LATEST_ROW[k]
    return current

def format_table(keys: set[OpenKey], include_delta: bool) -> str:
    # Headers updated per request
    headers = ["SYMBOL", "SIDE xLEV", "AVG Entry Price", "PNL", "PNL%"]
    if include_delta:
        headers.append("ΔPNL(6h)")

    rows: List[List[str]] = []
    for key in sorted(keys):
        row = LATEST_ROW.get(key, {})
        inst, side = key
        lev = row.get("leverage") or ""
        avg = row.get("entryPrice") or row.get("avgPrice") or row.get("averagePrice") or ""
        pnl = row.get("unrealizedPnl")
        pnlr = row.get("unrealizedPnlRatio")

        # Format values
        sidelev = f"{side} {fnum(lev,0)}x" if str(lev) not in ("", "0") else side
        pnl_pct = ""
        try:
            pnl_pct = f"{float(pnlr)*100:.2f}%"
        except Exception:
            pnl_pct = str(pnlr or "")

        rec = [inst, sidelev, fnum(avg,6), fnum(pnl,6), pnl_pct]

        if include_delta:
            base = PNL_BASELINE.get(key, None)
            try:
                cur = float(pnl) if pnl not in (None, "") else 0.0
            except:
                cur = 0.0
            if base is None:
                delta_str = "—"
            else:
                d = cur - base
                sign = "+" if d >= 0 else ""
                delta_str = f"{sign}{d:.6f}"
            rec.append(delta_str)

        rows.append(rec)

    # widths
    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]
    def fmt_row(row): return "  ".join(str(v).ljust(w) for v, w in zip(row, widths))

    lines = [f"**{TABLE_TITLE}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if rows:
        for r in rows:
            lines.append(fmt_row(r))
    else:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

async def post_table(keys: set[OpenKey], include_delta: bool, force: bool = False):
    global LAST_POST_TIME
    now = time.time()
    if not force and (now - LAST_POST_TIME) < SEND_MIN_INTERVAL:
        return
    text = format_table(keys, include_delta=include_delta)
    await send_text(text)
    LAST_POST_TIME = now

# ---------- Background periodic task ----------
async def periodic_pnl_updates():
    """Every PERIOD_HOURS, post a table with ΔPNL and refresh the baseline."""
    global PNL_BASELINE
    await asyncio.sleep(10)  # allow initial cache fill
    interval = max(0.1, PERIOD_HOURS) * 3600.0
    while True:
        keys_now = set(LATEST_ROW.keys())
        await post_table(keys_now, include_delta=True, force=True)
        # Refresh ΔPNL baselines
        new_baseline: Dict[OpenKey, float] = {}
        for k in keys_now:
            pnl = LATEST_ROW.get(k, {}).get("unrealizedPnl")
            try:
                new_baseline[k] = float(pnl) if pnl not in (None, "") else 0.0
            except:
                new_baseline[k] = 0.0
        PNL_BASELINE = new_baseline
        await asyncio.sleep(interval)

# ---------- Main loop ----------
async def run():
    asyncio.create_task(discord_sender())
    asyncio.create_task(periodic_pnl_updates())

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
                    keys_now = extract_open_keys_and_update_cache(rows)

                    # Post only on open/close (set change)
                    if (not first_sent) or (keys_now != LAST_OPEN_KEYS):
                        await post_table(keys_now, include_delta=True, force=True)
                        LAST_OPEN_KEYS = keys_now
                        first_sent = True

        except Exception:
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
