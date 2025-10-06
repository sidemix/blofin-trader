# main.py
# BloFin → Discord (Render Worker)
# Posts ONE compact "Active Positions" table ONLY when a trade is opened or closed.
# Shows: SYMBOL, Buy/Short + Leverage, AVG, uPNL
# Ignores all other changes (size adds/reduces, mark, liq, etc.).

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
SIZE_EPS = float(os.getenv("SIZE_EPS", "0.00000001"))          # treat <= EPS as closed
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5")) # basic anti-burst

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
                    await SEND_Q.put(content)   # retry same message
                else:
                    r.raise_for_status()
                    await asyncio.sleep(1.1)    # gentle pacing
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
    # Prefer explicit side if provided
    s = str(row.get("positionSide") or "").lower()
    if s in ("long", "buy"):
        return "Buy"
    if s in ("short", "sell"):
        return "Short"
    # Fallback: sign of size
    sz = parse_size(row)
    if sz > 0:  return "Buy"
    if sz < 0:  return "Short"
    return "Buy"  # default when unknown (won't matter for closed)

# ---------- Open-set tracking ----------
# Key is (instId, normalizedSide "Buy"/"Short")
OpenKey = Tuple[str, str]
LAST_OPEN_KEYS: set[OpenKey] = set()
LAST_POST_TIME: float = 0.0

def current_open_keys(rows: List[dict]) -> set[OpenKey]:
    keys: set[OpenKey] = set()
    for r in rows:
        sz = parse_size(r)
        if abs(sz) <= SIZE_EPS:
            continue
        inst = str(r.get("instId"))
        side = infer_side(r)
        keys.add((inst, side))
    return keys

def format_table(rows: List[dict]) -> str:
    # Keep only one latest row per (inst, side) for display values
    latest: Dict[OpenKey, dict] = {}
    for r in rows:
        sz = parse_size(r)
        if abs(sz) <= SIZE_EPS:  # skip closed entries
            continue
        key = (str(r.get("instId")), infer_side(r))
        latest[key] = r

    headers = ["SYMBOL", "SIDE xLEV", "AVG", "PNL"]
    data_rows: List[List[str]] = []

    for (inst, side) in sorted(latest.keys()):
        r = latest[(inst, side)]
        lev = r.get("leverage") or ""
        avg = r.get("entryPrice") or r.get("avgPrice") or r.get("averagePrice") or ""
        pnl = r.get("unrealizedPnl")
        side_lev = f"{side} {fnum(lev, 0)}x" if str(lev) not in ("", "0") else side
        data_rows.append([inst, side_lev, fnum(avg, 6), fnum(pnl, 6)])

    # widths
    cols = list(zip(*([headers] + data_rows))) if data_rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if data_rows else [len(h) for h in headers]
    def fmt_row(row): return "  ".join(str(v).ljust(w) for v, w in zip(row, widths))

    lines = [f"**{TABLE_TITLE}**", "```", fmt_row(headers), fmt_row(["-"*w for w in widths])]
    if data_rows:
        for r in data_rows:
            lines.append(fmt_row(r))
    else:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

# ---------- Main ----------
async def run():
    asyncio.create_task(discord_sender())

    global LAST_OPEN_KEYS, LAST_POST_TIME
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

                # updates
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
                    keys_now = current_open_keys(rows)

                    # Only post on open/close (set change), with a small rate cap
                    if (not first_sent) or (keys_now != LAST_OPEN_KEYS):
                        now = time.time()
                        if not first_sent or (now - LAST_POST_TIME) >= SEND_MIN_INTERVAL:
                            LAST_OPEN_KEYS = keys_now
                            table = format_table(rows)
                            await send_text(table)
                            LAST_POST_TIME = now
                            first_sent = True

        except Exception:
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
