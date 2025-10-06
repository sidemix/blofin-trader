# main.py
# BloFin → Discord (Render Worker)
# Posts ONE "Active BloFin Positions" table only when structure changes:
# - position opened/closed
# - size changes (by instId + positionSide)
# Ignores mark/uPnL/liq/entry noise. Max one post every SEND_MIN_INTERVAL seconds.

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

# Only trigger on SIZE changes > EPS (prevents float jitter)
SIZE_EPS = float(os.getenv("SIZE_EPS", "0.00000001"))  # 1e-8
# Don’t post more often than this (seconds)
SEND_MIN_INTERVAL = float(os.getenv("SEND_MIN_INTERVAL", "5"))

WS_URL = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

# ---------- Discord sender (queue + 429) ----------
SEND_Q: asyncio.Queue = asyncio.Queue()

async def discord_sender():
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            content = await SEND_Q.get()
            payload = {"content": content}
            try:
                r = await client.post(DISCORD_WEBHOOK, json=payload)
                if r.status_code == 429:
                    try:
                        retry_after = float(r.json().get("retry_after", 2.0))
                    except Exception:
                        retry_after = 2.0
                    await asyncio.sleep(retry_after)
                    await SEND_Q.put(content)
                else:
                    r.raise_for_status()
                    await asyncio.sleep(1.1)  # gentle pacing
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

# ---------- Snapshot & table ----------
PosKey = Tuple[str, str]  # (instId, posSide)
# We track ONLY size (float) per key; structure changes = key add/remove or size delta > EPS
LAST_SIZES: Dict[PosKey, float] = {}
LAST_POST_TIME: float = 0.0

def fnum(x, d=6):
    try: return f"{float(x):.{d}f}"
    except: return str(x or "")

def parse_size(r: dict) -> float:
    s = r.get("positions") or r.get("pos") or r.get("size") or 0
    try: return float(s)
    except: return 0.0

def build_current_sizes(rows: List[dict]) -> Dict[PosKey, float]:
    m: Dict[PosKey, float] = {}
    for r in rows:
        inst = str(r.get("instId"))
        side = str(r.get("positionSide", "net"))
        sz = parse_size(r)
        if abs(sz) <= SIZE_EPS:
            continue  # treat ~0 as closed
        m[(inst, side)] = sz
    return m

def needs_update(current: Dict[PosKey, float]) -> bool:
    # Any added/removed keys?
    if set(current.keys()) != set(LAST_SIZES.keys()):
        return True
    # Any size changed beyond EPS?
    for k, v in current.items():
        if k not in LAST_SIZES:  # handled by keys check, but keep safe
            return True
        if abs((LAST_SIZES[k] or 0.0) - (v or 0.0)) > SIZE_EPS:
            return True
    return False

def format_table(current: Dict[PosKey, float], sample_rows: List[dict]) -> str:
    # Map latest row per key for display-only MARK/PNL columns
    latest: Dict[PosKey, dict] = {}
    for r in sample_rows:
        key = (str(r.get("instId")), str(r.get("positionSide","net")))
        latest[key] = r

    headers = ["SYMBOL", "SIDE", "SIZE", "ENTRY", "MARK", "PNL", "PNL%"]
    data_rows: List[List[str]] = []

    for (inst, side) in sorted(current.keys()):
        row = latest.get((inst, side), {})
        size = fnum(current[(inst, side)], 6)
        entry = row.get("entryPrice") or row.get("avgPrice") or row.get("averagePrice") or ""
        mark  = row.get("markPrice")
        pnl   = row.get("unrealizedPnl")
        pnlr  = row.get("unrealizedPnlRatio")
        pnlp  = ""
        try: pnlp = f"{float(pnlr)*100:.2f}%"
        except: pnlp = str(pnlr or "")
        data_rows.append([inst, side, size, fnum(entry, 6), fnum(mark, 6), fnum(pnl, 6), pnlp])

    # Column widths
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

# ---------- Main loop ----------
async def run():
    asyncio.create_task(discord_sender())

    global LAST_SIZES, LAST_POST_TIME

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
                    if msg.get("event") == "error" or not is_push(msg):
                        continue

                    if msg.get("arg", {}).get("channel") != "positions":
                        continue

                    rows = msg.get("data", [])
                    current = build_current_sizes(rows)

                    # Only post when structural change AND rate limit interval passed
                    if (not first_sent) or needs_update(current):
                        now = time.time()
                        if not first_sent or (now - LAST_POST_TIME) >= SEND_MIN_INTERVAL:
                            LAST_SIZES = current
                            table = format_table(current, rows)
                            await send_text(table)
                            LAST_POST_TIME = now
                            first_sent = True

        except Exception:
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
