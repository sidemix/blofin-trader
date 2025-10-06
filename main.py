# main.py
# BloFin → Discord (Render Worker)
# Posts ONE consolidated "Active Positions" table,
# and only updates it when positions structurally change
# (opened/closed, size change, add/reduce, side/lev/liq change).

import os, json, time, hmac, base64, hashlib, asyncio
from typing import Optional, Dict, Tuple, List

import websockets
import httpx

# -------- Env --------
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
SUBSCRIBE_ORDERS = os.getenv("SUBSCRIBE_ORDERS", "false").lower() == "true"  # optional
TABLE_TITLE = os.getenv("TABLE_TITLE", "Active BloFin Positions")

WS_URL = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

# -------- Discord sender (queue + 429 safety) --------
SEND_Q: asyncio.Queue = asyncio.Queue()

async def discord_sender():
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            content, embed = await SEND_Q.get()
            payload = {"content": content} if content else {}
            if embed:
                payload["embeds"] = [embed]
            try:
                r = await client.post(DISCORD_WEBHOOK, json=payload)
                if r.status_code == 429:
                    # honor retry_after then requeue
                    try:
                        retry_after = float(r.json().get("retry_after", 2.0))
                    except Exception:
                        retry_after = 2.0
                    await asyncio.sleep(retry_after)
                    await SEND_Q.put((content, embed))
                else:
                    r.raise_for_status()
                    # gentle pace ~1 msg/sec
                    await asyncio.sleep(1.1)
            except Exception:
                await asyncio.sleep(2.0)
            finally:
                SEND_Q.task_done()

async def send_discord_text(text: str):
    await SEND_Q.put((text, None))

# -------- BloFin auth / subs --------
def ws_login_payload() -> dict:
    """
    sign = Base64( HEX( HMAC_SHA256(secret, "/users/self/verify" + "GET" + timestamp + nonce) ) )
    """
    ts = str(int(time.time() * 1000))
    nonce = ts
    msg = "/users/self/verify" + "GET" + ts + nonce
    hex_sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    sign_b64 = base64.b64encode(hex_sig).decode()
    return {"op": "login", "args": [{"apiKey": API_KEY, "passphrase": PASSPHRASE, "timestamp": ts, "nonce": nonce, "sign": sign_b64}]}

def sub_payloads() -> List[dict]:
    subs = [{"op": "subscribe", "args": [{"channel": "positions"}]}]
    if SUBSCRIBE_ORDERS:
        subs.append({"op": "subscribe", "args": [{"channel": "orders"}]})
    return subs

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

# -------- Snapshot + table logic --------
# We store only structural fields that indicate a position changed in a meaningful way.
# Key: (instId, positionSide)
# Val signature fields: size, entry/avg, leverage, liquidationPrice
PosKey = Tuple[str, str]  # (instId, posSide)
POS_SNAPSHOT: Dict[PosKey, Tuple[str, str, str, str]] = {}  # -> (size, entry, lev, liq)
LAST_SET_SIG: str = ""  # overall signature string of the table

def norm_str(x, decimals=6):
    if x is None or x == "":
        return ""
    try:
        return f"{float(x):.{decimals}f}"
    except Exception:
        return str(x)

def build_structural_map(rows: List[dict]) -> Dict[PosKey, Tuple[str, str, str, str]]:
    m: Dict[PosKey, Tuple[str, str, str, str]] = {}
    for r in rows:
        inst = str(r.get("instId"))
        side = str(r.get("positionSide", "net"))
        # size field names vary; use what's present
        size = r.get("positions") or r.get("pos") or r.get("size") or "0"
        # treat zero/near-zero as closed
        try:
            if float(size) == 0:
                continue
        except Exception:
            if str(size).strip() in ("0", "0.0", ""):
                continue

        entry = r.get("entryPrice") or r.get("avgPrice") or r.get("averagePrice") or ""
        lev   = r.get("leverage") or ""
        liq   = r.get("liquidationPrice") or ""

        key: PosKey = (inst, side)
        m[key] = (norm_str(size, 6), norm_str(entry, 6), norm_str(lev, 3), norm_str(liq, 6))
    return m

def snapshot_signature(m: Dict[PosKey, Tuple[str, str, str, str]]) -> str:
    # A canonical string (sorted) based on structural fields only.
    items = []
    for (inst, side), (size, entry, lev, liq) in sorted(m.items()):
        items.append(f"{inst}|{side}|{size}|{entry}|{lev}|{liq}")
    return "\n".join(items)

def format_table(m: Dict[PosKey, Tuple[str, str, str, str]], sample_rows: List[dict]) -> str:
    # For display we’ll also show Mark, PnL, PnL% from the latest payload if present,
    # but those DO NOT affect when we send.
    latest_by_key: Dict[PosKey, dict] = {}
    for r in sample_rows:
        inst = str(r.get("instId"))
        side = str(r.get("positionSide", "net"))
        key = (inst, side)
        latest_by_key[key] = r

    headers = ["SYMBOL", "SIDE", "SIZE", "ENTRY", "MARK", "PNL", "PNL%"]
    rows = []
    for (inst, side), (size, entry, lev, liq) in sorted(m.items()):
        r = latest_by_key.get((inst, side), {})
        mark = norm_str(r.get("markPrice"), 6)
        pnl  = norm_str(r.get("unrealizedPnl"), 6)
        pnlr = r.get("unrealizedPnlRatio")
        pnlp = ""
        try:
            pnlp = f"{float(pnlr)*100:.2f}%"
        except Exception:
            pnlp = str(pnlr or "")
        rows.append([inst, side, size, entry, mark, pnl, pnlp])

    # widths
    cols = list(zip(*([headers] + rows))) if rows else [headers]
    widths = [max(len(str(x)) for x in col) for col in cols] if rows else [len(h) for h in headers]

    def fmt_row(row):
        return "  ".join(str(v).ljust(w) for v, w in zip(row, widths))

    lines = [f"**{TABLE_TITLE}**", "```", fmt_row(headers)]
    if rows:
        lines.append(fmt_row(["-"*w for w in widths]))
        for r in rows:
            lines.append(fmt_row(r))
    else:
        lines.append(fmt_row(["-"*w for w in widths]))
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

# -------- Main loop --------
async def run():
    asyncio.create_task(discord_sender())

    global POS_SNAPSHOT, LAST_SET_SIG
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

                # read loop
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("event") in ("subscribe","unsubscribe","info","pong"):
                        continue
                    if msg.get("event") == "error":
                        # log quietly
                        continue
                    if not is_push(msg):
                        continue

                    channel = msg.get("arg", {}).get("channel", "")
                    rows = msg.get("data", [])

                    if channel == "positions":
                        # Build structural snapshot of OPEN positions
                        m = build_structural_map(rows)
                        sig = snapshot_signature(m)

                        # Only send when structure actually changed
                        if sig != LAST_SET_SIG or not first_sent:
                            LAST_SET_SIG = sig
                            POS_SNAPSHOT = m
                            table = format_table(POS_SNAPSHOT, rows)
                            await send_discord_text(table)
                            first_sent = True

                    elif channel == "orders" and SUBSCRIBE_ORDERS:
                        # If you want: trigger an immediate refresh on order fills/cancels
                        # by forcing a resend next positions push:
                        LAST_SET_SIG = ""  # next positions push will publish an updated table
                        continue

        except Exception:
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
