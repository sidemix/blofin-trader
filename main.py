# main.py
# BloFin → Discord (Render Worker)
# - Sends once per trade, then only on changes (order state or position size/side changes)
# - Private WS auth + subscriptions
# - Discord webhook sender with basic 429 handling

import os, json, time, hmac, base64, hashlib, asyncio
from typing import Optional, Dict, Tuple

import websockets
import httpx

# ===== .env (optional) =====
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ===== Required env =====
API_KEY = os.environ["BLOFIN_API_KEY"]
API_SECRET = os.environ["BLOFIN_API_SECRET"]
PASSPHRASE = os.environ["BLOFIN_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]

# ===== Optional env =====
IS_DEMO = os.getenv("BLOFIN_IS_DEMO", "false").lower() == "true"
SUB_POS = os.getenv("SUBSCRIBE_POSITIONS", "true").lower() == "true"
SUB_ACCT = os.getenv("SUBSCRIBE_ACCOUNT", "false").lower() == "true"  # default off to avoid noise

WS_URL = "wss://demo-trading-openapi.blofin.com/ws/private" if IS_DEMO else "wss://openapi.blofin.com/ws/private"

# ===== Discord sender (single queue, gentle rate) =====
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

async def send_discord(content: str = "", embed: Optional[dict] = None):
    await SEND_Q.put((content, embed))

# ===== BloFin auth & subs =====
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

def sub_payloads() -> list[dict]:
    subs = [{"op": "subscribe", "args": [{"channel": "orders"}]}]
    if SUB_POS:
        subs.append({"op": "subscribe", "args": [{"channel": "positions"}]})
    if SUB_ACCT:
        subs.append({"op": "subscribe", "args": [{"channel": "account"}]})
    return subs

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

# ===== Change tracking =====
# Keep compact signatures; only notify on change.
ORDER_SIG: Dict[str, str] = {}  # orderId -> sig
POS_SIG: Dict[Tuple[str, str], str] = {}  # (instId, positionSide) -> sig

def order_signature(row: dict) -> str:
    # include fields that indicate “meaningful” trade change
    parts = [
        str(row.get("state")),                # live/filled/canceled/partially_filled, etc.
        str(row.get("filledSize")),
        str(row.get("averagePrice")),
        str(row.get("size")),
        str(row.get("price")),
        str(row.get("pnl")),
        str(row.get("fee")),
        str(row.get("tpTriggerPrice")),
        str(row.get("slTriggerPrice")),
    ]
    return "|".join(parts)

def position_signature(row: dict) -> Tuple[Tuple[str, str], str]:
    inst = str(row.get("instId"))
    side = str(row.get("positionSide", "net"))
    # focus on structural changes (size/side/lev/liq); mark & uPnL fluctuate too much
    sig_parts = [
        str(row.get("positions") or row.get("pos") or "0"),
        str(row.get("positionSide", "net")),
        str(row.get("leverage") or ""),
        str(row.get("liquidationPrice") or ""),
        str(row.get("entryPrice") or row.get("avgPrice") or ""),  # if provided
    ]
    return (inst, side), "|".join(sig_parts)

# ===== Formatting =====
def embed_order(row: dict) -> dict:
    inst = row.get("instId")
    side = (row.get("side") or "").upper()
    otype = row.get("orderType")
    state = (row.get("state") or "").upper()
    title = f"{inst} — {side} {otype} — {state}"

    fields = []
    def add(n, v, inline=True):
        if v not in (None, "", "0", "0.000000000000000000"): fields.append({"name": n, "value": str(v), "inline": inline})

    add("Order ID", row.get("orderId"))
    add("Size", row.get("size"))
    add("Filled", row.get("filledSize"))
    add("Avg Fill", row.get("averagePrice"))
    add("Limit Px", row.get("price"))
    add("Leverage", row.get("leverage"))
    add("TP", row.get("tpTriggerPrice"))
    add("SL", row.get("slTriggerPrice"))
    add("PnL", row.get("pnl"))
    add("Fee", row.get("fee"))

    return {
        "title": f"Order Update — {title}",
        "description": f"**Margin**: {row.get('marginMode','')}  •  **PosSide**: {row.get('positionSide','net')}",
        "fields": fields,
        "footer": {"text": f"create: {row.get('createTime')}  update: {row.get('updateTime')}"}
    }

def embed_position(row: dict) -> dict:
    inst = row.get("instId")
    size = row.get("positions") or row.get("pos")
    return {
        "title": f"Position Update — {inst}",
        "fields": [
            {"name":"PosSide","value": str(row.get("positionSide","net")), "inline": True},
            {"name":"Size","value": str(size), "inline": True},
            {"name":"Lev","value": str(row.get("leverage")), "inline": True},
            {"name":"Entry/Avg","value": str(row.get("entryPrice") or row.get("avgPrice") or ""), "inline": True},
            {"name":"Liq","value": str(row.get("liquidationPrice") or ""), "inline": True},
            {"name":"Mark","value": str(row.get("markPrice") or ""), "inline": True},
            {"name":"uPnL","value": str(row.get("unrealizedPnl") or ""), "inline": True},
            {"name":"uPnL Ratio","value": str(row.get("unrealizedPnlRatio") or ""), "inline": True},
        ],
        "footer": {"text": f"update: {row.get('updateTime')}"}
    }

# ===== Main WS loop =====
async def run():
    # start webhook sender
    asyncio.create_task(discord_sender())

    backoff = 1
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
                await send_discord("✅ Connected to **BloFin WS** (orders/positions).")

                # read loop
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("event") in ("subscribe","unsubscribe"):
                        continue
                    if msg.get("event") == "error":
                        # quietly log to Discord once
                        await send_discord(f"⚠️ BloFin WS error: `{msg}`")
                        continue
                    if not is_push(msg):
                        continue

                    channel = msg.get("arg", {}).get("channel", "")
                    rows = msg.get("data", [])

                    if channel == "orders":
                        for row in rows:
                            oid = str(row.get("orderId"))
                            sig = order_signature(row)
                            if ORDER_SIG.get(oid) != sig:
                                ORDER_SIG[oid] = sig
                                await send_discord("", embed_order(row))

                    elif channel == "positions":
                        for row in rows:
                            key, sig = position_signature(row)
                            if POS_SIG.get(key) != sig:
                                POS_SIG[key] = sig
                                await send_discord("", embed_position(row))

                    elif channel in ("account","inverse-account"):
                        # optional: only on big equity change — disabled by default
                        pass

        except Exception as e:
            # quiet reconnect; no spam
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
