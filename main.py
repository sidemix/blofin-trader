import os, json, time, hmac, base64, asyncio, hashlib, uuid
import websockets
import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["BLOFIN_API_KEY"]
API_SECRET = os.environ["BLOFIN_API_SECRET"]
PASSPHRASE = os.environ["BLOFIN_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]
IS_DEMO = os.getenv("BLOFIN_IS_DEMO", "false").lower() == "true"

SUB_POS = os.getenv("SUBSCRIBE_POSITIONS", "true").lower() == "true"
SUB_ACCT = os.getenv("SUBSCRIBE_ACCOUNT", "true").lower() == "true"

WS_URL = (
    "wss://demo-trading-openapi.blofin.com/ws/private"
    if IS_DEMO else
    "wss://openapi.blofin.com/ws/private"
)

# ---------- Helpers ----------
def ws_login_payload():
    """
    BloFin WS auth: sign "/users/self/verify" + "GET" + timestamp + nonce
    with HMAC-SHA256(secret) -> hex -> base64
    """
    ts = str(int(time.time() * 1000))
    nonce = ts  # can be UUID, using ts for simplicity
    msg = "/users/self/verify" + "GET" + ts + nonce
    hex_sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    sign_b64 = base64.b64encode(hex_sig).decode()
    return {
        "op": "login",
        "args": [{
            "apiKey": API_KEY,
            "passphrase": PASSPHRASE,
            "timestamp": ts,
            "nonce": nonce,
            "sign": sign_b64
        }]
    }

def sub_payloads():
    subs = []
    # Subscribe to all orders for all instruments
    subs.append({"op": "subscribe", "args": [{"channel": "orders"}]})
    if SUB_POS:
        subs.append({"op": "subscribe", "args": [{"channel": "positions"}]})
    if SUB_ACCT:
        subs.append({"op": "subscribe", "args": [{"channel": "account"}]})
    return subs

async def send_discord(message: str, embed=None):
    async with httpx.AsyncClient(timeout=10) as client:
        payload = {"content": message}
        if embed:
            payload["embeds"] = [embed]
        r = await client.post(DISCORD_WEBHOOK, json=payload)
        r.raise_for_status()

def fmt_order_update(row: dict) -> tuple[str, dict]:
    # Keys per BloFin WS Orders push
    inst = row.get("instId")
    side = row.get("side")
    otype = row.get("orderType")
    state = row.get("state")            # live, filled, canceled, etc.
    sz = row.get("size")
    filled = row.get("filledSize")
    avg = row.get("averagePrice")
    px = row.get("price")
    pnl = row.get("pnl")
    fee = row.get("fee")
    lev = row.get("leverage")
    tp = row.get("tpTriggerPrice")
    sl = row.get("slTriggerPrice")
    oid = row.get("orderId")

    title = f"BloFin {inst} — {side.upper()} {otype} — {state.upper()}"
    fields = []
    def add(name, value):
        if value not in (None, "", "0", "0.000000000000000000"):
            fields.append({"name": name, "value": str(value), "inline": True})

    add("Order ID", oid)
    add("Price", px)
    add("Size", sz)
    add("Filled", filled)
    add("Avg Fill", avg)
    add("Leverage", lev)
    add("TP", tp)
    add("SL", sl)
    add("PnL", pnl)
    add("Fee", fee)

    embed = {
        "title": title,
        "description": f"**PositionSide**: {row.get('positionSide','net')}  •  **MarginMode**: {row.get('marginMode','')}",
        "fields": fields,
        "footer": {"text": f"createTime: {row.get('createTime')}  updateTime: {row.get('updateTime')}"}
    }
    return f"", embed

def fmt_position_update(row: dict) -> tuple[str, dict]:
    inst = row.get("instId")
    pos = row.get("positions")
    upnl = row.get("unrealizedPnl")
    upnlr = row.get("unrealizedPnlRatio")
    mprice = row.get("markPrice")
    liq = row.get("liquidationPrice")
    lev = row.get("leverage")
    title = f"BloFin Position — {inst}"
    embed = {
        "title": title,
        "fields": [
            {"name":"Positions","value": str(pos), "inline": True},
            {"name":"uPnL","value": str(upnl), "inline": True},
            {"name":"uPnL Ratio","value": str(upnlr), "inline": True},
            {"name":"Mark","value": str(mprice), "inline": True},
            {"name":"Liq","value": str(liq), "inline": True},
            {"name":"Lev","value": str(lev), "inline": True},
        ],
        "footer": {"text": f"updateTime: {row.get('updateTime')}"}
    }
    return "", embed

def is_push(msg: dict) -> bool:
    return "data" in msg and ("arg" in msg or "action" in msg)

# ---------- WS Client ----------
async def run():
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=5_000_000) as ws:
                # LOGIN
                await ws.send(json.dumps(ws_login_payload()))
                login_ack = json.loads(await ws.recv())
                if login_ack.get("event") == "error":
                    raise RuntimeError(f"Login failed: {login_ack}")
                # SUBSCRIBE
                for sub in sub_payloads():
                    await ws.send(json.dumps(sub))
                await send_discord("✅ Connected to **BloFin WS** and subscribed (orders/positions/account).")

                # MESSAGE LOOP
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    # acks/errors
                    if msg.get("event") in ("subscribe","unsubscribe"):
                        # you could also send to Discord if you want
                        continue
                    if msg.get("event") == "error":
                        await send_discord(f"⚠️ BloFin WS error: `{msg}`")
                        continue

                    if not is_push(msg):  # heartbeats or others
                        continue

                    arg = msg.get("arg", {})
                    channel = arg.get("channel", "")
                    rows = msg.get("data", [])

                    if channel == "orders":
                        for row in rows:
                            content, embed = fmt_order_update(row)
                            await send_discord(content or "**Order Update**", embed)
                    elif channel == "positions":
                        for row in rows:
                            content, embed = fmt_position_update(row)
                            await send_discord(content or "**Position Update**", embed)
                    elif channel in ("account","inverse-account"):
                        # keep it light: only notify on balance changes
                        await send_discord("💼 **Account Update**", {"title":"BloFin Account","description":f"payload: ```json\n{json.dumps(rows)[:1800]}\n```"})

        except Exception as e:
            try:
                await send_discord(f"🔁 Reconnecting to BloFin WS in {backoff}s…\n`{repr(e)}`")
            except Exception:
                pass
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(run())
