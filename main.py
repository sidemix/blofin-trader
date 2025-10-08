# main.py
# Blofin → Discord (and Notion) notifier
# - Posts ONLY on new/closed positions (plus optional 6h snapshot)
# - Robust symbol normalization (BNBUSDT, BNB-USDT, BNB/USDT, etc.)
# - Notion: creates/updates a row in your "Trade Journal" on open/changes/close
# - WS first with gentle REST polling fallback

import asyncio
import contextlib
import dataclasses
import datetime as dt
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import websockets

# ────────────────────────────────────────────────────────────────────────────────
# ENV
# ────────────────────────────────────────────────────────────────────────────────
BLOFIN_API_KEY = os.getenv("BLOFIN_API_KEY", "")
BLOFIN_API_SECRET = os.getenv("BLOFIN_API_SECRET", "")
BLOFIN_PASSPHRASE = os.getenv("BLOFIN_PASSPHRASE", "")
BLOFIN_IS_DEMO = os.getenv("BLOFIN_IS_DEMO", "false").lower() in ("1", "true", "yes")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# Notion
NOTION_TOKEN = os.getenv("NOTION_API_TOKEN", "")  # can be an "ntn_*" internal token
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")

# How often to emit the “Active Positions” snapshot (hours). Set 0 to disable.
PERIOD_HOURS = float(os.getenv("PERIOD_HOURS", "6"))

# minimum interval between sending two Discord messages (in seconds)
SEND_MIN_INTERVAL = int(os.getenv("SEND_MIN_INTERVAL", "5"))

# poll fallback (seconds) — if WS is quiet we still poll and diff
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))

STATE_FILE = "/mnt/data/blofin_state.json"

# ────────────────────────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────────────────────────
def now_ts() -> int:
    return int(time.time() * 1000)

def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def normalize_symbol(sym: str) -> str:
    """
    Normalize into NAME-QUOTE (e.g. BNB-USDT).
    Accepts: 'BNBUSDT', 'BNB-USDT', 'BNB/USDT', 'bnb_usdt', etc.
    """
    s = sym.strip().upper().replace(" ", "")
    s = s.replace("/", "-").replace("_", "-")
    # If it already has a dash, keep it
    if "-" in s:
        return s
    # “XXXUSDT” → “XXX-USDT” (works for 3+ char bases)
    if s.endswith("USDT") and len(s) > 4:
        return f"{s[:-4]}-USDT"
    if s.endswith("USD") and len(s) > 3:
        return f"{s[:-3]}-USD"
    return s

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default

def signed_okx_like(message: str, secret: str) -> str:
    """OKX-style HMAC SHA256 then base64; Blofin follows a near-identical pattern."""
    mac = hmac.new(secret.encode(), msg=message.encode(), digestmod=hashlib.sha256)
    return mac.digest().hex()  # hex works for Blofin WS sig in practice

def fmt_price(p: float) -> str:
    # sensible display (no trailing zeros spam)
    if abs(p) >= 100:
        return f"{p:,.0f}"
    if abs(p) >= 10:
        return f"{p:,.2f}"
    if abs(p) >= 1:
        return f"{p:,.4f}"
    return f"{p:,.6f}"

def fmt_pnl(v: float) -> str:
    return f"{v:+.5f}".rstrip("0").rstrip(".") if v else "0"

def fmt_pct(v: float) -> str:
    sign = "+" if v > 0 else "-" if v < 0 else ""
    return f"{sign}{abs(v):.2f}%"

def load_state() -> Dict[str, Any]:
    with contextlib.suppress(Exception):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"positions": {}, "notion": {}, "last_snapshot_at": 0, "last_send_at": 0}

def save_state(st: Dict[str, Any]) -> None:
    with contextlib.suppress(Exception):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(st, f)

# ────────────────────────────────────────────────────────────────────────────────
# DISCORD
# ────────────────────────────────────────────────────────────────────────────────
async def send_discord(text: str, client: httpx.AsyncClient, st: Dict[str, Any]) -> None:
    if not DISCORD_WEBHOOK_URL:
        return
    # simple rate-limit
    if time.time() - st.get("last_send_at", 0) < SEND_MIN_INTERVAL:
        return
    try:
        r = await client.post(DISCORD_WEBHOOK_URL, json={"content": text}, timeout=30)
        r.raise_for_status()
        st["last_send_at"] = time.time()
        save_state(st)
    except httpx.HTTPStatusError as e:
        # 429 etc. — keep running, don’t crash
        print(f"Discord error: {e.response.status_code} {e.response.text[:200]}")
    except Exception as e:
        print(f"Discord error: {e!r}")

def format_new_trade_line(p: Dict[str, Any]) -> str:
    # p keys we use: symbol, side("Long"/"Short"), lev, entry
    s = normalize_symbol(p["symbol"])
    side = p.get("side", "Long")
    lev = int(safe_float(p.get("lev", 1)))
    entry = safe_float(p.get("entry", 0.0))
    return f"🟢 New trade: **{s}** {side} {lev}x @ avg {fmt_price(entry)}"

def format_closed_trade_line(symbol: str, pnl: float, pnl_pct: float) -> str:
    s = normalize_symbol(symbol)
    return f"🔴 Closed trade: **{s}** • PNL {fmt_pnl(pnl)} • {fmt_pct(pnl_pct)}"

def format_positions_table(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "Active positions\n```\n(no open positions)\n```"
    header = "Active positions\n```\nSYMBOL     SIDE xLEV   AVG Entry Price     PNL         PNL%\n" \
             "---------  ---------  ---------------  ----------  --------\n"
    body_lines = []
    for r in rows:
        s = normalize_symbol(r["symbol"])
        side = r.get("side", "Long")
        lev = int(safe_float(r.get("lev", 1)))
        entry = fmt_price(safe_float(r.get("entry", 0.0)))
        pnl = fmt_pnl(safe_float(r.get("pnl", 0.0)))
        pnlp = fmt_pct(safe_float(r.get("pnlPct", 0.0)))
        body_lines.append(f"{s:<10} {side:<4} {lev:>2}x  {entry:>15}  {pnl:>10}  {pnlp:>7}")
    return header + "\n".join(body_lines) + "\n```"

# ────────────────────────────────────────────────────────────────────────────────
# NOTION
# ────────────────────────────────────────────────────────────────────────────────
NOTION_BASE = "https://api.notion.com/v1"
N_VERSION = "2022-06-28"  # stable and works with your DB
N_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": N_VERSION,
    "Content-Type": "application/json",
}

async def notion_get_props(client: httpx.AsyncClient) -> Dict[str, str]:
    if not (NOTION_TOKEN and NOTION_DATABASE_ID):
        return {}
    try:
        r = await client.get(f"{NOTION_BASE}/databases/{NOTION_DATABASE_ID}", headers=N_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        props = data.get("properties", {})
        # map readable names to types
        return {k: v.get("type") for k, v in props.items()}
    except Exception as e:
        print(f"Notion DB schema error: {e}")
        return {}

def n_title(v: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": v[:200]}}]}

def n_select(opt: str) -> Dict[str, Any]:
    return {"select": {"name": opt}}

def n_number(x: float) -> Dict[str, Any]:
    return {"number": x}

async def notion_find_open_page_id(client: httpx.AsyncClient, trade_name: str) -> Optional[str]:
    if not (NOTION_TOKEN and NOTION_DATABASE_ID):
        return None
    try:
        payload = {
            "database_id": NOTION_DATABASE_ID,
            "page_size": 25,
            "filter": {
                "and": [
                    {"property": "Trade Name", "title": {"equals": trade_name}},
                    {"property": "Status", "select": {"equals": "1.Open"}},
                ]
            },
        }
        r = await client.post(f"{NOTION_BASE}/databases/{NOTION_DATABASE_ID}/query", headers=N_HEADERS, json=payload, timeout=30)
        r.raise_for_status()
        res = r.json()
        results = res.get("results", [])
        if results:
            return results[0]["id"]
    except Exception as e:
        print(f"Notion query error: {e}")
    return None

async def notion_create_or_update(
    client: httpx.AsyncClient,
    props_supported: Dict[str, str],
    trade: Dict[str, Any],
    st: Dict[str, Any],
) -> None:
    """Create (on open) then update as values change."""
    if not (NOTION_TOKEN and NOTION_DATABASE_ID):
        return

    trade_name = normalize_symbol(trade["symbol"])
    # build property payload using only props that exist
    props: Dict[str, Any] = {}

    def maybe_set(name: str, val: Any, builder):
        if name in props_supported:
            props[name] = builder(val)

    maybe_set("Trade Name", trade_name, n_title)
    maybe_set("Status", "1.Open" if trade.get("isOpen") else "3.Closed", n_select)
    maybe_set("Direction", trade.get("side", "Long"), n_select)
    maybe_set("Leverage", safe_float(trade.get("lev", 1)), n_number)
    maybe_set("Qty", safe_float(trade.get("qty", 0)), n_number)
    maybe_set("Entry Price", safe_float(trade.get("entry", 0)), n_number)
    maybe_set("Exit Price", safe_float(trade.get("exit", 0)), n_number)
    maybe_set("Market Price", safe_float(trade.get("mark", 0)), n_number)
    maybe_set("Closed P&L", safe_float(trade.get("pnl", 0)), n_number)
    maybe_set("P&L %", safe_float(trade.get("pnlPct", 0)), n_number)

    page_id = st.get("notion", {}).get(trade_name)
    if not page_id and trade.get("isOpen"):
        page_id = await notion_find_open_page_id(client, trade_name)

    try:
        if not page_id and trade.get("isOpen"):
            # Create
            payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props}
            r = await client.post(f"{NOTION_BASE}/pages", headers=N_HEADERS, json=payload, timeout=30)
            r.raise_for_status()
            pid = r.json()["id"]
            st.setdefault("notion", {})[trade_name] = pid
            save_state(st)
        elif page_id:
            # Update
            payload = {"properties": props}
            r = await client.patch(f"{NOTION_BASE}/pages/{page_id}", headers=N_HEADERS, json=payload, timeout=30)
            r.raise_for_status()
    except httpx.HTTPStatusError as e:
        print(f"Notion create/update error: {e.response.status_code} {e.response.text[:200]}")
    except Exception as e:
        print(f"Notion create/update error: {e!r}")

# ────────────────────────────────────────────────────────────────────────────────
# BLOFIN (WS + tiny REST fallback)
# ────────────────────────────────────────────────────────────────────────────────
if BLOFIN_IS_DEMO:
    BLOFIN_HTTP = "https://openapi-sandbox.blofin.com"
    BLOFIN_WS = "wss://openapi-sandbox.blofin.com/ws/private"
else:
    BLOFIN_HTTP = "https://openapi.blofin.com"
    BLOFIN_WS = "wss://openapi.blofin.com/ws/private"

def blofin_sign(ts: str) -> str:
    # Blofin auth mirrors OKX (timestamp + "GET" + "/users/self/verify") on WS login
    prehash = f"{ts}GET/users/self/verify"
    return signed_okx_like(prehash, BLOFIN_API_SECRET)

async def ws_login(ws) -> None:
    ts = str(time.time())
    msg = {
        "op": "login",
        "args": [{
            "apiKey": BLOFIN_API_KEY,
            "passphrase": BLOFIN_PASSPHRASE,
            "timestamp": ts,
            "sign": blofin_sign(ts),
        }]
    }
    await ws.send(json.dumps(msg))

async def ws_subscribe(ws) -> None:
    subs = [
        {"channel": "orders"},
        {"channel": "positions"},
    ]
    await ws.send(json.dumps({"op": "subscribe", "args": subs}))
    print("✅ Connected & subscribed (orders/positions)")

async def fetch_positions_http(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """
    Best-effort REST fallback. Endpoint name may evolve; we try a couple variants.
    Returned structure is normalized to a short dict per position.
    """
    cand_paths = [
        "/api/v5/account/positions",            # OKX-style (many OKX-like exchanges use this)
        "/api/v1/private/positions",           # generic
        "/api/v1/account/positions",
    ]
    for path in cand_paths:
        try:
            # timestamp-based auth (OKX-like)
            ts = str(time.time())
            sign = signed_okx_like(f"{ts}GET{path}", BLOFIN_API_SECRET)
            headers = {
                "OK-ACCESS-KEY": BLOFIN_API_KEY,
                "OK-ACCESS-PASSPHRASE": BLOFIN_PASSPHRASE,
                "OK-ACCESS-TIMESTAMP": ts,
                "OK-ACCESS-SIGN": sign,
            }
            r = await client.get(BLOFIN_HTTP + path, headers=headers, timeout=10)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            data = r.json()
            raw = data.get("data") or data.get("result") or data
            return [normalize_position_obj(x) for x in ensure_list(raw)]
        except Exception:
            continue
    return []

def ensure_list(x) -> List[Any]:
    if isinstance(x, list):
        return x
    if x is None:
        return []
    return [x]

def normalize_position_obj(x: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Blofin/OKX-ish payload into our internal position dict:
      symbol, side(Long/Short), lev, qty, entry, mark, pnl, pnlPct, isOpen
    """
    sym = normalize_symbol(x.get("instId") or x.get("symbol") or x.get("inst", ""))
    side = x.get("posSide") or x.get("side") or ("Long" if safe_float(x.get("pos", x.get("sz", 0))) >= 0 else "Short")
    side = "Long" if str(side).lower().startswith("long") or str(side).lower().startswith("buy") else "Short"
    lev = safe_float(x.get("lever") or x.get("leveraged") or x.get("lev", 0))
    qty = abs(safe_float(x.get("pos") or x.get("sz") or x.get("size", 0)))
    entry = safe_float(x.get("avgPx") or x.get("avgPrice") or x.get("entryPx") or x.get("entryPrice") or 0.0)
    mark = safe_float(x.get("markPx") or x.get("markPrice") or x.get("last", 0.0))
    pnl = safe_float(x.get("upl") or x.get("uPnL") or x.get("pnl") or 0.0)
    pnl_pct = safe_float(x.get("uplRatio") or x.get("uPnLRatio") or x.get("pnlPct") or 0.0) * (100.0 if abs(safe_float(x.get("uplRatio") or x.get("uPnLRatio"), 0)) < 1 else 1)

    return {
        "symbol": sym,
        "side": side,
        "lev": lev,
        "qty": qty,
        "entry": entry,
        "mark": mark,
        "pnl": pnl,
        "pnlPct": pnl_pct,
        "isOpen": qty > 0,
    }

# ────────────────────────────────────────────────────────────────────────────────
# CORE LOOP
# ────────────────────────────────────────────────────────────────────────────────
async def run():
    st = load_state()
    async with httpx.AsyncClient(timeout=30) as client:
        # Probe Notion schema just once (to know which properties we can set safely)
        notion_props = await notion_get_props(client)
        if notion_props:
            print("Notion DB schema detected.")

        # background poller (fallback)
        async def poller():
            while True:
                await asyncio.sleep(POLL_INTERVAL)
                with contextlib.suppress(Exception):
                    polled = await fetch_positions_http(client)
                    if polled:
                        await reconcile_positions(polled, client, st, notion_props)

        asyncio.create_task(poller())

        # WS loop (auto reconnect)
        while True:
            print(f"Connecting to BloFin WS… ({BLOFIN_WS})" if "openapi" in BLOFIN_WS else "Connecting to BloFin WS…")
            try:
                async with websockets.connect(BLOFIN_WS, ping_interval=20, ping_timeout=20) as ws:
                    await ws_login(ws)
                    await ws_subscribe(ws)

                    # basic heartbeat
                    last_seen = time.time()
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            # send ping frame (websockets lib does this automatically if ping_interval set)
                            continue

                        last_seen = time.time()
                        data = json.loads(raw)

                        # login/subscribe acks
                        if data.get("event") in ("login", "subscribe"):
                            continue

                        # OKX-ish channel frame: {"arg":{"channel":"positions"},"data":[...]}
                        arg = data.get("arg", {})
                        ch = arg.get("channel") or data.get("channel")
                        payload = data.get("data") or data.get("result") or []
                        if not ch or not payload:
                            continue

                        if ch == "positions":
                            normalized = [normalize_position_obj(x) for x in payload]
                            await reconcile_positions(normalized, client, st, notion_props)

                        elif ch == "orders":
                            # Some exchanges provide fills here. We still rely on positions diffing for truth.
                            pass

            except Exception as e:
                print(f"WS error: {e!r}")
                print("🔁 Reconnecting to BloFin WS in 2s…", e)
                await asyncio.sleep(2)

# ────────────────────────────────────────────────────────────────────────────────
# RECONCILIATION (diff → actions)
# ────────────────────────────────────────────────────────────────────────────────
async def reconcile_positions(
    positions: List[Dict[str, Any]],
    client: httpx.AsyncClient,
    st: Dict[str, Any],
    notion_props: Dict[str, str],
) -> None:
    """
    positions: list of normalized dicts (see normalize_position_obj)
    We diff against st["positions"] and detect opens/closes & value changes.
    """
    # Build current map
    cur: Dict[str, Dict[str, Any]] = {}
    for p in positions:
        sym = normalize_symbol(p["symbol"])
        # net modes sometimes deliver duplicates (net pos only). Keep one.
        # We merge so the latest entry/mark/pnl stays fresh.
        if sym in cur:
            # choose the one with bigger qty or fresher mark
            if safe_float(p.get("qty", 0)) >= safe_float(cur[sym].get("qty", 0)):
                cur[sym].update(p)
        else:
            cur[sym] = p

    prev: Dict[str, Any] = st.get("positions", {})

    # OPEN: in cur and qty>0 but not in prev (or prev qty == 0)
    for sym, p in cur.items():
        qty = safe_float(p.get("qty", 0))
        prev_qty = safe_float(prev.get(sym, {}).get("qty", 0))
        if qty > 0 and prev_qty == 0:
            # new open
            line = format_new_trade_line(p)
            await send_discord(line, client, st)
            # Notion create/update
            await notion_create_or_update(client, notion_props, p, st)

    # CLOSE: in prev with qty>0 but not in cur (or qty==0)
    for sym, pprev in prev.items():
        prev_qty = safe_float(pprev.get("qty", 0))
        cur_qty = safe_float(cur.get(sym, {}).get("qty", 0))
        if prev_qty > 0 and cur_qty == 0:
            pnl = safe_float(pprev.get("pnl", 0))
            pnlp = safe_float(pprev.get("pnlPct", 0))
            await send_discord(format_closed_trade_line(sym, pnl, pnlp), client, st)
            # close in Notion
            closing = dict(pprev)
            closing["isOpen"] = False
            await notion_create_or_update(client, notion_props, closing, st)

    # UPDATE: for open positions, keep Notion row fresh (avg/mark/pnl)
    for sym, p in cur.items():
        if safe_float(p.get("qty", 0)) <= 0:
            continue
        # we only update Notion if something meaningful changed vs prev
        prev_p = prev.get(sym, {})
        changed = False
        for k in ("entry", "mark", "pnl", "pnlPct"):
            if abs(safe_float(p.get(k, 0)) - safe_float(prev_p.get(k, 0))) > 1e-10:
                changed = True
                break
        if changed:
            await notion_create_or_update(client, notion_props, p, st)

    # Persist current
    st["positions"] = cur
    save_state(st)

    # 6h snapshot to Discord (optional)
    if PERIOD_HOURS > 0:
        due = st.get("last_snapshot_at", 0) + PERIOD_HOURS * 3600
        if time.time() >= due:
            rows = []
            for sym, p in cur.items():
                rows.append({
                    "symbol": sym,
                    "side": p.get("side", "Long"),
                    "lev": p.get("lev", 1),
                    "entry": p.get("entry", 0.0),
                    "pnl": p.get("pnl", 0.0),
                    "pnlPct": p.get("pnlPct", 0.0),
                })
            msg = format_positions_table(rows)
            await send_discord(msg, client, st)
            st["last_snapshot_at"] = time.time()
            save_state(st)

# ────────────────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
