"""Records NDAX's public live feed (best bid/ask and every trade) plus Kraken's best bid/ask, for research.

Read-only public market data: no account, no keys, no orders. One connection to each exchange.
NDAX subscriptions are paced slowly and reconnects back off, to stay well inside NDAX's limits.
Writes gzipped CSV chunks to ndax/ every 5 minutes and commits them.

Usage: python scripts/ndax_recorder.py --seconds 3300
"""
import argparse, asyncio, csv, datetime as dt, gzip, io, json, os, subprocess, time

import websockets

NDAX_WS = "wss://api.ndax.io/WSGateway"
KRAKEN_WS = "wss://ws.kraken.com/v2"
COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "HBAR", "SUI", "NEAR", "PEPE", "AVAX", "SHIB", "LTC", "DOT", "UNI"]
KRAKEN_SYMBOLS = [f"{c}/USD" for c in COINS] + ["BTC/CAD", "USDT/CAD", "USDC/CAD"]
OUT = "ndax"
FLUSH_SEC = 300
SUB_PAUSE = 1.5          # seconds between NDAX subscribe calls

buf = {"l1": [], "trades": [], "kraken": []}
stats = {"started": int(time.time()), "ndax_connects": 0, "kraken_connects": 0, "ndax_msgs": 0, "kraken_msgs": 0,
         "trades": 0, "l1": 0, "kraken_rows": 0, "errors": [], "instruments": {}, "kraken_rejected": []}
seen_trades = set()
k_last = {}              # symbol -> (second, bid, ask) to keep at most one row per second per symbol


def err(msg):
    stats["errors"].append(f"{dt.datetime.utcnow():%H:%M:%S} {msg}"[:200])
    stats["errors"] = stats["errors"][-30:]
    print("ERR", msg, flush=True)


def frame(seq, name, payload, m=0):
    return json.dumps({"m": m, "i": seq, "n": name, "o": json.dumps(payload)})


def handle_ndax(msg, inst_coin):
    m = json.loads(msg)
    n = m.get("n", ""); o = m.get("o", "")
    stats["ndax_msgs"] += 1
    try:
        o = json.loads(o) if isinstance(o, str) and o else o
    except Exception:
        return n, None
    now = int(time.time() * 1000)
    if n in ("SubscribeLevel1", "Level1UpdateEvent") and isinstance(o, dict):
        c = inst_coin.get(o.get("InstrumentId"))
        if c:
            buf["l1"].append([o.get("TimeStamp") or now, now, c, o.get("BestBid"), o.get("BestOffer"), o.get("LastTradedPx"),
                              o.get("LastTradedQty"), o.get("BidQty"), o.get("AskQty")])
            stats["l1"] += 1
    elif n in ("SubscribeTrades", "TradeDataUpdateEvent") and isinstance(o, list):
        for t in o:
            # [TradeId, InstrumentId, Quantity, Price, Order1, Order2, TradeTime(ms), Direction, TakerSide, BlockTrade, ...]
            if not isinstance(t, list) or len(t) < 9:
                continue
            tid = t[0]
            if tid in seen_trades:
                continue
            seen_trades.add(tid)
            c = inst_coin.get(t[1])
            if c:
                buf["trades"].append([t[6], now, c, t[3], t[2], t[8], t[7], tid])
                stats["trades"] += 1
    return n, o


async def ndax_loop(stop_at):
    backoff = 5
    while time.time() < stop_at:
        try:
            async with websockets.connect(NDAX_WS, ping_interval=20, ping_timeout=20, max_size=2 ** 22) as ws:
                stats["ndax_connects"] += 1
                seq = 2
                await ws.send(frame(seq, "GetInstruments", {"OMSId": 1}))
                inst_coin = {}
                while not inst_coin:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 30))
                    if m.get("n") == "GetInstruments":
                        for x in json.loads(m["o"]):
                            p1, p2 = x.get("Product1Symbol"), x.get("Product2Symbol")
                            if p2 == "CAD" and p1 in COINS:
                                inst_coin[x["InstrumentId"]] = p1
                stats["instruments"] = {v: k for k, v in inst_coin.items()}
                for iid in sorted(inst_coin):
                    await asyncio.sleep(SUB_PAUSE); seq += 2
                    await ws.send(frame(seq, "SubscribeLevel1", {"OMSId": 1, "InstrumentId": iid}))
                    await asyncio.sleep(SUB_PAUSE); seq += 2
                    await ws.send(frame(seq, "SubscribeTrades", {"OMSId": 1, "InstrumentId": iid, "IncludeLastCount": 50}))
                backoff = 5
                while time.time() < stop_at:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), 60)
                    except asyncio.TimeoutError:
                        continue
                    n, o = handle_ndax(msg, inst_coin)
                    if json.loads(msg).get("m") == 5:
                        err(f"ndax error reply {n}: {str(o)[:120]}")
                return
        except Exception as e:
            err(f"ndax connection: {type(e).__name__} {str(e)[:120]}")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 300)


async def kraken_loop(stop_at):
    backoff = 5
    while time.time() < stop_at:
        try:
            async with websockets.connect(KRAKEN_WS, ping_interval=20, ping_timeout=20, max_size=2 ** 22) as ws:
                stats["kraken_connects"] += 1
                for i, s in enumerate(KRAKEN_SYMBOLS):
                    await ws.send(json.dumps({"method": "subscribe", "params": {"channel": "ticker", "symbol": [s], "event_trigger": "bbo"}, "req_id": i}))
                    await asyncio.sleep(0.2)
                backoff = 5
                while time.time() < stop_at:
                    try:
                        msg = json.loads(await asyncio.wait_for(ws.recv(), 60))
                    except asyncio.TimeoutError:
                        continue
                    stats["kraken_msgs"] += 1
                    if msg.get("method") == "subscribe" and not msg.get("success", True):
                        sym = (msg.get("result") or {}).get("symbol") or msg.get("symbol") or msg.get("req_id")
                        if sym not in stats["kraken_rejected"]:
                            stats["kraken_rejected"].append(sym)
                        continue
                    if msg.get("channel") != "ticker":
                        continue
                    now = time.time(); sec = int(now)
                    for d in msg.get("data", []):
                        s, b, a = d.get("symbol"), d.get("bid"), d.get("ask")
                        prev = k_last.get(s)
                        if prev and prev[0] == sec:
                            k_last[s] = (sec, b, a, True)      # remember latest, write at most once a second
                            continue
                        k_last[s] = (sec, b, a, False)
                        buf["kraken"].append([int(now * 1000), s, b, a])
                        stats["kraken_rows"] += 1
                return
        except Exception as e:
            err(f"kraken connection: {type(e).__name__} {str(e)[:120]}")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 300)


HEAD = {"l1": ["exch_ts_ms", "recv_ts_ms", "coin", "bid", "ask", "last", "last_qty", "bid_qty", "ask_qty"],
        "trades": ["trade_ts_ms", "recv_ts_ms", "coin", "price", "qty", "taker_side", "direction", "trade_id"],
        "kraken": ["recv_ts_ms", "symbol", "bid", "ask"]}


def flush(final=False):
    stamp = dt.datetime.utcnow()
    for k, rows in buf.items():
        if not rows:
            continue
        d = f"{OUT}/{k}/{stamp:%Y%m%d}"; os.makedirs(d, exist_ok=True)
        s = io.StringIO(); w = csv.writer(s); w.writerow(HEAD[k]); w.writerows(rows)
        with gzip.open(f"{d}/{stamp:%H%M%S}.csv.gz", "wt") as f:
            f.write(s.getvalue())
        buf[k] = []
    stats["last_flush"] = f"{stamp:%Y-%m-%d %H:%M:%S}"
    os.makedirs(OUT, exist_ok=True)
    json.dump(stats, open(f"{OUT}/status.json", "w"), indent=0)
    for cmd in (["git", "add", OUT], ["git", "commit", "-q", "-m", "NDAX recorder update"]):
        subprocess.run(cmd, capture_output=True)
    for attempt in range(4):
        a = subprocess.run(["git", "pull", "-q", "--rebase", "origin", "main"], capture_output=True)
        b = subprocess.run(["git", "push", "-q"], capture_output=True)
        if b.returncode == 0:
            break
        time.sleep(5 + 5 * attempt)
    else:
        err("save failed this time")
    print("flushed", {k: stats[k] for k in ("trades", "l1", "kraken_rows", "ndax_connects", "kraken_connects")}, flush=True)


async def flusher(stop_at):
    while time.time() < stop_at:
        await asyncio.sleep(min(FLUSH_SEC, max(1, stop_at - time.time())))
        await asyncio.get_running_loop().run_in_executor(None, flush)


async def main(seconds):
    stop_at = time.time() + seconds
    await asyncio.gather(ndax_loop(stop_at), kraken_loop(stop_at), flusher(stop_at))
    flush(final=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--seconds", type=int, default=3300)
    asyncio.run(main(ap.parse_args().seconds))
