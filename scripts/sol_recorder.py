"""Recorder for new Solana tokens. Read-only: public data, no wallet, no keys, no trades.

Each run it (1) looks for recently created pools that are attracting trading, (2) saves a snapshot of
price, liquidity and buy/sell counts for every token it is tracking, for 48 hours after first sight,
and (3) fetches a safety summary once per token. Output goes to data/. Tokens are snapshotted every run for their first 6 hours, then every 30 minutes.
"""
import csv, datetime as dt, glob, gzip, io, json, os, time, urllib.request

DIR = "data"
GT = "https://api.geckoterminal.com/api/v2"
DS = "https://api.dexscreener.com"
RC = "https://api.rugcheck.xyz/v1"
TRACK_HOURS = 48          # follow each token this long after first sight
MAX_POOL_AGE_H = 72       # only pick up pools created within this window
MIN_LIQ = 15000           # USD in the pool at first sight
MAX_ACTIVE = 5000
FAST_HOURS = 6           # snapshot every run while a token is this new
SKIP = {"So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
FIELDS = ["ts", "mint", "price_usd", "liq_usd", "fdv", "mcap", "vol_m5", "vol_h1", "vol_h6", "vol_h24",
          "buys_m5", "sells_m5", "buys_h1", "sells_h1", "pc_m5", "pc_h1", "pc_h6", "pc_h24", "dex", "pair"]


def get(url, timeout=20):
    req = urllib.request.Request(url, headers={"accept": "application/json", "User-Agent": "Mozilla/5.0 (research recorder)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def iso_ts(s):
    try:
        return int(dt.datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def num(x):
    try:
        return float(x)
    except Exception:
        return None


def discover_gt(now, diag):
    """Candidates from GeckoTerminal lists: mint -> info."""
    out = {}
    lists = [("trending_5m", f"{GT}/networks/solana/trending_pools?page=1&duration=5m"),
             ("trending_1h", f"{GT}/networks/solana/trending_pools?page=1&duration=1h"),
             ("new_p1", f"{GT}/networks/solana/new_pools?page=1"),
             ("new_p2", f"{GT}/networks/solana/new_pools?page=2"),
             ("pumpswap_p1", f"{GT}/networks/solana/dexes/pumpswap/pools?page=1&sort=h24_tx_count_desc"),
             ("pumpswap_p2", f"{GT}/networks/solana/dexes/pumpswap/pools?page=2&sort=h24_tx_count_desc")]
    for name, url in lists:
        try:
            data = get(url).get("data", [])
        except Exception as e:
            diag["errors"].append(f"{name}: {str(e)[:80]}"); time.sleep(3.2); continue
        kept = 0
        for p in data:
            try:
                a = p["attributes"]; rel = p.get("relationships", {})
                dex = rel.get("dex", {}).get("data", {}).get("id", "")
                mint = rel.get("base_token", {}).get("data", {}).get("id", "").replace("solana_", "")
                created = iso_ts(a.get("pool_created_at")); liq = num(a.get("reserve_in_usd")) or 0
                diag["dex_seen"][dex] = diag["dex_seen"].get(dex, 0) + 1
                if not mint or mint in SKIP or not created: continue
                if now - created > MAX_POOL_AGE_H * 3600 or liq < MIN_LIQ: continue
                out.setdefault(mint, {"pool": a.get("address"), "dex": dex, "created": created, "name": a.get("name"), "src": name,
                                      "liq0": liq, "fdv0": num(a.get("fdv_usd"))})
                kept += 1
            except Exception:
                continue
        diag["lists"][name] = [len(data), kept]
        time.sleep(3.2)
    return out


def discover_ds(diag):
    """Mints from DexScreener's latest profiles and boosts (checked for age and liquidity at snapshot time)."""
    mints = {}
    for name, url in (("ds_profiles", f"{DS}/token-profiles/latest/v1"), ("ds_boosts", f"{DS}/token-boosts/latest/v1")):
        try:
            data = get(url)
            n = 0
            for e in data if isinstance(data, list) else []:
                if e.get("chainId") == "solana" and e.get("tokenAddress") and e["tokenAddress"] not in SKIP:
                    mints.setdefault(e["tokenAddress"], name); n += 1
            diag["lists"][name] = [len(data) if isinstance(data, list) else 0, n]
        except Exception as e:
            diag["errors"].append(f"{name}: {str(e)[:80]}")
    return mints


def snapshots(mints, diag):
    """Best (deepest) pair per mint from DexScreener, 30 mints per call."""
    best = {}
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        try:
            data = get(f"{DS}/tokens/v1/solana/{','.join(chunk)}")
        except Exception as e:
            diag["errors"].append(f"snapshot {i}: {str(e)[:80]}"); time.sleep(1); continue
        for p in data if isinstance(data, list) else []:
            try:
                m = p["baseToken"]["address"]
                liq_raw = num((p.get("liquidity") or {}).get("usd"))   # often missing for pumpswap pools
                liq = liq_raw or 0                                      # 0 only for choosing the deepest pool
                if m not in best or liq > best[m]["_liq_cmp"]:
                    p["_liq"] = liq_raw; p["_liq_cmp"] = liq; best[m] = p   # recorded blank when unknown, not 0
            except Exception:
                continue
        time.sleep(0.25)
    return best


def row(now, m, p):
    tx = p.get("txns") or {}; v = p.get("volume") or {}; pc = p.get("priceChange") or {}
    g = lambda d, k: (d.get(k) or {}) if isinstance(d.get(k), dict) else {}
    return [now, m, p.get("priceUsd"), p.get("_liq"), p.get("fdv"), p.get("marketCap"), v.get("m5"), v.get("h1"), v.get("h6"), v.get("h24"),
            g(tx, "m5").get("buys"), g(tx, "m5").get("sells"), g(tx, "h1").get("buys"), g(tx, "h1").get("sells"),
            pc.get("m5"), pc.get("h1"), pc.get("h6"), pc.get("h24"), p.get("dexId"), p.get("pairAddress")]


def safety(mint):
    j = get(f"{RC}/tokens/{mint}/report/summary", timeout=15)
    return {"score": j.get("score"), "score_norm": j.get("score_normalised"), "lp_locked_pct": j.get("lpLockedPct"),
            "risks": [[r.get("name"), r.get("level"), r.get("score")] for r in (j.get("risks") or [])][:12]}


def day_of(ts):
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y%m%d")


def load_tokens(now):
    """Registry is split into one file per first-seen day; only the last three days can still be active."""
    files = {}
    for d in range(0, 4):
        k = day_of(now - d * 86400); p = f"{DIR}/tokens/{k}.json"
        files[k] = json.load(open(p)) if os.path.exists(p) else {}
    known = set()
    if os.path.exists(f"{DIR}/known.txt"):
        known = set(open(f"{DIR}/known.txt").read().split())
    return files, known


def run():
    os.makedirs(f"{DIR}/tokens", exist_ok=True)
    now = int(time.time())
    files, known = load_tokens(now)
    tokens = {}
    for k in files: tokens.update(files[k])
    diag = {"ts": now, "errors": [], "lists": {}, "dex_seen": {}}

    found = discover_gt(now, diag)
    ds = discover_ds(diag)
    active = [m for m, t in tokens.items() if now - t["first_seen"] <= TRACK_HOURS * 3600]
    minute = dt.datetime.utcfromtimestamp(now).minute
    slow_turn = minute % 30 < 5
    due = [m for m in active if slow_turn or now - tokens[m]["first_seen"] <= FAST_HOURS * 3600]
    probe = [m for m in list(found) + list(ds) if m not in tokens and m not in known]
    probe = list(dict.fromkeys(probe))[:max(0, MAX_ACTIVE - len(active))]
    snaps = snapshots(due + probe, diag)

    added = 0; today = day_of(now)
    for m in probe:
        p = snaps.get(m); info = found.get(m)
        created = (int(p["pairCreatedAt"]) // 1000 if p and p.get("pairCreatedAt") else 0) or (info or {}).get("created", 0)
        liq = (p or {}).get("_liq") or (info or {}).get("liq0") or 0
        if not created or now - created > MAX_POOL_AGE_H * 3600 or liq < MIN_LIQ:
            continue
        files[today][m] = tokens[m] = {"first_seen": now, "created": created, "src": (info or {}).get("src") or ds.get(m),
                     "name": (info or {}).get("name") or ((p or {}).get("baseToken") or {}).get("symbol"),
                     "symbol": ((p or {}).get("baseToken") or {}).get("symbol"),
                     "dex": (p or {}).get("dexId") or (info or {}).get("dex"), "pool": (p or {}).get("pairAddress") or (info or {}).get("pool"),
                     "liq0": liq, "fdv0": (p or {}).get("fdv") or (info or {}).get("fdv0"), "price0": (p or {}).get("priceUsd")}
        due.append(m); added += 1

    stamp = dt.datetime.utcfromtimestamp(now); sdir = f"{DIR}/snap/{stamp.strftime('%Y%m%d')}"; os.makedirs(sdir, exist_ok=True)
    buf = io.StringIO(); w = csv.writer(buf); w.writerow(FIELDS); wrote = 0
    for m in due:
        if m in snaps:
            w.writerow(row(now, m, snaps[m])); wrote += 1
    if wrote:
        with gzip.open(f"{sdir}/{stamp.strftime('%H%M')}.csv.gz", "wt") as f:
            f.write(buf.getvalue())

    checked = 0
    for m in [m for m in active + [x for x in due if x not in active] if "safety" not in tokens[m]][:25]:
        try:
            tokens[m]["safety"] = safety(m); checked += 1
        except Exception as e:
            tokens[m]["safety_err"] = tokens[m].get("safety_err", 0) + 1
            if tokens[m]["safety_err"] >= 3: tokens[m]["safety"] = None
            diag["errors"].append(f"safety: {str(e)[:60]}")
        time.sleep(1.0)

    for k, d in files.items():
        if d: json.dump(d, open(f"{DIR}/tokens/{k}.json", "w"))
    if added:
        with open(f"{DIR}/known.txt", "a") as f:
            f.write("".join(m + "\n" for m in probe if m in tokens))
    total = sum(len(json.load(open(p))) for p in glob.glob(f"{DIR}/tokens/*.json"))
    diag.update({"tracked_total": total, "active": len(active) + added, "added": added, "snapshots": wrote, "safety_checked": checked,
                 "seconds": int(time.time()) - now})
    diag["errors"] = diag["errors"][:20]
    json.dump(diag, open(f"{DIR}/last_run.json", "w"))
    print("sol recorder:", {k: diag[k] for k in ("tracked_total", "active", "added", "snapshots", "safety_checked", "seconds")}, "errors", len(diag["errors"]), flush=True)


if __name__ == "__main__":
    run()
