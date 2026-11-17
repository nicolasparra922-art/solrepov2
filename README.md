# sol_repo

A read-only recorder of public market data for newly created Solana tokens.

Every 5 minutes it lists recently created pools that are attracting trading, then saves price, pool size
and buy/sell counts for each token it is following (48 hours per token), plus a one-time safety summary.

- Sources: DexScreener, GeckoTerminal and RugCheck public APIs.
- It holds no wallet, uses no keys and places no trades.
- Data is in `data/`: `tokens/` (one file per day of first sighting) and `snap/` (one small file per reading).

## NDAX recorder

`scripts/ndax_recorder.py` records NDAX's public live feed (best bid and ask, and every trade) for a set of
Canadian-dollar markets, plus Kraken's best bid and ask once a second, into `ndax/`. It uses one public
connection to each exchange, with no account, no keys and no orders.
