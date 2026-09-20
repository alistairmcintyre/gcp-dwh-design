"""Generate synthetic AppsFlyer + CFD/spread-betting trading raw data.

Writes four tables into a ``raw`` schema/dataset that the dbt project reads as sources:

    raw.appsflyer_events     mobile-attribution events (install, registration, login, first_deposit)
    raw.clients              registered clients, with MiFID II categorisation and account status
    raw.orders               client instructions (market / limit / stop)
    raw.trades               executions against those orders -- an order fills in 0..n trades
    raw.quotes               sampled bid/ask ticks per instrument
    raw.account_transactions client-money deposits and withdrawals

Locally the destination is a DuckDB file (``--target duckdb``, the default); for the BigQuery flow
(``--target bigquery``) the same frames are loaded into a BigQuery dataset using Application Default
Credentials. The generator is deterministic for a given ``--seed`` so re-runs reproduce identical data,
which lets the incremental dbt models be exercised for idempotency.

Examples
--------
    python scripts/generate_test_data.py                       # -> data/dev.duckdb
    python scripts/generate_test_data.py --clients 5000 --days 120
    python scripts/generate_test_data.py --target bigquery --gcp-project my-proj --bq-raw-dataset raw
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Distributions (kept small + readable so the demo data is easy to reason about)
# ---------------------------------------------------------------------------
MEDIA_SOURCES = {
    "facebook_ads": 0.28,
    "google_search_ads": 0.24,
    "tiktok_ads": 0.16,
    "apple_search_ads": 0.12,
    "organic": 0.20,
}
PAID_SOURCES = {"facebook_ads", "google_search_ads", "tiktok_ads", "apple_search_ads"}
# install -> registration conversion rate by channel
REG_RATE = {
    "facebook_ads": 0.55,
    "google_search_ads": 0.62,
    "tiktok_ads": 0.48,
    "apple_search_ads": 0.58,
    "organic": 0.70,
}
PLATFORMS = {"ios": 0.45, "android": 0.55}
COUNTRIES = {"GB": 0.38, "DE": 0.14, "IE": 0.08, "ES": 0.08, "AU": 0.12, "SG": 0.08, "US": 0.12}
# Tradable instruments, keyed by a short instrument code.
#   instrument_id -> (instrument_name, asset_class, reference_price, spread_bps, annual_funding_rate)
MARKETS = {
    "EURUSD": ("EUR/USD",     "FX",              1.08,   0.6,  0.045),
    "GBPUSD": ("GBP/USD",     "FX",              1.27,   0.9,  0.048),
    "USDJPY": ("USD/JPY",     "FX",            157.40,   0.7,  0.030),
    "UK100":  ("FTSE 100",    "INDICES",      8200.0,    1.0,  0.052),
    "US500":  ("S&P 500",     "INDICES",      5600.0,    0.5,  0.055),
    "DE40":   ("DAX 40",      "INDICES",     18500.0,    1.2,  0.040),
    "XAUUSD": ("Gold",        "COMMODITIES",  2400.0,    3.0,  0.050),
    "BRENT":  ("Brent Crude", "COMMODITIES",    82.0,    2.8,  0.050),
    "AAPL":   ("Apple Inc",   "SHARES",        225.0,    8.0,  0.058),
    "TSLA":   ("Tesla Inc",   "SHARES",        250.0,   12.0,  0.058),
    "BTCUSD": ("Bitcoin",     "CRYPTO",      65000.0,   30.0,  0.090),
}
INSTRUMENTS = list(MARKETS)
INSTRUMENT_WEIGHTS = {i: w for i, w in zip(
    INSTRUMENTS, [0.16, 0.10, 0.07, 0.13, 0.14, 0.07, 0.08, 0.06, 0.07, 0.06, 0.06])}
PRODUCT_TYPES = {"CFD": 0.62, "SPREAD_BET": 0.38}
ORDER_TYPES = {"MARKET": 0.58, "LIMIT": 0.27, "STOP": 0.15}
# MiFID II categorisation. Professionals are a small minority but trade much larger.
CLIENT_CATEGORIES = {"RETAIL": 0.93, "PROFESSIONAL": 0.05, "ELECTIVE_PROFESSIONAL": 0.02}
# Account lifecycle. RESTRICTED/SUSPENDED carry a reason that must suppress marketing downstream.
STATUS_REASONS = {
    "RESTRICTED": ["APPROPRIATENESS_FAILED", "KYC_EXPIRED", "NEGATIVE_BALANCE"],
    "SUSPENDED": ["VULNERABLE_CLIENT", "KYC_EXPIRED"],
    "CLOSED": ["CLIENT_REQUEST", "VULNERABLE_CLIENT"],
}
CAMPAIGNS = ["brand_generic", "acq_prospecting", "retargeting", "lookalike_2pct", "seasonal_promo"]

# The platform does not profit from client losses -- it profits from the dealing spread, commission on share
# CFDs, and overnight funding. Client P&L is modelled independently of platform revenue for exactly that
# reason, and the two are reconciled by a singular dbt test.
COMMISSION_BPS_SHARES = 10.0  # share CFDs carry explicit commission; other markets do not

# Synthetic personal data. These four columns exist purely to demonstrate BigQuery column-level
# security: they carry Dataplex policy tags (see terraform/modules/governance) and are masked at query
# time for principals holding only the Masked Reader role. Nothing here is real personal data --
# the pools are small and deliberately obviously fake.
FIRST_NAMES = [
    "Amelia", "Oliver", "Isla", "Noah", "Ava", "Leo", "Freya", "Arthur", "Sofia", "Jack",
    "Mia", "Ethan", "Lily", "Mateo", "Clara", "Hugo", "Nora", "Liam", "Elena", "Finn",
]
LAST_NAMES = [
    "Okafor", "Mccarthy", "Nowak", "Silva", "Fischer", "Duarte", "Kaur", "Novak", "Moreau", "Rossi",
    "Andersen", "Petrov", "Garcia", "Hoffmann", "Murphy", "Lindqvist", "Costa", "Weber", "Nagy", "Reyes",
]
EMAIL_DOMAINS = ["example.com", "example.net", "example.org"]



def _choice(rng: np.random.Generator, mapping: dict[str, float], size: int) -> np.ndarray:
    keys = list(mapping.keys())
    probs = np.array(list(mapping.values()), dtype=float)
    probs = probs / probs.sum()
    return rng.choice(keys, size=size, p=probs)


def _rand_times(rng: np.random.Generator, start_s: np.ndarray, end_s: int, size: int) -> np.ndarray:
    """Uniform unix-second timestamps in [start_s, end_s), per-element start."""
    frac = rng.random(size)
    return (start_s + frac * (end_s - start_s)).astype("int64")


def _to_ts(unix_seconds: np.ndarray) -> pd.Series:
    return pd.to_datetime(unix_seconds, unit="s", utc=True)


def build_frames(
    n_devices: int, start: datetime, end: datetime, seed: int
) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    start_s = int(start.timestamp())
    end_s = int(end.timestamp())
    loaded_at = pd.Timestamp.now(tz="UTC")

    # ---- Devices / installs -------------------------------------------------
    device_idx = np.arange(n_devices)
    appsflyer_id = np.array([f"afid-{i:08d}" for i in device_idx])
    media_source = _choice(rng, MEDIA_SOURCES, n_devices)
    platform = _choice(rng, PLATFORMS, n_devices)
    country = _choice(rng, COUNTRIES, n_devices)
    campaign = np.where(
        np.isin(media_source, list(PAID_SOURCES)),
        rng.choice(CAMPAIGNS, size=n_devices),
        "organic",
    )
    install_s = (start_s + rng.random(n_devices) * (end_s - start_s)).astype("int64")
    touch_s = np.where(
        np.isin(media_source, list(PAID_SOURCES)),
        install_s - rng.integers(60, 6 * 3600, n_devices),  # click before install
        -1,
    )

    # ---- Registration -------------------------------------------------------
    reg_prob = np.array([REG_RATE[m] for m in media_source])
    registered = rng.random(n_devices) < reg_prob
    reg_delay = rng.integers(120, 3 * 24 * 3600, n_devices)  # 2 min .. 3 days
    reg_s = install_s + reg_delay
    reg_s = np.where(reg_s < end_s, reg_s, end_s - 1)
    client_id = np.where(registered, np.array([f"cli-{i:08d}" for i in device_idx]), None)

    # active = registered clients who go on to deposit + trade
    active = registered & (rng.random(n_devices) < 0.75)

    # ---- appsflyer_events ---------------------------------------------------
    ev_frames = []

    def _event_frame(mask, name, time_s, with_user):
        idx = np.where(mask)[0]
        if idx.size == 0:
            return None
        return pd.DataFrame(
            {
                "appsflyer_id": appsflyer_id[idx],
                "client_id": (client_id[idx] if with_user else np.full(idx.size, None)),
                "event_name": name,
                "event_time": _to_ts(time_s[idx]),
                "media_source": media_source[idx],
                "campaign": campaign[idx],
                "platform": platform[idx],
                "country": country[idx],
                "attributed_touch_time": pd.to_datetime(
                    np.where(touch_s[idx] > 0, touch_s[idx], np.nan), unit="s", utc=True
                ),
            }
        )

    ev_frames.append(_event_frame(np.ones(n_devices, bool), "install", install_s, with_user=False))
    ev_frames.append(_event_frame(registered, "registration", reg_s, with_user=True))

    # a few logins per active user
    login_counts = np.where(active, rng.poisson(4, n_devices) + 1, 0)
    total_logins = int(login_counts.sum())
    if total_logins:
        owner = np.repeat(device_idx, login_counts)
        login_s = _rand_times(rng, reg_s[owner].astype("int64"), end_s, total_logins)
        ev_frames.append(
            pd.DataFrame(
                {
                    "appsflyer_id": appsflyer_id[owner],
                    "client_id": client_id[owner],
                    "event_name": "login",
                    "event_time": _to_ts(login_s),
                    "media_source": media_source[owner],
                    "campaign": campaign[owner],
                    "platform": platform[owner],
                    "country": country[owner],
                    "attributed_touch_time": pd.NaT,
                }
            )
        )

    # first_deposit event for active users
    first_dep_s = _rand_times(rng, reg_s.astype("int64"), end_s, n_devices)
    ev_frames.append(_event_frame(active, "first_deposit", first_dep_s, with_user=True))

    events = pd.concat([f for f in ev_frames if f is not None], ignore_index=True)
    events.insert(0, "event_id", [f"evt-{i:09d}" for i in range(len(events))])
    events["_loaded_at"] = loaded_at

    # ---- users --------------------------------------------------------------
    # first_name / last_name / date_of_birth / email are the PII columns the column-level security
    # demo masks. They are generated here rather than in dbt so the raw (Bronze) layer holds the
    # unmasked values and every downstream layer inherits the policy tags from the schema YAML.
    reg_idx = np.where(registered)[0]
    n_users = reg_idx.size
    first_name = rng.choice(FIRST_NAMES, size=n_users)
    last_name = rng.choice(LAST_NAMES, size=n_users)
    # Adults only: uniform ages 18-75 at the end of the window, to the day.
    age_days = rng.integers(18 * 365, 75 * 365, n_users)
    date_of_birth = (pd.Timestamp(end).normalize().tz_localize(None) - pd.to_timedelta(age_days, unit="D")).date
    email = pd.Series(
        [
            f"{f}.{l}.{u.split('-')[1]}@{d}".lower()
            for f, l, u, d in zip(
                first_name, last_name, client_id[reg_idx], rng.choice(EMAIL_DOMAINS, size=n_users)
            )
        ]
    )
    # MiFID II categorisation and account lifecycle. `account_status_reason` is the attribute that
    # has to reach marketing suppression fast: a VULNERABLE_CLIENT or APPROPRIATENESS_FAILED client
    # still sitting in an audience is a regulatory breach, not a data-quality issue.
    client_category = _choice(rng, CLIENT_CATEGORIES, n_users)
    status_roll = rng.random(n_users)
    account_status = np.where(
        status_roll < 0.885, "ACTIVE",
        np.where(status_roll < 0.925, "DORMANT",
        np.where(status_roll < 0.960, "RESTRICTED",
        np.where(status_roll < 0.983, "SUSPENDED", "CLOSED"))),
    )
    reason = np.full(n_users, None, dtype=object)
    for state, options in STATUS_REASONS.items():
        mask = account_status == state
        if mask.any():
            reason[mask] = rng.choice(options, size=int(mask.sum()))

    users = pd.DataFrame(
        {
            "client_id": client_id[reg_idx],
            "appsflyer_id": appsflyer_id[reg_idx],
            "first_name": first_name,
            "last_name": last_name,
            "date_of_birth": date_of_birth,
            "email": email,
            "registration_time": _to_ts(reg_s[reg_idx]),
            "country": country[reg_idx],
            "client_category": client_category,
            "account_status": account_status,
            "account_status_reason": reason,
            "acquisition_media_source": media_source[reg_idx],
            "platform": platform[reg_idx],
            "_loaded_at": loaded_at,
        }
    )

    # ---- trades (active clients only) ---------------------------------------
    # A trade is a position opened on a market and later closed. the platform's revenue is the dealing spread
    # plus commission (share CFDs only) plus overnight funding -- NOT the client's loss, which is
    # modelled independently as `client_pnl`.
    active_idx = np.where(active)[0]
    trade_counts = rng.poisson(14, active_idx.size) + 1
    owner = np.repeat(active_idx, trade_counts)
    n_trades = owner.size

    instrument_id = _choice(rng, INSTRUMENT_WEIGHTS, n_trades)
    market_name = np.array([MARKETS[e][0] for e in instrument_id])
    asset_class = np.array([MARKETS[e][1] for e in instrument_id])
    ref_price = np.array([MARKETS[e][2] for e in instrument_id])
    spread_bps = np.array([MARKETS[e][3] for e in instrument_id])
    funding_rate = np.array([MARKETS[e][4] for e in instrument_id])

    product_type = _choice(rng, PRODUCT_TYPES, n_trades)
    direction = np.where(rng.random(n_trades) < 0.54, "BUY", "SELL")
    sign = np.where(direction == "BUY", 1.0, -1.0)

    opened_s = _rand_times(rng, reg_s[owner].astype("int64") + 3600, end_s, n_trades)
    hold_s = rng.integers(300, 12 * 24 * 3600, n_trades)
    closed_s = np.minimum(opened_s + hold_s, end_s)
    days_held = np.maximum((closed_s - opened_s) / 86400.0, 0.0)

    # Opening price jitters around the market reference; the close is a short random walk from it.
    opening_price = np.round(ref_price * (1.0 + rng.normal(0.0, 0.012, n_trades)), 5)
    move_pct = rng.normal(0.0, 0.010, n_trades) + rng.standard_t(3, n_trades) * 0.004
    closing_price = np.round(opening_price * (1.0 + move_pct), 5)

    # Trade size: contracts for a CFD, stake per point for a spread bet. Professionals trade larger.
    is_pro = np.isin(client_category[np.searchsorted(reg_idx, owner)], ["PROFESSIONAL", "ELECTIVE_PROFESSIONAL"])
    size_scale = np.where(is_pro, 6.0, 1.0)
    quantity = np.round(np.clip(rng.gamma(1.8, 1.4, n_trades) * size_scale, 0.1, 400.0), 2)

    notional_value = np.round(quantity * opening_price, 2)
    client_pnl = np.round((closing_price - opening_price) * quantity * sign, 2)

    # platform revenue components.
    spread_revenue = np.round(notional_value * spread_bps / 10000.0, 2)
    commission = np.round(
        np.where(asset_class == "SHARES", notional_value * COMMISSION_BPS_SHARES / 10000.0, 0.0), 2
    )
    funding_charge = np.round(notional_value * funding_rate / 365.0 * days_held, 2)

    currency = np.where(country[owner] == "US", "USD",
               np.where(country[owner] == "GB", "GBP",
               np.where(country[owner] == "AU", "AUD",
               np.where(country[owner] == "SG", "SGD", "EUR"))))

    roll = rng.random(n_trades)
    status = np.where(roll < 0.015, "CANCELLED",
             np.where(roll < 0.085, "STOPPED_OUT",
             np.where(roll < 0.125, "OPEN",
             np.where(roll < 0.175, "PART_CLOSED", "CLOSED"))))

    # A stop-out closes at a loss by construction.
    stopped = status == "STOPPED_OUT"
    client_pnl = np.where(stopped, -np.abs(client_pnl) - np.round(quantity * opening_price * 0.004, 2), client_pnl)
    closing_price = np.where(stopped, np.round(opening_price * (1.0 - 0.004 * sign * np.sign(sign)), 5), closing_price)

    # A cancelled trade never reached the market: no price, no P&L, no revenue.
    cancelled = status == "CANCELLED"
    still_open = status == "OPEN"
    no_close = cancelled | still_open
    client_pnl = np.where(cancelled, 0.0, client_pnl)
    spread_revenue = np.where(cancelled, 0.0, spread_revenue)
    commission = np.where(cancelled, 0.0, commission)
    funding_charge = np.where(cancelled, 0.0, funding_charge)

    closed_ts = _to_ts(closed_s)
    trades = pd.DataFrame(
        {
            "trade_id": [f"TRD{i:012d}" for i in range(n_trades)],
            "order_id": [f"ORD{i:012d}" for i in range(n_trades)],
            "client_id": client_id[owner],
            "instrument_id": instrument_id,
            "market_name": market_name,
            "asset_class": asset_class,
            "product_type": product_type,
            "direction": direction,
            "opened_at": _to_ts(opened_s),
            "closed_at": closed_ts.where(~pd.Series(no_close), other=pd.NaT),
            "status": status,
            "currency": currency,
            "quantity": quantity,
            "opening_price": opening_price,
            "closing_price": pd.Series(closing_price).where(~pd.Series(no_close), other=np.nan),
            "notional_value": notional_value,
            "client_pnl": np.round(client_pnl, 2),
            "spread_revenue": np.round(spread_revenue, 2),
            "commission": np.round(commission, 2),
            "funding_charge": np.round(funding_charge, 2),
            "_loaded_at": loaded_at,
        }
    )

    # ---- orders (the instruction) -------------------------------------------
    # An order is what the client asked for; a trade is what the market gave them. They are separate
    # entities because the relationship is 1:0..n -- a working order may never fill, and a large one
    # commonly fills in several pieces at different prices. Collapsing them into one row is the
    # classic modelling error here: it makes partial fills invisible and average execution price
    # impossible to compute honestly.
    order_type = _choice(rng, ORDER_TYPES, n_trades)
    order_roll = rng.random(n_trades)
    order_status = np.where(order_roll < 0.03, "REJECTED",
                   np.where(order_roll < 0.09, "CANCELLED",
                   np.where(order_roll < 0.14, "WORKING",
                   np.where(order_roll < 0.30, "PART_FILLED", "FILLED"))))
    # A market order is never left working; it fills or it is rejected.
    order_status = np.where((order_type == "MARKET") & (order_status == "WORKING"), "FILLED", order_status)
    placed_s = opened_s - rng.integers(1, 900, n_trades)

    orders = pd.DataFrame(
        {
            "order_id": [f"ORD{i:012d}" for i in range(n_trades)],
            "client_id": client_id[owner],
            "instrument_id": instrument_id,
            "side": direction,
            "order_type": order_type,
            "quantity": quantity,
            "limit_price": np.where(order_type == "LIMIT", np.round(opening_price * 0.998, 5), np.nan),
            "stop_price": np.where(order_type == "STOP", np.round(opening_price * 1.002, 5), np.nan),
            "status": order_status,
            "placed_at": _to_ts(placed_s),
            "currency": currency,
            "_loaded_at": loaded_at,
        }
    )

    # An order that never reached the market has no executions behind it. Dropping those trades here
    # is what makes the order:trade relationship 1:0..n rather than a disguised 1:1 -- and it is what
    # the `assert_cancelled_orders_have_no_trades` test exists to protect.
    executed = np.isin(order_status, ["FILLED", "PART_FILLED"])
    trades = trades.loc[executed].reset_index(drop=True)

    # ---- quotes (market data) -----------------------------------------------
    # Sampled, not tick-for-tick: the full feed belongs on the trading platform, not the warehouse.
    # Retained for execution-quality analysis -- comparing fill price against the prevailing quote is
    # how you evidence best execution, which is a regulatory obligation rather than an analytics nicety.
    n_quote_days = (end_s - start_s) // 86400 + 1
    quote_rows = []
    for e, (_name, _ac, ref, spread_bps, _fund) in MARKETS.items():
        ticks = int(n_quote_days) * 24
        ts = start_s + rng.integers(0, max(end_s - start_s, 1), ticks)
        mid = ref * (1.0 + rng.normal(0.0, 0.006, ticks))
        half = mid * (spread_bps / 10000.0) / 2.0
        quote_rows.append(
            pd.DataFrame(
                {
                    "instrument_id": e,
                    "quote_time": _to_ts(np.sort(ts)),
                    "bid": np.round(mid - half, 5),
                    "ask": np.round(mid + half, 5),
                    "mid": np.round(mid, 5),
                }
            )
        )
    quotes = pd.concat(quote_rows, ignore_index=True)
    quotes["_loaded_at"] = loaded_at

    # ---- client-money transactions (active clients only) --------------------
    dep_counts = rng.poisson(2, active_idx.size) + 1
    wd_counts = rng.poisson(0.8, active_idx.size)
    dep_owner = np.repeat(active_idx, dep_counts)
    wd_owner = np.repeat(active_idx, wd_counts)
    dep_s = _rand_times(rng, reg_s[dep_owner].astype("int64"), end_s, dep_owner.size)
    wd_s = _rand_times(rng, reg_s[wd_owner].astype("int64") + 3600, end_s, wd_owner.size)
    dep_amt = np.round(np.clip(rng.gamma(2.2, 22.0, dep_owner.size), 5.0, 2000.0), 2)
    wd_amt = np.round(np.clip(rng.gamma(2.0, 30.0, wd_owner.size), 5.0, 3000.0), 2)
    tx_owner = np.concatenate([dep_owner, wd_owner])
    tx = pd.DataFrame(
        {
            "client_id": np.concatenate([client_id[dep_owner], client_id[wd_owner]]),
            "created_at": _to_ts(np.concatenate([dep_s, wd_s])),
            "transaction_type": (["deposit"] * dep_owner.size) + (["withdrawal"] * wd_owner.size),
            "amount": np.concatenate([dep_amt, wd_amt]),
        }
    )
    status_roll = rng.random(len(tx))
    tx["status"] = np.where(status_roll < 0.94, "completed", np.where(status_roll < 0.98, "pending", "failed"))
    tx["currency"] = np.where(country[tx_owner] == "US", "USD",
                      np.where(country[tx_owner] == "GB", "GBP",
                      np.where(country[tx_owner] == "AU", "AUD",
                      np.where(country[tx_owner] == "SG", "SGD", "EUR"))))
    tx = tx.sort_values("created_at").reset_index(drop=True)
    tx.insert(0, "transaction_id", [f"txn-{i:09d}" for i in range(len(tx))])
    tx["_loaded_at"] = loaded_at

    # A queue of erasure requests, so the sweep and its tests have something real to run against.
    # Two are outstanding, one of them old enough to be near the one-month deadline, and one was
    # dealt with last week. The completed row is what proves the sweep does not process twice.
    erased_sample = users["client_id"].iloc[[3, 11, 17]].tolist()
    erasure_requests = pd.DataFrame(
        {
            "client_id": erased_sample,
            "requested_at": [
                end - timedelta(days=2),
                end - timedelta(days=25),
                end - timedelta(days=9),
            ],
            "completed_at": [pd.NaT, pd.NaT, end - timedelta(days=7)],
            "rows_deleted": [None, None, 14],
            "_loaded_at": loaded_at,
        }
    )

    # The completed request is a past erasure, so the data really is gone: the client row and their
    # attribution events. Their trades and account transactions stay, which is the retention
    # obligation in privacy/erasure_targets.yaml, and is why the relationships tests on those tables
    # allow an orphan for a subject under an erasure request.
    already_erased = erased_sample[2]
    users = users[users["client_id"] != already_erased].reset_index(drop=True)
    events = events[events["client_id"] != already_erased].reset_index(drop=True)

    return {
        "appsflyer_events": events,
        "clients": users,
        "erasure_requests": erasure_requests,
        "orders": orders,
        "trades": trades,
        "quotes": quotes,
        "account_transactions": tx,
    }


# ---------------------------------------------------------------------------
# Crypto shredding
# ---------------------------------------------------------------------------
def encrypt_pii(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Replace the personal columns with ciphertext, one key per client.

    This is what a producer would do before the data ever reaches Kafka, so no plaintext exists
    downstream to go looking for later. The wrapped keys come back as their own frame, written to a
    separate schema that nothing else reads and that must be left out of every backup.

    date_of_birth moves to its own column because it is a date, and a date column cannot hold
    ciphertext without changing type on every model that reads it. The original is nulled rather
    than dropped so the shape of the table, and every contract written against it, still holds.
    """
    # Run as a file rather than a module, so the repo root is not on the path by default.
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    from privacy.crypto import encrypt_field, new_data_key, wrap

    clients = frames["clients"].copy()
    keys = {client_id: new_data_key() for client_id in clients["client_id"]}

    clients["first_name"] = [
        encrypt_field(v, keys[c]) for v, c in zip(clients["first_name"], clients["client_id"], strict=True)
    ]
    clients["last_name"] = [
        encrypt_field(v, keys[c]) for v, c in zip(clients["last_name"], clients["client_id"], strict=True)
    ]
    # Deterministic for the email so a client's own rows still match each other, which is what the
    # marketing join needs. Randomised for the names, where nothing downstream joins on the value.
    clients["email"] = [
        encrypt_field(v, keys[c], deterministic=True)
        for v, c in zip(clients["email"], clients["client_id"], strict=True)
    ]
    clients["date_of_birth_encrypted"] = [
        encrypt_field(v.isoformat() if v is not None else None, keys[c])
        for v, c in zip(clients["date_of_birth"], clients["client_id"], strict=True)
    ]
    clients["date_of_birth"] = pd.NaT

    frames = dict(frames)
    frames["clients"] = clients
    frames["privacy.subject_keys"] = pd.DataFrame(
        {
            "subject_id": list(keys),
            "wrapped_key": [wrap(key) for key in keys.values()],
            "created_at": datetime.now(timezone.utc),
        }
    )
    return frames


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def write_duckdb(frames: dict[str, pd.DataFrame], path: str) -> None:
    import duckdb

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    con = duckdb.connect(path)
    try:
        con.execute("create schema if not exists raw")
        for name, df in frames.items():
            schema, _, table = name.rpartition(".")
            schema = schema or "raw"
            con.execute(f"create schema if not exists {schema}")
            con.register("df_tmp", df)
            con.execute(f"create or replace table {schema}.{table} as select * from df_tmp")
            con.unregister("df_tmp")
    finally:
        con.close()


def write_bigquery(frames: dict[str, pd.DataFrame], project: str, dataset: str, location: str) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    ds_ref = bigquery.Dataset(f"{project}.{dataset}")
    ds_ref.location = location
    client.create_dataset(ds_ref, exists_ok=True)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    for name, df in frames.items():
        # A frame named schema.table goes to its own dataset. The key vault lives apart from the
        # warehouse so it can be excluded from copies and exports without excluding anything else.
        target_dataset, _, table = name.rpartition(".")
        target_dataset = target_dataset or dataset
        if target_dataset != dataset:
            vault_ref = bigquery.Dataset(f"{project}.{target_dataset}")
            vault_ref.location = location
            client.create_dataset(vault_ref, exists_ok=True)
        table_id = f"{project}.{target_dataset}.{table}"
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--target", choices=["duckdb", "bigquery"], default="duckdb")
    p.add_argument("--clients", type=int, default=2500, help="number of installs/devices to generate")
    p.add_argument("--days", type=int, default=90, help="history length ending at --end")
    p.add_argument("--end", default=None, help="ISO end date (default: now, UTC)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--encrypt-pii",
        action="store_true",
        help="write the personal columns as per-client ciphertext and emit the key vault "
             "(off by default: the column masking demo needs readable values)",
    )
    p.add_argument("--duckdb-path", default=os.environ.get("DUCKDB_PATH", "data/dev.duckdb"))
    p.add_argument("--gcp-project", default=os.environ.get("GCP_PROJECT"))
    p.add_argument("--bq-raw-dataset", default=os.environ.get("BQ_RAW_DATASET", "raw"))
    p.add_argument("--bq-location", default=os.environ.get("BQ_LOCATION", "EU"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    end = (
        datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        if args.end
        else datetime.now(timezone.utc)
    )
    start = end - timedelta(days=args.days)

    frames = build_frames(args.clients, start, end, args.seed)
    if args.encrypt_pii:
        frames = encrypt_pii(frames)
    rows = {k: len(v) for k, v in frames.items()}

    if args.target == "duckdb":
        write_duckdb(frames, args.duckdb_path)
        dest = args.duckdb_path
    else:
        if not args.gcp_project:
            raise SystemExit("--gcp-project (or GCP_PROJECT) is required for --target bigquery")
        write_bigquery(frames, args.gcp_project, args.bq_raw_dataset, args.bq_location)
        dest = f"{args.gcp_project}.{args.bq_raw_dataset}"

    print(f"Wrote raw data to {args.target}: {dest}")
    print(f"  window: {start.date()} .. {end.date()} ({args.days} days), seed={args.seed}")
    for name, n in rows.items():
        print(f"  {name if '.' in name else f'raw.{name}'}: {n:,} rows")


if __name__ == "__main__":
    main()
