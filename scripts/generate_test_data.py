"""Generate synthetic AppsFlyer + sports-betting raw data.

Writes four tables into a ``raw`` schema/dataset that the dbt project reads as sources:

    raw.appsflyer_events   mobile-attribution events (install, registration, login, first_deposit)
    raw.users              registered users
    raw.bets               settled sports bets
    raw.transactions       deposits and withdrawals

Locally the destination is a DuckDB file (``--target duckdb``, the default); for the BigQuery flow
(``--target bigquery``) the same frames are loaded into a BigQuery dataset using Application Default
Credentials. The generator is deterministic for a given ``--seed`` so re-runs reproduce identical data,
which lets the incremental dbt models be exercised for idempotency.

Examples
--------
    python scripts/generate_test_data.py                       # -> data/dev.duckdb
    python scripts/generate_test_data.py --users 5000 --days 120
    python scripts/generate_test_data.py --target bigquery --gcp-project my-proj --bq-raw-dataset raw
"""

from __future__ import annotations

import argparse
import os
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
COUNTRIES = {"GB": 0.40, "IE": 0.12, "DE": 0.18, "ES": 0.15, "BR": 0.15}
SPORTS = {"football": 0.50, "tennis": 0.15, "basketball": 0.15, "horse_racing": 0.12, "esports": 0.08}
CAMPAIGNS = ["brand_generic", "acq_prospecting", "retargeting", "lookalike_2pct", "seasonal_promo"]

HOUSE_MARGIN = 0.92  # actual win prob = implied prob * margin (the operator's edge)


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
    user_id = np.where(registered, np.array([f"usr-{i:08d}" for i in device_idx]), None)

    # active = registered users who go on to deposit + bet
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
                "user_id": (user_id[idx] if with_user else np.full(idx.size, None)),
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
                    "user_id": user_id[owner],
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
    reg_idx = np.where(registered)[0]
    users = pd.DataFrame(
        {
            "user_id": user_id[reg_idx],
            "appsflyer_id": appsflyer_id[reg_idx],
            "registration_time": _to_ts(reg_s[reg_idx]),
            "country": country[reg_idx],
            "acquisition_media_source": media_source[reg_idx],
            "platform": platform[reg_idx],
            "_loaded_at": loaded_at,
        }
    )

    # ---- bets (active users only) ------------------------------------------
    active_idx = np.where(active)[0]
    bet_counts = rng.poisson(14, active_idx.size) + 1
    owner = np.repeat(active_idx, bet_counts)
    n_bets = owner.size
    placed_s = _rand_times(rng, reg_s[owner].astype("int64") + 3600, end_s, n_bets)
    stake = np.round(np.clip(rng.gamma(2.0, 9.0, n_bets), 1.0, 1000.0), 2)
    odds = np.round(np.clip(1.1 + rng.exponential(1.3, n_bets), 1.05, 26.0), 2)
    implied = 1.0 / odds
    win = rng.random(n_bets) < implied * HOUSE_MARGIN
    roll = rng.random(n_bets)
    status = np.where(roll < 0.02, "void", np.where(roll < 0.07, "cashout", np.where(win, "won", "lost")))
    payout = np.zeros(n_bets)
    payout = np.where(status == "won", np.round(stake * odds, 2), payout)
    payout = np.where(status == "void", stake, payout)
    cashout_mask = status == "cashout"
    payout = np.where(
        cashout_mask, np.round(stake * rng.uniform(0.4, 1.0, n_bets) * odds * 0.6, 2), payout
    )
    settle_delay = rng.integers(300, 3 * 24 * 3600, n_bets)
    settled_s = np.minimum(placed_s + settle_delay, end_s)
    bets = pd.DataFrame(
        {
            "bet_id": [f"bet-{i:09d}" for i in range(n_bets)],
            "user_id": user_id[owner],
            "placed_at": _to_ts(placed_s),
            "settled_at": _to_ts(settled_s),
            "sport": _choice(rng, SPORTS, n_bets),
            "stake": stake,
            "decimal_odds": odds,
            "status": status,
            "payout": np.round(payout, 2),
            "_loaded_at": loaded_at,
        }
    )

    # ---- transactions (active users only) ----------------------------------
    dep_counts = rng.poisson(2, active_idx.size) + 1
    wd_counts = rng.poisson(0.8, active_idx.size)
    dep_owner = np.repeat(active_idx, dep_counts)
    wd_owner = np.repeat(active_idx, wd_counts)
    dep_s = _rand_times(rng, reg_s[dep_owner].astype("int64"), end_s, dep_owner.size)
    wd_s = _rand_times(rng, reg_s[wd_owner].astype("int64") + 3600, end_s, wd_owner.size)
    dep_amt = np.round(np.clip(rng.gamma(2.2, 22.0, dep_owner.size), 5.0, 2000.0), 2)
    wd_amt = np.round(np.clip(rng.gamma(2.0, 30.0, wd_owner.size), 5.0, 3000.0), 2)
    tx = pd.DataFrame(
        {
            "user_id": np.concatenate([user_id[dep_owner], user_id[wd_owner]]),
            "created_at": _to_ts(np.concatenate([dep_s, wd_s])),
            "transaction_type": (["deposit"] * dep_owner.size) + (["withdrawal"] * wd_owner.size),
            "amount": np.concatenate([dep_amt, wd_amt]),
        }
    )
    status_roll = rng.random(len(tx))
    tx["status"] = np.where(status_roll < 0.94, "completed", np.where(status_roll < 0.98, "pending", "failed"))
    tx = tx.sort_values("created_at").reset_index(drop=True)
    tx.insert(0, "transaction_id", [f"txn-{i:09d}" for i in range(len(tx))])
    tx["_loaded_at"] = loaded_at

    return {"appsflyer_events": events, "users": users, "bets": bets, "transactions": tx}


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
            con.register("df_tmp", df)
            con.execute(f"create or replace table raw.{name} as select * from df_tmp")
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
        table_id = f"{project}.{dataset}.{name}"
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--target", choices=["duckdb", "bigquery"], default="duckdb")
    p.add_argument("--users", type=int, default=2500, help="number of installs/devices to generate")
    p.add_argument("--days", type=int, default=90, help="history length ending at --end")
    p.add_argument("--end", default=None, help="ISO end date (default: now, UTC)")
    p.add_argument("--seed", type=int, default=42)
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

    frames = build_frames(args.users, start, end, args.seed)
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
        print(f"  raw.{name}: {n:,} rows")


if __name__ == "__main__":
    main()
