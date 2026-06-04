"""Render trajectory map (Folium HTML + matplotlib PNG) for detected pairs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import cast

import folium
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WINDOW_MINUTES = 10


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("aisdk-visualize")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_window(
    spark: SparkSession,
    clean_dir: Path,
    mmsi_a: int,
    mmsi_b: int,
    t_center: pd.Timestamp,
) -> pd.DataFrame:
    t_start = t_center - pd.Timedelta(minutes=WINDOW_MINUTES)
    t_end = t_center + pd.Timedelta(minutes=WINDOW_MINUTES)
    df = (
        spark.read.parquet(str(clean_dir))
        .filter(F.col("mmsi").isin(mmsi_a, mmsi_b))
        .filter(F.col("ts").between(t_start.to_pydatetime(), t_end.to_pydatetime()))
        .orderBy("mmsi", "ts")
    )
    return df.select("mmsi", "ts", "lat", "lon", "sog", "cog", "name", "ship_type").toPandas()


def _vessel_label(group: pd.DataFrame, mmsi: int) -> str:
    name = group["name"].dropna().iloc[0] if not group["name"].dropna().empty else f"MMSI {mmsi}"
    return f"{name} ({mmsi})"


def _closest_to(group: pd.DataFrame, t_center: pd.Timestamp) -> pd.Series:
    ts_ns = group["ts"].to_numpy().astype("int64")
    pos = int(np.abs(ts_ns - t_center.value).argmin())
    return group.iloc[pos]


def _add_direction_arrows(
    ax, group: pd.DataFrame, color: str, arrow_len: float, n_arrows: int = 5
) -> None:
    n = len(group)
    if n < 2:
        return
    indices = np.linspace(0, n - 2, n_arrows).astype(int)
    for i in indices:
        x0, y0 = group["lon"].iloc[i], group["lat"].iloc[i]
        for j in range(i + 1, min(i + 10, n)):
            x1, y1 = group["lon"].iloc[j], group["lat"].iloc[j]
            dx, dy = x1 - x0, y1 - y0
            d = np.hypot(dx, dy)
            if d > 1e-7:
                ux = dx / d * arrow_len
                uy = dy / d * arrow_len
                ax.annotate(
                    "",
                    xy=(x0 + ux, y0 + uy), xytext=(x0, y0),
                    arrowprops=dict(
                        arrowstyle="->", color=color, alpha=0.85, lw=2,
                        mutation_scale=15,
                    ),
                )
                break


def render_folium(
    traj: pd.DataFrame,
    mmsi_a: int,
    mmsi_b: int,
    pair_ts: dict[int, pd.Timestamp],
    out_html: Path,
) -> None:
    center_lat = traj["lat"].mean()
    center_lon = traj["lon"].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=15, tiles="CartoDB positron")

    colors = {mmsi_a: "blue", mmsi_b: "red"}
    for mmsi_raw, group in traj.groupby("mmsi"):
        mmsi = cast(int, mmsi_raw)
        color = colors[mmsi]
        label = _vessel_label(group, mmsi)
        coords = list(zip(group["lat"], group["lon"]))
        folium.PolyLine(coords, color=color, weight=3, opacity=0.8, tooltip=label).add_to(m)
        for _, r in group.iterrows():
            folium.CircleMarker(
                location=[r["lat"], r["lon"]],
                radius=3, color=color, fill=True, fill_opacity=0.9,
                popup=f"{label}<br>{r['ts']}<br>SOG: {r['sog']:.1f} kn<br>COG: {r['cog']:.0f}°",
            ).add_to(m)
        nearest = _closest_to(group, pair_ts[mmsi])
        folium.Marker(
            location=[nearest["lat"], nearest["lon"]],
            tooltip=f"{label} @ closest approach",
            popup=f"<b>{label}</b><br>{nearest['ts']}<br>SOG: {nearest['sog']:.1f} kn",
            icon=folium.Icon(color=color, icon="ship", prefix="fa"),
        ).add_to(m)

    out_html.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_html))


def render_matplotlib(
    traj: pd.DataFrame,
    mmsi_a: int,
    mmsi_b: int,
    pair_ts: dict[int, pd.Timestamp],
    title: str,
    out_png: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = {mmsi_a: "tab:blue", mmsi_b: "tab:red"}

    lon_range = traj["lon"].max() - traj["lon"].min()
    lat_range = traj["lat"].max() - traj["lat"].min()
    arrow_len = max(lon_range, lat_range, 1e-4) * 0.04

    for mmsi_raw, group in traj.groupby("mmsi"):
        mmsi = cast(int, mmsi_raw)
        color = colors[mmsi]
        label = _vessel_label(group, mmsi)
        ax.plot(group["lon"], group["lat"], "-", color=color, alpha=0.6, linewidth=1.5)
        ax.scatter(group["lon"], group["lat"], color=color, s=15, alpha=0.7, label=label)
        _add_direction_arrows(ax, group, color, arrow_len)
        nearest = _closest_to(group, pair_ts[mmsi])
        ax.scatter(
            [nearest["lon"]], [nearest["lat"]],
            color=color, s=300, marker="*",
            edgecolor="black", linewidth=1.5, zorder=5,
        )

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(title)
    ax.legend(loc="best", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_aspect("equal", adjustable="datalim")
    ax.ticklabel_format(useOffset=False, style="plain")
    ax.tick_params(axis="x", rotation=20)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close(fig)


def render_one(
    spark: SparkSession,
    clean_dir: Path,
    pair: dict,
    out_html: Path,
    out_png: Path,
    out_result: Path,
) -> dict | None:
    mmsi_a = int(pair["mmsi_a"])
    mmsi_b = int(pair["mmsi_b"])
    t_a = pd.Timestamp(pair["ts_a"])
    t_b = pd.Timestamp(pair["ts_b"])
    pair_ts = {mmsi_a: t_a, mmsi_b: t_b}
    traj = extract_window(spark, Path(clean_dir), mmsi_a, mmsi_b, t_a)
    if traj.empty:
        return None

    title = (
        f"{pair['name_a']} ({pair['ship_type_a']}) <-> "
        f"{pair['name_b']} ({pair['ship_type_b']})  "
        f"-- closest approach {pair['dist_m']:.2f} m @ {pair['ts_a']}"
    )
    render_folium(traj, mmsi_a, mmsi_b, pair_ts, out_html)
    render_matplotlib(traj, mmsi_a, mmsi_b, pair_ts, title, out_png)

    result = {
        "mmsi_a": mmsi_a,
        "mmsi_b": mmsi_b,
        "name_a": pair["name_a"],
        "name_b": pair["name_b"],
        "ship_type_a": pair["ship_type_a"],
        "ship_type_b": pair["ship_type_b"],
        "timestamp_utc": pair["ts_a"],
        "lat_a": pair["lat_a"],
        "lon_a": pair["lon_a"],
        "lat_b": pair["lat_b"],
        "lon_b": pair["lon_b"],
        "distance_m": pair["dist_m"],
        "window_minutes": WINDOW_MINUTES,
    }
    out_result.parent.mkdir(parents=True, exist_ok=True)
    out_result.write_text(json.dumps(result, indent=2) + "\n")
    return result


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Visualize a detected collision pair.")
    p.add_argument("--in", dest="inp", type=Path,
                   default=Path("/app/data/processed/aisdk-2021-12-clean"))
    p.add_argument("--pairs", type=Path,
                   default=Path("/app/output/top_pairs_civilian.json"))
    p.add_argument("--rank", type=int, default=1, help="1-indexed pair to render")
    p.add_argument("--out-html", type=Path, default=Path("/app/output/collision_map.html"))
    p.add_argument("--out-png", type=Path, default=Path("/app/output/collision_map.png"))
    p.add_argument("--out-result", type=Path, default=Path("/app/output/result.json"))
    args = p.parse_args(argv)

    pairs = json.loads(args.pairs.read_text())
    if not pairs:
        print(f"No pairs in {args.pairs}", file=sys.stderr)
        return 1
    if args.rank < 1 or args.rank > len(pairs):
        print(f"--rank must be in [1, {len(pairs)}]", file=sys.stderr)
        return 1
    pair = pairs[args.rank - 1]

    spark = build_spark()
    try:
        result = render_one(
            spark, args.inp, pair,
            args.out_html, args.out_png, args.out_result,
        )
    finally:
        spark.stop()

    if result is None:
        print("No pings found in window", file=sys.stderr)
        return 1

    print("=" * 70)
    print("DETECTED COLLISION CANDIDATE")
    print("=" * 70)
    print(f"  Vessel A : {result['name_a']}  (MMSI {result['mmsi_a']}, {result['ship_type_a']})")
    print(f"  Vessel B : {result['name_b']}  (MMSI {result['mmsi_b']}, {result['ship_type_b']})")
    print(f"  Time     : {result['timestamp_utc']} UTC")
    print(f"  Position : A=({result['lat_a']:.6f}, {result['lon_a']:.6f})")
    print(f"             B=({result['lat_b']:.6f}, {result['lon_b']:.6f})")
    print(f"  Distance : {result['distance_m']:.2f} m")
    print("=" * 70)
    print(f"wrote: {args.out_html}")
    print(f"wrote: {args.out_png}")
    print(f"wrote: {args.out_result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
