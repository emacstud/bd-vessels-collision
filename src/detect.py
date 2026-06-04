"""Detect the closest vessel encounters via a geohash-bucketed spatial-temporal self-join."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_NM = 3440.065
NM_PER_M = 1.0 / 1852.0

TIME_BUCKET_S = 60
LAT_CELL_DEG = 150.0 / 111_000.0
LON_CELL_DEG = 150.0 / 63_700.0
DISTANCE_THRESHOLD_M = 100.0
MIN_MOVING_SOG_KN = 1.0

OPERATIONAL_SHIP_TYPES = ["Pilot", "Tug", "SAR", "Law enforcement", "Dredging", "Towing", "Towing long/wide"]

TOP_N_DEFAULT = 10

FORWARD_OFFSETS = [(0, 0), (0, 1), (1, -1), (1, 0), (1, 1)]


def haversine_nm(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    lat1_r = F.radians(lat1)
    lat2_r = F.radians(lat2)
    dlat = lat2_r - lat1_r
    dlon = F.radians(lon2) - F.radians(lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_r) * F.cos(lat2_r) * F.sin(dlon / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_NM) * F.asin(F.sqrt(a))


def with_bucket_keys(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("time_bucket", (F.col("ts").cast("long") / TIME_BUCKET_S).cast("long"))
        .withColumn("lat_cell", F.floor(F.col("lat") / LAT_CELL_DEG).cast("int"))
        .withColumn("lon_cell", F.floor(F.col("lon") / LON_CELL_DEG).cast("int"))
    )


def expand_neighbors(df: DataFrame, spark: SparkSession) -> DataFrame:
    offsets_df = spark.createDataFrame(FORWARD_OFFSETS, ["dx", "dy"])
    return (
        df.crossJoin(F.broadcast(offsets_df))
        .withColumn("cell_lat_v", F.col("lat_cell") + F.col("dx"))
        .withColumn("cell_lon_v", F.col("lon_cell") + F.col("dy"))
        .drop("dx", "dy")
    )


def detect_pairs(spark: SparkSession, df: DataFrame) -> DataFrame:
    df = with_bucket_keys(df).filter(F.col("sog") > MIN_MOVING_SOG_KN)
    right = expand_neighbors(df, spark).alias("b")
    left = df.alias("a")

    threshold_nm = DISTANCE_THRESHOLD_M * NM_PER_M

    same_cell = (F.col("a.lat_cell") == F.col("b.lat_cell")) & (
        F.col("a.lon_cell") == F.col("b.lon_cell")
    )

    pairs = (
        left.join(
            right,
            (F.col("a.time_bucket") == F.col("b.time_bucket"))
            & (F.col("a.lat_cell") == F.col("b.cell_lat_v"))
            & (F.col("a.lon_cell") == F.col("b.cell_lon_v"))
            & (F.col("a.mmsi") != F.col("b.mmsi"))
            & (
                (same_cell & (F.col("a.mmsi") < F.col("b.mmsi")))
                | ~same_cell
            ),
        )
        .withColumn(
            "dist_nm",
            haversine_nm(F.col("a.lat"), F.col("a.lon"), F.col("b.lat"), F.col("b.lon")),
        )
        .filter(F.col("dist_nm") < threshold_nm)
    )

    a_lower = F.col("a.mmsi") < F.col("b.mmsi")

    def pick(col_a: str, col_b: str) -> Column:
        return F.when(a_lower, F.col(col_a)).otherwise(F.col(col_b))

    return pairs.select(
        pick("a.mmsi", "b.mmsi").alias("mmsi_a"),
        pick("b.mmsi", "a.mmsi").alias("mmsi_b"),
        pick("a.ts", "b.ts").alias("ts_a"),
        pick("b.ts", "a.ts").alias("ts_b"),
        pick("a.lat", "b.lat").alias("lat_a"),
        pick("b.lat", "a.lat").alias("lat_b"),
        pick("a.lon", "b.lon").alias("lon_a"),
        pick("b.lon", "a.lon").alias("lon_b"),
        pick("a.sog", "b.sog").alias("sog_a"),
        pick("b.sog", "a.sog").alias("sog_b"),
        F.coalesce(
            pick("a.name", "b.name"),
            F.format_string("MMSI %d", pick("a.mmsi", "b.mmsi")),
        ).alias("name_a"),
        F.coalesce(
            pick("b.name", "a.name"),
            F.format_string("MMSI %d", pick("b.mmsi", "a.mmsi")),
        ).alias("name_b"),
        pick("a.ship_type", "b.ship_type").alias("ship_type_a"),
        pick("b.ship_type", "a.ship_type").alias("ship_type_b"),
        (F.col("dist_nm") / NM_PER_M).alias("dist_m"),
    )


def top_n_distinct(pairs: DataFrame, n: int) -> DataFrame:
    w = Window.partitionBy("mmsi_a", "mmsi_b").orderBy("dist_m")
    return (
        pairs.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .orderBy("dist_m")
        .limit(n)
    )


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("aisdk-detect")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _print_table(title: str, rows) -> None:
    print(f"\n{title}")
    print("-" * 110)
    for _, r in rows.iterrows():
        print(
            f"  {int(r['mmsi_a']):>9} {str(r['name_a'])[:18]:<18} "
            f"({str(r['ship_type_a'])[:10]:<10}) "
            f"<-> {int(r['mmsi_b']):>9} {str(r['name_b'])[:18]:<18} "
            f"({str(r['ship_type_b'])[:10]:<10})  "
            f"dist={r['dist_m']:6.1f} m  "
            f"sog=({r['sog_a']:4.1f}/{r['sog_b']:4.1f}) kn  "
            f"@ {r['ts_a']}"
        )
    print("-" * 110)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Detect top-N closest vessel pairs.")
    p.add_argument("--in", dest="inp", type=Path,
                   default=Path("/app/data/processed/aisdk-2021-12-clean"))
    p.add_argument("--out", type=Path,
                   default=Path("/app/output/top_pairs.json"))
    p.add_argument("--out-civilian", type=Path,
                   default=Path("/app/output/top_pairs_civilian.json"))
    p.add_argument("--top-n", type=int, default=TOP_N_DEFAULT)
    args = p.parse_args(argv)

    args.out.parent.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    try:
        clean = spark.read.parquet(str(args.inp))
        pairs = detect_pairs(spark, clean).cache()

        top_all = top_n_distinct(pairs, args.top_n)
        civilian_pairs = pairs.filter(
            ~F.col("ship_type_a").isin(*OPERATIONAL_SHIP_TYPES)
            & ~F.col("ship_type_b").isin(*OPERATIONAL_SHIP_TYPES)
        )
        top_civilian = top_n_distinct(civilian_pairs, args.top_n)

        rows_all = top_all.toPandas()
        rows_civilian = top_civilian.toPandas()
        pairs.unpersist()
    finally:
        spark.stop()

    _print_table(f"Top {args.top_n} closest pairs (all ship types):", rows_all)
    _print_table(
        f"Top {args.top_n} closest pairs (civilian only, excluding "
        + ", ".join(OPERATIONAL_SHIP_TYPES) + "):",
        rows_civilian,
    )

    rows_all.to_json(args.out, orient="records", date_format="iso", indent=2)
    rows_civilian.to_json(args.out_civilian, orient="records", date_format="iso", indent=2)
    print(f"\nwrote: {args.out}")
    print(f"wrote: {args.out_civilian}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
