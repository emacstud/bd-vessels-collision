"""Filter raw AIS Parquet to moving, ITU-valid vessels inside the 50 nm Bornholm circle."""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CENTER_LAT = 55.225
CENTER_LON = 14.245
RADIUS_NM = 50.0
EARTH_RADIUS_NM = 3440.065

BBOX_LAT_MIN, BBOX_LAT_MAX = 54.3, 56.15
BBOX_LON_MIN, BBOX_LON_MAX = 12.7, 15.8

MIN_VESSEL_MAX_SOG_KN = 1.0
MAX_PLAUSIBLE_SPEED_KN = 50.0
MIN_PINGS_PER_MMSI = 20
SOG_AIS_MAX_VALID = 102.2

BAD_MMSI = [123456789, 987654321]
ALL_SAME_DIGIT_MMSI = [int(str(d) * 9) for d in range(1, 10)]

MID_FILE = Path(__file__).resolve().parent / "resources" / "MID.csv"


def _load_mid_codes() -> list[int]:
    with open(MID_FILE, newline="", encoding="cp1252") as f:
        return [int(row["Digit"].strip()) for row in csv.DictReader(f)]


MID_CODES = _load_mid_codes()


def haversine_nm(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    lat1_r = F.radians(lat1)
    lat2_r = F.radians(lat2)
    dlat = lat2_r - lat1_r
    dlon = F.radians(lon2) - F.radians(lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_r) * F.cos(lat2_r) * F.sin(dlon / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_NM) * F.asin(F.sqrt(a))


def spatial_filter(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("lat").between(BBOX_LAT_MIN, BBOX_LAT_MAX))
        .filter(F.col("lon").between(BBOX_LON_MIN, BBOX_LON_MAX))
        .withColumn(
            "dist_center_nm",
            haversine_nm(F.lit(CENTER_LAT), F.lit(CENTER_LON), F.col("lat"), F.col("lon")),
        )
        .filter(F.col("dist_center_nm") <= RADIUS_NM)
        .drop("dist_center_nm")
    )


def normalize_sog(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "sog",
        F.when(F.col("sog") < 0, F.lit(0.0))
        .when(F.col("sog") >= SOG_AIS_MAX_VALID, F.lit(0.0))
        .otherwise(F.col("sog")),
    )


def mmsi_filter(df: DataFrame) -> DataFrame:
    df = (
        df
        .filter(F.col("mmsi").isNotNull())
        .filter(F.col("mmsi").between(200_000_000, 799_999_999))
        .filter(~F.col("mmsi").isin(*BAD_MMSI))
        .filter(~F.col("mmsi").isin(*ALL_SAME_DIGIT_MMSI))
        .filter((F.col("mmsi") / 1_000_000).cast("int").isin(*MID_CODES))
    )
    moving = (
        df.groupBy("mmsi")
        .agg(F.max("sog").alias("max_sog"))
        .filter(F.col("max_sog") > MIN_VESSEL_MAX_SOG_KN)
        .select("mmsi")
    )
    return df.join(F.broadcast(moving), "mmsi", "inner")


def gps_jump_filter(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("mmsi").orderBy("ts")
    df = (
        df
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_ts", F.lag("ts").over(w))
        .withColumn("next_lat", F.lead("lat").over(w))
        .withColumn("next_lon", F.lead("lon").over(w))
        .withColumn("next_ts", F.lead("ts").over(w))
    )
    df = (
        df
        .withColumn(
            "speed_prev_kn",
            haversine_nm(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon"))
            * 3600 / (F.col("ts").cast("long") - F.col("prev_ts").cast("long")),
        )
        .withColumn(
            "speed_next_kn",
            haversine_nm(F.col("lat"), F.col("lon"), F.col("next_lat"), F.col("next_lon"))
            * 3600 / (F.col("next_ts").cast("long") - F.col("ts").cast("long")),
        )
    )
    df = df.filter(
        ~(
            (F.col("speed_prev_kn") > MAX_PLAUSIBLE_SPEED_KN)
            & (F.col("speed_next_kn") > MAX_PLAUSIBLE_SPEED_KN)
        )
    )
    return df.drop(
        "prev_lat", "prev_lon", "prev_ts",
        "next_lat", "next_lon", "next_ts",
        "speed_prev_kn", "speed_next_kn",
    )


def fill_vessel_names(df: DataFrame) -> DataFrame:
    """Propagate any known vessel name and ship_type across all pings of the same MMSI.

    AIS Class A broadcasts position reports (Type 1/2/3) and static data (Type 5)
    as separate messages; in the source CSV the static fields are null whenever
    a position ping arrives before the next Type 5 broadcast for that MMSI.
    Here we backfill those nulls from the best record we have for that same
    vessel anywhere in the cleaned data.

    For ship_type we treat the AIS sentinel "Undefined" (numeric code 0,
    "not available") as missing for the purpose of picking the canonical
    value, and we pick the MOST FREQUENTLY broadcast non-Undefined type per
    MMSI -- some vessels (e.g. tugs switching between "Tug" and "Towing
    long/wide" depending on what they are doing) emit more than one. Tiebreak
    by ship_type lex order so the choice is deterministic across runs.
    The literal "Undefined" is only kept if no specific type was ever broadcast.
    """
    name_lookup = (
        df.groupBy("mmsi")
        .agg(F.first(F.col("name"), ignorenulls=True).alias("_vessel_name"))
    )

    specific_type_counts = (
        df.filter(
            F.col("ship_type").isNotNull() & (F.col("ship_type") != "Undefined")
        )
        .groupBy("mmsi", "ship_type")
        .count()
    )
    rank_by_freq = Window.partitionBy("mmsi").orderBy(
        F.col("count").desc(), F.col("ship_type")
    )
    type_lookup = (
        specific_type_counts.withColumn("_rn", F.row_number().over(rank_by_freq))
        .filter(F.col("_rn") == 1)
        .select("mmsi", F.col("ship_type").alias("_vessel_ship_type"))
    )

    return (
        df.join(F.broadcast(name_lookup), "mmsi", "left")
        .join(F.broadcast(type_lookup), "mmsi", "left")
        .withColumn("name", F.coalesce(F.col("name"), F.col("_vessel_name")))
        .withColumn(
            "ship_type",
            F.coalesce(
                F.when(F.col("ship_type") != "Undefined", F.col("ship_type")),
                F.col("_vessel_ship_type"),
                F.col("ship_type"),
            ),
        )
        .drop("_vessel_name", "_vessel_ship_type")
    )


def sparsity_filter(df: DataFrame) -> DataFrame:
    keep = (
        df.groupBy("mmsi")
        .count()
        .filter(F.col("count") >= MIN_PINGS_PER_MMSI)
        .select("mmsi")
    )
    return df.join(F.broadcast(keep), "mmsi", "inner")


def _print_stage(label: str, n: int, prev_n: int | None) -> None:
    if prev_n is None:
        print(f"  {label:25s}: {n:>15,}")
    else:
        dropped = prev_n - n
        pct = 100.0 * dropped / prev_n if prev_n else 0.0
        print(f"  {label:25s}: {n:>15,}  (-{dropped:>12,}  {pct:5.2f}%)")


def clean(df: DataFrame) -> DataFrame:
    n_prev = df.count()
    _print_stage("input (raw)", n_prev, None)
    prev_cached: DataFrame | None = None

    def stage(new_df: DataFrame, label: str) -> DataFrame:
        nonlocal n_prev, prev_cached
        new_df = new_df.cache()
        n = new_df.count()
        _print_stage(label, n, n_prev)
        if prev_cached is not None:
            prev_cached.unpersist()
        prev_cached = new_df
        n_prev = n
        return new_df

    df = stage(spatial_filter(df), "after spatial filter")
    df = normalize_sog(df)
    df = stage(mmsi_filter(df), "after MMSI filter")
    df = stage(gps_jump_filter(df), "after GPS-jump filter")
    df = stage(sparsity_filter(df), "after sparsity filter")
    df = fill_vessel_names(df)
    return df


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("aisdk-clean")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def summarize(spark: SparkSession, out_dir: Path) -> None:
    df = spark.read.parquet(str(out_dir))
    rows = df.count()
    mmsis = df.select("mmsi").distinct().count()
    print(f"clean rows    : {rows:,}")
    print(f"distinct MMSI : {mmsis:,}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Clean AIS Parquet (spatial + motion + GPS-jump).")
    p.add_argument("--in", dest="inp", type=Path,
                   default=Path("/app/data/processed/aisdk-2021-12"))
    p.add_argument("--out", type=Path,
                   default=Path("/app/data/processed/aisdk-2021-12-clean"))
    p.add_argument("--force", action="store_true", help="re-run even if _SUCCESS exists")
    args = p.parse_args(argv)

    success = args.out / "_SUCCESS"
    if success.exists() and not args.force:
        print(f"already cleaned: {args.out}  (use --force to redo)")
        return 0

    spark = build_spark()
    try:
        raw = spark.read.parquet(str(args.inp))
        cleaned = clean(raw)
        cleaned.write.mode("overwrite").partitionBy("day").parquet(str(args.out))
        summarize(spark, args.out)
    finally:
        spark.stop()

    print(f"wrote Parquet: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
