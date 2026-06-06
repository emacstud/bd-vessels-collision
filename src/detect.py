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

SILENCE_WINDOW_S = 60
SILENCE_OBSERVATION_BUFFER_S = 60 * 60

PORT_CELL_SIZE_M = 500.0
PORT_LAT_CELL_DEG = PORT_CELL_SIZE_M / 111_000.0
PORT_LON_CELL_DEG = PORT_CELL_SIZE_M / 63_700.0
PORT_SILENCE_THRESHOLD = 2

OPERATIONAL_SHIP_TYPES = ["Pilot", "Tug", "SAR", "Law enforcement", "Dredging", "Towing", "Towing long/wide"]

TOP_N_DEFAULT = 10

FORWARD_OFFSETS = [(0, 0), (0, 1), (1, -1), (1, 0), (1, 1)]


def haversine_nm(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Vectorised Spark SQL Haversine distance in nautical miles between two lat/lon pairs."""
    lat1_r = F.radians(lat1)
    lat2_r = F.radians(lat2)
    dlat = lat2_r - lat1_r
    dlon = F.radians(lon2) - F.radians(lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_r) * F.cos(lat2_r) * F.sin(dlon / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_NM) * F.asin(F.sqrt(a))


def with_bucket_keys(df: DataFrame) -> DataFrame:
    """Add (time_bucket, lat_cell, lon_cell) integer keys used as the self-join hash key."""
    return (
        df
        .withColumn("time_bucket", (F.col("ts").cast("long") / TIME_BUCKET_S).cast("long"))
        .withColumn("lat_cell", F.floor(F.col("lat") / LAT_CELL_DEG).cast("int"))
        .withColumn("lon_cell", F.floor(F.col("lon") / LON_CELL_DEG).cast("int"))
    )


def expand_neighbors(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Emit each ping into 5 variant cells (self + 4 forward neighbours) so adjacent-cell pairs can join."""
    offsets_df = spark.createDataFrame(FORWARD_OFFSETS, ["dx", "dy"])
    return (
        df.crossJoin(F.broadcast(offsets_df))
        .withColumn("cell_lat_v", F.col("lat_cell") + F.col("dx"))
        .withColumn("cell_lon_v", F.col("lon_cell") + F.col("dy"))
        .drop("dx", "dy")
    )


def detect_pairs(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Spatial-temporal self-join: emit candidate pairs of moving pings within 100 m of each other."""
    clean_df = df  # keep the original (unfiltered) clean dataset for the silence enrichment step
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
        """Canonicalise A/B columns so the vessel with the smaller MMSI is always 'a'."""
        return F.when(a_lower, F.col(col_a)).otherwise(F.col(col_b))

    pairs_canonical = pairs.select(
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

    return enrich_with_silence(pairs_canonical, clean_df)


def enrich_with_silence(pairs: DataFrame, clean_df: DataFrame) -> DataFrame:
    """Add silenced_a, silenced_b boolean columns indicating each vessel went silent
    after the close approach IN OPEN WATER (not in a harbour / AIS-coverage gap).
    """
    last_pings = clean_df.groupBy("mmsi").agg(F.max("ts").alias("last_ts"))
    end_row = clean_df.agg(F.max("ts").alias("end")).first()
    if end_row is None or end_row["end"] is None:
        raise ValueError("clean_df has no rows -- cannot determine dataset end timestamp")
    dataset_end_ts = end_row["end"]
    dataset_end_s = int(dataset_end_ts.timestamp())

    w_last = Window.partitionBy("mmsi").orderBy(F.col("ts").desc())
    last_with_cell = (
        clean_df
        .withColumn("_rn", F.row_number().over(w_last))
        .filter(F.col("_rn") == 1)
        .select(
            "mmsi",
            F.floor(F.col("lat") / F.lit(PORT_LAT_CELL_DEG)).cast("int").alias("_p_lat"),
            F.floor(F.col("lon") / F.lit(PORT_LON_CELL_DEG)).cast("int").alias("_p_lon"),
        )
    )

    port_cells = (
        last_with_cell
        .groupBy("_p_lat", "_p_lon")
        .agg(F.countDistinct("mmsi").alias("_n"))
        .filter(F.col("_n") >= PORT_SILENCE_THRESHOLD)
        .select("_p_lat", "_p_lon")
    )

    mmsi_in_port = (
        last_with_cell
        .join(F.broadcast(port_cells), ["_p_lat", "_p_lon"])
        .select(F.col("mmsi").alias("_port_mmsi"))
    )

    last_a = (
        last_pings.withColumnRenamed("mmsi", "_mmsi_a_lkp")
        .withColumnRenamed("last_ts", "last_ts_a")
    )
    last_b = (
        last_pings.withColumnRenamed("mmsi", "_mmsi_b_lkp")
        .withColumnRenamed("last_ts", "last_ts_b")
    )
    port_a = mmsi_in_port.withColumnRenamed("_port_mmsi", "_port_a_lkp")
    port_b = mmsi_in_port.withColumnRenamed("_port_mmsi", "_port_b_lkp")

    enriched = (
        pairs
        .join(F.broadcast(last_a), F.col("mmsi_a") == F.col("_mmsi_a_lkp"), "left")
        .drop("_mmsi_a_lkp")
        .join(F.broadcast(last_b), F.col("mmsi_b") == F.col("_mmsi_b_lkp"), "left")
        .drop("_mmsi_b_lkp")
        .join(F.broadcast(port_a), F.col("mmsi_a") == F.col("_port_a_lkp"), "left")
        .withColumn("_in_port_a", F.col("_port_a_lkp").isNotNull())
        .drop("_port_a_lkp")
        .join(F.broadcast(port_b), F.col("mmsi_b") == F.col("_port_b_lkp"), "left")
        .withColumn("_in_port_b", F.col("_port_b_lkp").isNotNull())
        .drop("_port_b_lkp")
    )

    ts_a_long = F.col("ts_a").cast("long")
    ts_b_long = F.col("ts_b").cast("long")
    has_room_a = ts_a_long + SILENCE_OBSERVATION_BUFFER_S <= F.lit(dataset_end_s)
    has_room_b = ts_b_long + SILENCE_OBSERVATION_BUFFER_S <= F.lit(dataset_end_s)

    return (
        enriched
        .withColumn(
            "silenced_a",
            (F.col("last_ts_a").cast("long") <= ts_a_long + SILENCE_WINDOW_S)
            & has_room_a
            & ~F.col("_in_port_a"),
        )
        .withColumn(
            "silenced_b",
            (F.col("last_ts_b").cast("long") <= ts_b_long + SILENCE_WINDOW_S)
            & has_room_b
            & ~F.col("_in_port_b"),
        )
        .drop("last_ts_a", "last_ts_b", "_in_port_a", "_in_port_b")
    )


def top_n_distinct(pairs: DataFrame, n: int) -> DataFrame:
    """Keep the closest ping pair per (mmsi_a, mmsi_b) and return the global top-N rows ordered by distance."""
    w = Window.partitionBy("mmsi_a", "mmsi_b").orderBy("dist_m")
    return (
        pairs.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .orderBy("dist_m")
        .limit(n)
    )


def build_spark() -> SparkSession:
    """Build a local-mode SparkSession tuned for the single-machine detect join."""
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
    """Render a top-N pair table to stdout as a fixed-width text block."""
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
    """CLI entrypoint: run the self-join, emit three JSON files (all-ships, civilian-only, silenced top-N)."""
    p = argparse.ArgumentParser(description="Detect top-N closest vessel pairs.")
    p.add_argument("--in", dest="inp", type=Path,
                   default=Path("/app/data/processed/aisdk-2021-12-clean"))
    p.add_argument("--out", type=Path,
                   default=Path("/app/output/top_pairs.json"))
    p.add_argument("--out-civilian", type=Path,
                   default=Path("/app/output/top_pairs_civilian.json"))
    p.add_argument("--out-silenced", type=Path,
                   default=Path("/app/output/top_pairs_silenced.json"))
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
        silenced_pairs = pairs.filter(F.col("silenced_a") | F.col("silenced_b"))
        top_silenced = top_n_distinct(silenced_pairs, args.top_n)

        rows_all = top_all.toPandas()
        rows_civilian = top_civilian.toPandas()
        rows_silenced = top_silenced.toPandas()
        pairs.unpersist()
    finally:
        spark.stop()

    _print_table(f"Top {args.top_n} closest pairs (all ship types):", rows_all)
    _print_table(
        f"Top {args.top_n} closest pairs (civilian only, excluding "
        + ", ".join(OPERATIONAL_SHIP_TYPES) + "):",
        rows_civilian,
    )
    _print_table(
        f"Top {args.top_n} closest pairs, where at least one of the vessels became silent:",
        rows_silenced,
    )

    rows_all.to_json(args.out, orient="records", date_format="iso", indent=2)
    rows_civilian.to_json(args.out_civilian, orient="records", date_format="iso", indent=2)
    rows_silenced.to_json(args.out_silenced, orient="records", date_format="iso", indent=2)
    print(f"\nwrote: {args.out}")
    print(f"wrote: {args.out_civilian}")
    print(f"wrote: {args.out_silenced}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
