"""Extract the AIS monthly ZIP and write day-partitioned Parquet."""
from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


RAW_SCHEMA = StructType([
    StructField("# Timestamp", StringType()),
    StructField("Type of mobile", StringType()),
    StructField("MMSI", LongType()),
    StructField("Latitude", DoubleType()),
    StructField("Longitude", DoubleType()),
    StructField("Navigational status", StringType()),
    StructField("ROT", DoubleType()),
    StructField("SOG", DoubleType()),
    StructField("COG", DoubleType()),
    StructField("Heading", DoubleType()),
    StructField("IMO", StringType()),
    StructField("Callsign", StringType()),
    StructField("Name", StringType()),
    StructField("Ship type", StringType()),
    StructField("Cargo type", StringType()),
    StructField("Width", DoubleType()),
    StructField("Length", DoubleType()),
    StructField("Type of position fixing device", StringType()),
    StructField("Draught", DoubleType()),
    StructField("Destination", StringType()),
    StructField("ETA", StringType()),
    StructField("Data source type", StringType()),
    StructField("A", DoubleType()),
    StructField("B", DoubleType()),
    StructField("C", DoubleType()),
    StructField("D", DoubleType()),
])


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """Extract the AIS monthly ZIP's daily CSVs into `dest_dir`, skipping already-extracted ones."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    if not zip_path.exists():
        existing = sorted(dest_dir.glob("aisdk-*.csv"))
        if not existing:
            raise FileNotFoundError(
                f"zip not found and no extracted CSVs in {dest_dir}: {zip_path}"
            )
        print(f"zip absent, using {len(existing)} extracted CSVs from {dest_dir}")
        return existing

    with zipfile.ZipFile(zip_path) as zf:
        names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
        out: list[Path] = []
        for name in names:
            target = dest_dir / Path(name).name
            expected = zf.getinfo(name).file_size
            if target.exists() and target.stat().st_size == expected:
                print(f"cached {target.name}")
                out.append(target)
                continue
            print(f"extract {target.name} ({expected / 1e9:.2f} GB)")
            with zf.open(name) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst, length=1 << 20)
            out.append(target)
    return out


def build_spark() -> SparkSession:
    """Build a local-mode SparkSession tuned for the single-machine ingest job."""
    spark = (
        SparkSession.builder
        .appName("aisdk-ingest")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform(df: DataFrame) -> DataFrame:
    """Parse the raw 26-column AIS schema down to the 13 columns the rest of the pipeline uses."""
    return (
        df.select(
            F.to_timestamp(F.col("`# Timestamp`"), "dd/MM/yyyy HH:mm:ss").alias("ts"),
            F.col("MMSI").alias("mmsi"),
            F.col("Latitude").alias("lat"),
            F.col("Longitude").alias("lon"),
            F.col("SOG").alias("sog"),
            F.col("COG").alias("cog"),
            F.col("Navigational status").alias("nav_status"),
            F.col("Type of mobile").alias("mobile_type"),
            F.col("Name").alias("name"),
            F.col("Ship type").alias("ship_type"),
            F.col("IMO").alias("imo"),
            F.col("Callsign").alias("callsign"),
        )
        .withColumn("day", F.to_date("ts"))
    )


def ingest(spark: SparkSession, csv_glob: str, out_dir: Path) -> None:
    """Read all matching CSVs, apply the schema, and write a day-partitioned Parquet dataset."""
    raw = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(RAW_SCHEMA)
        .csv(csv_glob)
    )
    (
        transform(raw)
        .write
        .mode("overwrite")
        .partitionBy("day")
        .parquet(str(out_dir))
    )


def summarize(spark: SparkSession, out_dir: Path) -> None:
    """Print row count and day-partition count of the written Parquet for verification."""
    df = spark.read.parquet(str(out_dir))
    rows = df.count()
    partitions = sorted(
        p.name for p in out_dir.iterdir() if p.is_dir() and p.name.startswith("day=")
    )
    print(f"rows written   : {rows:,}")
    print(f"day partitions : {len(partitions)} ({partitions[0]} .. {partitions[-1]})")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint: extract the ZIP and run the ingest job, skipping if a _SUCCESS marker exists."""
    p = argparse.ArgumentParser(description="Ingest AIS ZIP into day-partitioned Parquet.")
    p.add_argument("--zip", type=Path, default=Path("/app/data/raw/aisdk-2021-12.zip"))
    p.add_argument("--extracted-dir", type=Path, default=Path("/app/data/raw/extracted"))
    p.add_argument("--out", type=Path, default=Path("/app/data/processed/aisdk-2021-12"))
    p.add_argument("--force", action="store_true", help="re-run even if _SUCCESS exists")
    args = p.parse_args(argv)

    success = args.out / "_SUCCESS"
    if success.exists() and not args.force:
        print(f"already ingested: {args.out}  (use --force to redo)")
        return 0

    csvs = extract_zip(args.zip, args.extracted_dir)
    print(f"extracted {len(csvs)} CSVs into {args.extracted_dir}")

    spark = build_spark()
    try:
        ingest(spark, f"{args.extracted_dir}/aisdk-2021-12-*.csv", args.out)
        summarize(spark, args.out)
    finally:
        spark.stop()

    print(f"wrote Parquet: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
