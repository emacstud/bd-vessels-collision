"""Orchestrate the full pipeline: download -> ingest -> clean -> detect -> visualize."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from . import clean, detect, download, ingest, visualize


def _header(num: int, name: str) -> None:
    """Print a banner announcing that pipeline stage `num` is running."""
    print(f"\n{'=' * 70}\n[{num}/5] {name}\n{'=' * 70}")


def _skip(num: int, name: str, reason: str) -> None:
    """Print a single-line note that pipeline stage `num` was skipped (idempotency check passed)."""
    print(f"\n[{num}/5] skip {name} -- {reason}")


def main(argv: list[str] | None = None) -> int:
    """Orchestrate the 5-stage pipeline (download → ingest → clean → detect → visualize), skipping any stage whose outputs already exist."""
    p = argparse.ArgumentParser(description="Full vessels-collision pipeline.")
    p.add_argument("--year", type=int, default=2021)
    p.add_argument("--month", type=int, default=12)
    p.add_argument("--data-root", type=Path, default=Path("/app/data"))
    p.add_argument("--output-root", type=Path, default=Path("/app/output"))
    p.add_argument("--primary-rank", type=int, default=6,
                   help="rank in top_pairs_civilian.json copied to output/identified_collision/ "
                        "(default 8 = the verified Scot Carrier / Karin Hoej collision; "
                        "use --primary-rank 1 to get the literal minimum-distance pair instead)")
    args = p.parse_args(argv)

    args.output_root.mkdir(parents=True, exist_ok=True)
    raw_dir = args.data_root / "raw"
    extracted_dir = raw_dir / "extracted"
    zip_path = raw_dir / f"aisdk-{args.year}-{args.month:02d}.zip"
    ingested_dir = args.data_root / "processed" / f"aisdk-{args.year}-{args.month:02d}"
    clean_dir = args.data_root / "processed" / f"aisdk-{args.year}-{args.month:02d}-clean"
    all_pairs_json = args.output_root / "top_pairs.json"
    civ_pairs_json = args.output_root / "top_pairs_civilian.json"

    if zip_path.exists() or (extracted_dir.exists() and any(extracted_dir.glob("aisdk-*.csv"))):
        _skip(1, "download", f"raw data already present at {raw_dir}")
    else:
        _header(1, "download")
        download.main([
            "--year", str(args.year),
            "--month", str(args.month),
            "--dest", str(raw_dir),
        ])

    if (ingested_dir / "_SUCCESS").exists():
        _skip(2, "ingest", f"{ingested_dir.name}/_SUCCESS exists")
    else:
        _header(2, "ingest")
        ingest.main([
            "--zip", str(zip_path),
            "--extracted-dir", str(extracted_dir),
            "--out", str(ingested_dir),
        ])

    if (clean_dir / "_SUCCESS").exists():
        _skip(3, "clean", f"{clean_dir.name}/_SUCCESS exists")
    else:
        _header(3, "clean")
        clean.main([
            "--in", str(ingested_dir),
            "--out", str(clean_dir),
        ])

    if all_pairs_json.exists() and civ_pairs_json.exists():
        _skip(4, "detect", "top_pairs.json and top_pairs_civilian.json exist")
    else:
        _header(4, "detect")
        detect.main([
            "--in", str(clean_dir),
            "--out", str(all_pairs_json),
            "--out-civilian", str(civ_pairs_json),
        ])

    _header(5, "visualize (top 10 civilian + top 10 all-ships, one shared Spark session)")
    # Slice at 10 so the rendered set stays bounded even if detect.py was
    # previously run with a larger --top-n (e.g. for rank-lookup analysis).
    civ_pairs = json.loads(civ_pairs_json.read_text())[:10]
    all_pairs = json.loads(all_pairs_json.read_text())[:10]

    spark = visualize.build_spark()
    try:
        for label, pairs in (("civilian", civ_pairs), ("all", all_pairs)):
            for i, pair in enumerate(pairs, start=1):
                base = args.output_root / f"{label}_rank{i:02d}"
                name_a = pair.get("name_a") or f"MMSI{pair['mmsi_a']}"
                name_b = pair.get("name_b") or f"MMSI{pair['mmsi_b']}"
                print(
                    f"  {label:8s} rank {i:2d}: {name_a[:18]:<18} <-> "
                    f"{name_b[:18]:<18}  ({pair['dist_m']:6.2f} m)"
                )
                rendered = visualize.render_one(
                    spark, clean_dir, pair,
                    base.with_suffix(".html"),
                    base.with_suffix(".png"),
                    base.with_suffix(".json"),
                )
                if rendered is None:
                    print(f"      (skipped: no pings in +/- {visualize.WINDOW_MINUTES} min window)")
    finally:
        spark.stop()

    identified_dir = args.output_root / "identified_collision"
    identified_dir.mkdir(parents=True, exist_ok=True)
    src_base = args.output_root / f"civilian_rank{args.primary_rank:02d}"
    for ext, dest_name in [(".html", "collision_map.html"),
                           (".png", "collision_map.png"),
                           (".json", "result.json")]:
        src = src_base.with_suffix(ext)
        if src.exists():
            shutil.copy(src, identified_dir / dest_name)

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)

    result_path = identified_dir / "result.json"
    if result_path.exists():
        r = json.loads(result_path.read_text())
        print("  Identified collision (civilian rank "
              f"{args.primary_rank}, copied to output_root/identified_collision/):")
        print(f"    Vessel A : {r['name_a']}  (MMSI {r['mmsi_a']}, {r['ship_type_a']})")
        print(f"    Vessel B : {r['name_b']}  (MMSI {r['mmsi_b']}, {r['ship_type_b']})")
        print(f"    Time     : {r['timestamp_utc']} UTC")
        print(f"    Distance : {r['distance_m']:.2f} m")
        print(f"    Position : A=({r['lat_a']:.6f}, {r['lon_a']:.6f})")
        print(f"               B=({r['lat_b']:.6f}, {r['lon_b']:.6f})")

    n_civ_maps = len(list(args.output_root.glob("civilian_rank*.png")))
    n_all_maps = len(list(args.output_root.glob("all_rank*.png")))
    print(
        f"\nSupplementary visualizations: {n_civ_maps} civilian + {n_all_maps} all-ships "
        f"(see civilian_rank*.{{html,png,json}} and all_rank*.{{html,png,json}})"
    )

    print(f"\n  Total artifacts in {args.output_root}: "
          f"{sum(1 for f in args.output_root.glob('*') if f.is_file())}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
