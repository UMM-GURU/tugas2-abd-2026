import argparse
import os
from pathlib import Path

import polars as pl
from huggingface_hub import snapshot_download

DEFAULT_HF_REPO = os.getenv("HF_DATASET_REPO", "3cdiqolbu/chicago-crime-clean")

OUTPUT_COLUMNS = [
    "id",
    "datetime",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "district",
    "beat",
    "ward",
    "community_area",
    "latitude",
    "longitude",
]


def _to_bool_expr(col_name: str, dtype: pl.DataType) -> pl.Expr:
    if dtype == pl.Boolean:
        return pl.col(col_name)
    return pl.col(col_name).cast(pl.String).str.to_lowercase().eq("true")


def _to_int16_expr(col_name: str) -> pl.Expr:
    return pl.col(col_name).cast(pl.Float64, strict=False).cast(pl.Int16, strict=False)


def build_clean_lazyframe(input_csv: Path) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    lf = pl.scan_csv(
        input_csv,
        try_parse_dates=False,
        ignore_errors=True,
        infer_schema_length=10000,
    )
    rename_map = {
        col: col.strip().lower().replace(" ", "_")
        for col in lf.collect_schema().names()
    }
    lf_std = lf.rename(rename_map)
    schema = lf_std.collect_schema()

    lf_clean = (
        lf_std.with_columns(
            pl.col("date")
            .str.to_datetime("%m/%d/%Y %I:%M:%S %p", strict=False)
            .alias("datetime"),
            pl.col("latitude").cast(pl.Float64, strict=False),
            pl.col("longitude").cast(pl.Float64, strict=False),
        )
        .drop_nulls(subset=["datetime", "latitude", "longitude", "primary_type"])
        .filter(
            pl.col("latitude").is_between(41.60, 42.05)
            & pl.col("longitude").is_between(-87.95, -87.50)
            & (pl.col("datetime").dt.year() >= 2001)
            & (pl.col("datetime").dt.year() <= 2026)
        )
        .unique(subset=["id"], keep="first")
        .with_columns(
            pl.col("id").cast(pl.Int64, strict=False),
            pl.col("primary_type").cast(pl.String),
            pl.col("description").cast(pl.String),
            pl.col("location_description").cast(pl.String),
            _to_bool_expr("arrest", schema["arrest"]).alias("arrest"),
            _to_bool_expr("domestic", schema["domestic"]).alias("domestic"),
            _to_int16_expr("district").alias("district"),
            _to_int16_expr("beat").alias("beat"),
            _to_int16_expr("ward").alias("ward"),
            _to_int16_expr("community_area").alias("community_area"),
            pl.col("latitude").cast(pl.Float32),
            pl.col("longitude").cast(pl.Float32),
            pl.col("datetime").dt.year().cast(pl.Int16).alias("_year"),
        )
        .select([*OUTPUT_COLUMNS, "_year"])
    )
    return lf_std, lf_clean


def run_etl(
    input_csv: Path,
    output_dir: Path,
    only_year: int | None = None,
) -> None:
    if not input_csv.exists():
        raise FileNotFoundError(f"File CSV tidak ditemukan: {input_csv}")

    output_dir.mkdir(parents=True, exist_ok=True)
    lf_std, lf_clean = build_clean_lazyframe(input_csv)

    audit_raw = lf_std.select(
        pl.len().alias("raw_rows"),
        pl.col("latitude").null_count().alias("null_coords"),
    ).collect()
    raw_rows = audit_raw["raw_rows"][0]
    null_coords = audit_raw["null_coords"][0]

    temp_parquet = output_dir.parent / "_tmp_chicago_clean.parquet"
    try:
        lf_clean.sink_parquet(temp_parquet, compression="zstd")
        lf_tmp = pl.scan_parquet(temp_parquet)

        clean_rows = lf_tmp.select(pl.len()).collect().item()
        print(f"Total baris mentah   : {raw_rows:,}")
        print(f"Baris tanpa koordinat: {null_coords:,}")
        print(f"Total baris bersih   : {clean_rows:,}")

        if only_year is not None:
            years = [only_year]
        else:
            years = (
                lf_tmp.select("_year")
                .unique()
                .sort("_year")
                .collect()["_year"]
                .to_list()
            )

        for yr in years:
            part_dir = output_dir / f"year={yr}"
            part_dir.mkdir(parents=True, exist_ok=True)
            out_file = part_dir / "data.parquet"
            (
                lf_tmp.filter(pl.col("_year") == yr)
                .drop("_year")
                .sink_parquet(out_file, compression="zstd")
            )
            part_rows = pl.scan_parquet(out_file).select(pl.len()).collect().item()
            print(f"Tersimpan {out_file} ({part_rows:,} baris)")

        sample_file = output_dir / f"year={years[-1]}" / "data.parquet"
        print("Skema output:", dict(pl.scan_parquet(sample_file).collect_schema()))
    finally:
        if temp_parquet.exists():
            temp_parquet.unlink()


def ensure_dataset(
    output_dir: Path = Path("data/processed/chicago_crimes"),
    repo_id: str = DEFAULT_HF_REPO,
) -> Path:
    existing = list(output_dir.glob("year=*/data.parquet"))
    if existing:
        print(f"Dataset lokal siap ({len(existing)} partisi) di {output_dir}")
        return output_dir

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    snapshot_download(
        repo_id=repo_id,
        repo_type="dataset",
        allow_patterns="chicago_crimes/year=*/data.parquet",
        local_dir=output_dir.parent,
    )
    downloaded = list(output_dir.glob("year=*/data.parquet"))
    print(f"Dataset diunduh ({len(downloaded)} partisi) ke {output_dir}")
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL dan pengunduhan dataset Chicago Crime."
    )
    parser.add_argument(
        "--download-hf",
        action="store_true",
    )
    parser.add_argument(
        "--repo-id",
        type=str,
        default=DEFAULT_HF_REPO,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw/chicago_crimes.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/chicago_crimes"),
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.download_hf:
        ensure_dataset(args.output, args.repo_id)
    else:
        run_etl(args.input, args.output, args.year)