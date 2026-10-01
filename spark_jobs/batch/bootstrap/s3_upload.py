"""Upload logistics CSV files to MinIO (an S3-compatible object store).

The script is usable locally and as an Airflow task. Configure it with
``DATA_DIR``, ``MINIO_ENDPOINT``, ``MINIO_ROOT_USER``,
``MINIO_ROOT_PASSWORD``, ``MINIO_BUCKET_RAW`` and ``MINIO_CSV_PREFIX`` (default ``csv``).

Files land in s3://<bucket>/<prefix>/<name>.csv (raw/csv/ by default), kept apart from
the streaming parquet in raw/logistics/. spark_jobs/batch/bootstrap/build_raw_history.py
turns them into the partitioned history in raw/history/.
"""
import os
from pathlib import Path

from minio import Minio


def minio_endpoint(url: str) -> tuple[str, bool]:
    """Return the endpoint format expected by the MinIO client."""
    secure = url.startswith("https://")
    return url.removeprefix("http://").removeprefix("https://"), secure


def upload_csvs(data_dir: str | None = None) -> int:
    source_dir = Path(data_dir or os.environ.get("DATA_DIR", "/opt/airflow/dataset"))
    if not source_dir.is_dir():
        raise FileNotFoundError(f"CSV source directory does not exist: {source_dir}")

    endpoint, secure = minio_endpoint(os.environ["MINIO_ENDPOINT"])
    client = Minio(
        endpoint,
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=secure,
    )
    bucket = os.environ.get("MINIO_BUCKET_RAW", "raw")
    prefix = os.environ.get("MINIO_CSV_PREFIX", "csv")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    files = sorted(source_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {source_dir}")
    for path in files:
        # Replaces an existing object, so an Airflow retry is safe.
        client.fput_object(bucket, f"{prefix}/{path.name}", str(path))
        print(f"Uploaded {path.name} to s3://{bucket}/{prefix}/{path.name}")
    return len(files)


if __name__ == "__main__":
    print(f"Uploaded {upload_csvs()} CSV files")
