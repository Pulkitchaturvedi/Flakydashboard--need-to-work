"""Data ingestion entry point for test analytics ETL.

This module supports collecting data from Google Sheets and Google BigQuery,
normalising it into a consistent schema, and writing the merged data set to a
persistent store.  Execution is controlled via a small CLI that understands the
source to ingest from and whether the run is part of the scheduled daily ETL.
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

# Optional imports: they are only required when the corresponding source is used.
try:  # pragma: no cover - exercised at runtime only when the package exists
    from google.oauth2 import service_account
except ImportError:  # pragma: no cover - handled gracefully when dependency missing
    service_account = None  # type: ignore

SCHEMA_FIELDS: Sequence[str] = (
    "run_timestamp",
    "platform",
    "team",
    "suite",
    "test_case_id",
    "failure_reason",
    "status",
    "build_id",
    "environment",
)


@dataclass
class IngestionConfig:
    """Configuration derived from CLI arguments."""

    sources: Sequence[str]
    schedule: str
    sheets_spreadsheet_id: Optional[str]
    sheets_range: Optional[str]
    bigquery_project: Optional[str]
    bigquery_dataset: Optional[str]
    bigquery_table: Optional[str]
    bigquery_where: Optional[str]
    bigquery_parameters: Sequence[str]
    output_dir: Path
    output_format: str


def parse_args(argv: Optional[Sequence[str]] = None) -> IngestionConfig:
    parser = argparse.ArgumentParser(description="Daily ETL ingestion entry point")
    parser.add_argument(
        "--source",
        choices=("sheets", "bigquery"),
        nargs="+",
        default=("sheets", "bigquery"),
        help="Which data source(s) to ingest. Default ingests from both.",
    )
    parser.add_argument(
        "--schedule",
        choices=("adhoc", "daily"),
        default="adhoc",
        help="Schedule label for the run. Use 'daily' for the production ETL",
    )
    parser.add_argument(
        "--sheets-spreadsheet-id",
        dest="sheets_spreadsheet_id",
        help="Spreadsheet ID to read test results from",
    )
    parser.add_argument(
        "--sheets-range",
        dest="sheets_range",
        default="Sheet1",
        help="A1-style range within the spreadsheet (default: Sheet1)",
    )
    parser.add_argument(
        "--bigquery-project",
        dest="bigquery_project",
        help="Google Cloud project that hosts the BigQuery dataset",
    )
    parser.add_argument(
        "--bigquery-dataset",
        dest="bigquery_dataset",
        help="Dataset name containing the source table",
    )
    parser.add_argument(
        "--bigquery-table",
        dest="bigquery_table",
        help="Table name containing the test results",
    )
    parser.add_argument(
        "--bigquery-where",
        dest="bigquery_where",
        help="Optional SQL WHERE clause (without the 'WHERE' keyword)",
    )
    parser.add_argument(
        "--bigquery-parameter",
        dest="bigquery_parameters",
        action="append",
        default=(),
        help=(
            "Query parameter in the form name:type:value. Supported types: "
            "STRING, INT64, FLOAT64, BOOL, DATE, DATETIME, TIMESTAMP"
        ),
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default="data/raw",
        help="Directory for persisted extracts (default: data/raw)",
    )
    parser.add_argument(
        "--output-format",
        dest="output_format",
        choices=("parquet", "csv"),
        default="parquet",
        help="Durable output format (default: parquet)",
    )

    args = parser.parse_args(argv)

    return IngestionConfig(
        sources=tuple(dict.fromkeys(args.source)),  # remove duplicates, preserve order
        schedule=args.schedule,
        sheets_spreadsheet_id=args.sheets_spreadsheet_id,
        sheets_range=args.sheets_range,
        bigquery_project=args.bigquery_project,
        bigquery_dataset=args.bigquery_dataset,
        bigquery_table=args.bigquery_table,
        bigquery_where=args.bigquery_where,
        bigquery_parameters=tuple(args.bigquery_parameters),
        output_dir=Path(args.output_dir),
        output_format=args.output_format,
    )


def load_service_account_credentials(scopes: Sequence[str]):
    """Load service-account credentials from environment variables.

    The loader understands multiple encodings to make local development easy:
    - ``GOOGLE_SERVICE_ACCOUNT_JSON``: raw JSON string
    - ``GOOGLE_SERVICE_ACCOUNT_B64``: base64 encoded JSON
    - ``GOOGLE_APPLICATION_CREDENTIALS``: file path to the JSON document
    """

    if service_account is None:  # pragma: no cover - dependency missing
        raise RuntimeError(
            "google-auth is required to load service-account credentials. "
            "Install the google-auth library to ingest from Google sources."
        )

    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw_json:
        info = json.loads(raw_json)
    else:
        raw_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64")
        if raw_b64:
            decoded = base64.b64decode(raw_b64)
            info = json.loads(decoded)
        else:
            credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if not credentials_path:
                raise RuntimeError(
                    "Service-account credentials were not provided. Set either "
                    "GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SERVICE_ACCOUNT_B64, or "
                    "GOOGLE_APPLICATION_CREDENTIALS."
                )
            with open(credentials_path, "r", encoding="utf-8") as fh:
                info = json.load(fh)

    return service_account.Credentials.from_service_account_info(info, scopes=scopes)


def fetch_from_sheets(spreadsheet_id: str, range_name: str) -> List[Dict[str, Any]]:
    """Pull tabular data from Google Sheets."""

    if not spreadsheet_id:
        raise ValueError("Spreadsheet ID must be provided when sourcing from Sheets")

    scopes = ("https://www.googleapis.com/auth/spreadsheets.readonly",)
    credentials = load_service_account_credentials(scopes)

    try:  # pragma: no cover - requires external dependency
        from googleapiclient.discovery import build
    except ImportError as exc:  # pragma: no cover - handled when dependency missing
        raise RuntimeError(
            "The google-api-python-client package is required for Sheets ingestion"
        ) from exc

    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    sheet = service.spreadsheets()
    response = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = response.get("values", [])
    if not values:
        return []

    header = [canonicalise_key(cell) for cell in values[0]]
    records: List[Dict[str, Any]] = []
    for row in values[1:]:
        padded_row = list(row) + [None] * (len(header) - len(row))
        record = {header[idx]: padded_row[idx] for idx in range(len(header))}
        records.append(normalise_record(record))
    return records


def fetch_from_bigquery(
    project: str,
    dataset: str,
    table: str,
    where_clause: Optional[str],
    parameter_specs: Sequence[str],
) -> List[Dict[str, Any]]:
    """Run a parameterised query against BigQuery to retrieve the schema."""

    for value, name in ((project, "project"), (dataset, "dataset"), (table, "table")):
        if not value:
            raise ValueError(f"BigQuery {name} must be provided when sourcing from BigQuery")

    scopes = (
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
    )
    credentials = load_service_account_credentials(scopes)

    try:  # pragma: no cover - requires external dependency
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover - handled when dependency missing
        raise RuntimeError("The google-cloud-bigquery package is required for BigQuery ingestion") from exc

    client = bigquery.Client(project=project, credentials=credentials)

    table_ref = f"`{project}.{dataset}.{table}`"
    query = (
        "SELECT run_timestamp, platform, team, suite, test_case_id, "
        "failure_reason, status, build_id, environment "
        f"FROM {table_ref}"
    )
    if where_clause:
        query = f"{query} WHERE {where_clause}"

    job_config = bigquery.QueryJobConfig()
    if parameter_specs:
        job_config.query_parameters = [parse_bigquery_parameter(spec) for spec in parameter_specs]

    logging.info("Executing BigQuery job: %s", query)
    result = client.query(query, job_config=job_config).result()
    records = [normalise_record(dict(row.items())) for row in result]
    return records


def parse_bigquery_parameter(spec: str):
    """Parse CLI input into a BigQuery query parameter."""

    try:
        name, type_name, value = spec.split(":", 2)
    except ValueError as exc:
        raise ValueError(
            "BigQuery parameter specification must follow name:type:value"
        ) from exc

    try:  # pragma: no cover - requires external dependency
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("The google-cloud-bigquery package is required for BigQuery ingestion") from exc

    scalar_types = {
        "STRING": (bigquery.ScalarQueryParameter, str),
        "INT64": (bigquery.ScalarQueryParameter, lambda x: int(x)),
        "FLOAT64": (bigquery.ScalarQueryParameter, float),
        "BOOL": (bigquery.ScalarQueryParameter, lambda x: x.lower() in {"true", "1", "t", "yes"}),
        "DATE": (bigquery.ScalarQueryParameter, str),
        "DATETIME": (bigquery.ScalarQueryParameter, str),
        "TIMESTAMP": (bigquery.ScalarQueryParameter, str),
    }
    type_name_upper = type_name.upper()
    if type_name_upper not in scalar_types:
        raise ValueError(f"Unsupported BigQuery parameter type: {type_name}")

    constructor, caster = scalar_types[type_name_upper]
    cast_value = caster(value)
    return constructor(name, type_name_upper, cast_value)


def canonicalise_key(value: str) -> str:
    """Convert arbitrary headings to snake_case used by the schema."""

    cleaned = value.strip().lower().replace(" ", "_")
    cleaned = cleaned.replace("-", "_")
    return cleaned


def normalise_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Ensure each record conforms to the shared schema."""

    normalised: Dict[str, Any] = {}
    for field in SCHEMA_FIELDS:
        normalised[field] = record.get(field)
        if normalised[field] is None:
            # Try alternative keys that may have different casing or spacing
            alt_keys = {
                field.upper(),
                field.replace("_", " "),
                field.replace("_", "").lower(),
            }
            for key in alt_keys:
                if key in record:
                    normalised[field] = record[key]
                    break
    # Ensure timestamps are ISO formatted strings where possible
    if normalised.get("run_timestamp"):
        normalised["run_timestamp"] = ensure_iso_timestamp(normalised["run_timestamp"])
    if normalised.get("status"):
        normalised["status"] = str(normalised["status"]).lower()
    return normalised


def ensure_iso_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc).isoformat()
    return str(value)


def write_durable_store(records: Sequence[Mapping[str, Any]], config: IngestionConfig) -> Path:
    if not records:
        raise ValueError("No records were ingested; nothing to persist")

    config.output_dir.mkdir(parents=True, exist_ok=True)
    run_suffix = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    schedule_tag = config.schedule
    output_path = config.output_dir / f"etl_{schedule_tag}_{run_suffix}.{config.output_format}"

    logging.info("Writing %d rows to %s", len(records), output_path)

    try:
        import pandas as pd  # type: ignore  # pragma: no cover - requires external dependency
    except ImportError as exc:  # pragma: no cover - handle missing pandas
        raise RuntimeError("The pandas package is required to write the durable extract") from exc

    frame = pd.DataFrame(records, columns=SCHEMA_FIELDS)
    if config.output_format == "parquet":
        try:
            frame.to_parquet(output_path, index=False)
        except ImportError:  # pragma: no cover - fallback when pyarrow/fastparquet missing
            logging.warning(
                "pyarrow/fastparquet is unavailable, falling back to CSV output"
            )
            fallback_path = output_path.with_suffix(".csv")
            frame.to_csv(fallback_path, index=False)
            return fallback_path
    else:
        frame.to_csv(output_path, index=False)
    return output_path


def ingest(config: IngestionConfig) -> Path:
    """Coordinate the end-to-end ingestion workflow."""

    all_records: List[Dict[str, Any]] = []
    if "sheets" in config.sources:
        logging.info("Fetching data from Google Sheets")
        sheets_records = fetch_from_sheets(
            spreadsheet_id=config.sheets_spreadsheet_id or "",
            range_name=config.sheets_range or "Sheet1",
        )
        all_records.extend(sheets_records)

    if "bigquery" in config.sources:
        logging.info("Fetching data from BigQuery")
        bigquery_records = fetch_from_bigquery(
            project=config.bigquery_project or "",
            dataset=config.bigquery_dataset or "",
            table=config.bigquery_table or "",
            where_clause=config.bigquery_where,
            parameter_specs=config.bigquery_parameters,
        )
        all_records.extend(bigquery_records)

    if not all_records:
        raise RuntimeError("Ingestion finished without retrieving any records")

    return write_durable_store(all_records, config)


def main(argv: Optional[Sequence[str]] = None) -> Path:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = parse_args(argv)
    output_path = ingest(config)
    logging.info("ETL completed; output written to %s", output_path)
    return output_path


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
