#!/usr/bin/env python3
"""Profile data files (Parquet/CSV) locally or from GCS.

Quickly inspect the shape, schema, and basic statistics of data files without
writing SQL or opening a notebook. Useful for exploring new datasets, validating
pipeline output, and documenting data assets.

Supports:
- Local files (CSV, Parquet)
- Local directories (summarizes all data files)
- Glob patterns (e.g., data/*.parquet)
- GCS URIs (gs://bucket/path)

Usage examples:
    python tools/gcs_profiler.py data/sample_data/claims.csv
    python tools/gcs_profiler.py data/sample_data/
    python tools/gcs_profiler.py data/*.parquet --json
    python tools/gcs_profiler.py gs://my-bucket/claims/ --sample 5
"""

import argparse
import glob
import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Data classes for structured profile results
# ---------------------------------------------------------------------------

@dataclass
class ColumnProfile:
    """Statistics for a single column in a data file."""
    name: str
    dtype: str
    null_count: int | None = None
    min_value: str | None = None
    max_value: str | None = None


@dataclass
class FileProfile:
    """Profile summary for a single data file."""
    path: str
    format: str
    size_bytes: int
    row_count: int
    column_count: int
    columns: list[ColumnProfile] = field(default_factory=list)
    sample_rows: list[dict] | None = None


@dataclass
class DirectoryProfile:
    """Aggregate profile for a directory of data files."""
    path: str
    files: list[FileProfile]
    total_size_bytes: int
    total_rows: int
    csv_count: int
    parquet_count: int


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_bytes(num_bytes: int) -> str:
    """Convert a byte count into a human-readable string.

    Args:
        num_bytes: Raw byte count.

    Returns:
        A string like "2.3 MB" or "4.2 GB".
    """
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024**2:
        return f"{num_bytes / 1024:.1f} KB"
    elif num_bytes < 1024**3:
        return f"{num_bytes / 1024**2:.1f} MB"
    elif num_bytes < 1024**4:
        return f"{num_bytes / 1024**3:.2f} GB"
    else:
        return f"{num_bytes / 1024**4:.3f} TB"


def format_number(n: int) -> str:
    """Format an integer with thousands separators for readability.

    Args:
        n: An integer to format.

    Returns:
        A string like "10,000".
    """
    return f"{n:,}"


def truncate(value: str, max_len: int = 20) -> str:
    """Truncate a string to max_len, appending '...' if shortened.

    Args:
        value: The string to truncate.
        max_len: Maximum length before truncation.

    Returns:
        The original or truncated string.
    """
    if len(value) > max_len:
        return value[: max_len - 3] + "..."
    return value


# ---------------------------------------------------------------------------
# CSV profiling via DuckDB
# ---------------------------------------------------------------------------

def profile_csv(file_path: str, sample_n: int | None = None) -> FileProfile:
    """Profile a CSV file using DuckDB for fast type inference and stats.

    DuckDB is used instead of pandas because it handles type inference well,
    is fast even on large files, and supports SQL-based profiling natively.

    Args:
        file_path: Path to the CSV file.
        sample_n: If set, include this many sample rows in the profile.

    Returns:
        A FileProfile with column-level statistics.
    """
    try:
        import duckdb
    except ImportError:
        print(
            "Error: duckdb is not installed.\n"
            "Install it with: pip install duckdb\n"
            "Or:              pip install -r tools/requirements.txt",
            file=sys.stderr,
        )
        sys.exit(1)

    size_bytes = os.path.getsize(file_path)

    # Use a fresh in-memory DuckDB connection per file to avoid state leaks.
    con = duckdb.connect()

    # read_csv_auto handles delimiter detection, header detection, and type
    # inference in a single pass -- much faster than manual sniffing.
    try:
        con.execute(
            "CREATE TABLE data AS SELECT * FROM read_csv_auto(?)",
            [file_path],
        )
    except Exception as exc:
        print(f"Error reading CSV file {file_path}: {exc}", file=sys.stderr)
        sys.exit(1)

    # Get row count.
    row_count = con.execute("SELECT COUNT(*) FROM data").fetchone()[0]

    # Get column names and types from DuckDB's information schema.
    col_info = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'data' ORDER BY ordinal_position"
    ).fetchall()

    columns: list[ColumnProfile] = []
    for col_name, col_type in col_info:
        # Compute null count, min, and max for each column.
        # We quote column names to handle spaces and reserved words.
        stats = con.execute(
            f'SELECT COUNT(*) - COUNT("{col_name}") AS nulls, '
            f'MIN("{col_name}")::VARCHAR AS min_val, '
            f'MAX("{col_name}")::VARCHAR AS max_val '
            f"FROM data"
        ).fetchone()

        null_count, min_val, max_val = stats
        columns.append(
            ColumnProfile(
                name=col_name,
                dtype=col_type,
                null_count=null_count,
                min_value=str(min_val) if min_val is not None else None,
                max_value=str(max_val) if max_val is not None else None,
            )
        )

    # Optionally collect sample rows.
    sample_rows = None
    if sample_n and sample_n > 0:
        rows = con.execute(f"SELECT * FROM data LIMIT {sample_n}").fetchall()
        col_names = [c.name for c in columns]
        sample_rows = [dict(zip(col_names, row)) for row in rows]
        # Convert non-serializable types to strings for JSON output.
        for row in sample_rows:
            for k, v in row.items():
                if not isinstance(v, (str, int, float, bool, type(None))):
                    row[k] = str(v)

    con.close()

    return FileProfile(
        path=file_path,
        format="CSV",
        size_bytes=size_bytes,
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
        sample_rows=sample_rows,
    )


# ---------------------------------------------------------------------------
# Parquet profiling via PyArrow
# ---------------------------------------------------------------------------

def profile_parquet(file_path: str, sample_n: int | None = None) -> FileProfile:
    """Profile a Parquet file using PyArrow metadata (avoids full-file scan).

    PyArrow can read Parquet metadata (schema, row counts, column stats) from
    the file footer without reading all row groups. This makes profiling fast
    even for multi-GB files.

    Args:
        file_path: Path to the Parquet file.
        sample_n: If set, include this many sample rows in the profile.

    Returns:
        A FileProfile with column-level statistics from Parquet metadata.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print(
            "Error: pyarrow is not installed.\n"
            "Install it with: pip install pyarrow\n"
            "Or:              pip install -r tools/requirements.txt",
            file=sys.stderr,
        )
        sys.exit(1)

    size_bytes = os.path.getsize(file_path)

    try:
        parquet_file = pq.ParquetFile(file_path)
    except Exception as exc:
        print(f"Error reading Parquet file {file_path}: {exc}", file=sys.stderr)
        sys.exit(1)

    metadata = parquet_file.metadata
    schema = parquet_file.schema_arrow
    row_count = metadata.num_rows

    columns: list[ColumnProfile] = []
    for i in range(schema.__len__()):
        col_field = schema.field(i)
        col_name = col_field.name
        col_type = str(col_field.type)

        # Aggregate column statistics across all row groups.
        # Parquet stores min/max and null counts per row group per column.
        total_nulls = 0
        min_val = None
        max_val = None
        stats_available = True

        for rg_idx in range(metadata.num_row_groups):
            col_chunk = metadata.row_group(rg_idx).column(i)
            if col_chunk.is_stats_set:
                total_nulls += col_chunk.statistics.num_nulls or 0
                if col_chunk.statistics.has_min_max:
                    rg_min = col_chunk.statistics.min
                    rg_max = col_chunk.statistics.max
                    if min_val is None or rg_min < min_val:
                        min_val = rg_min
                    if max_val is None or rg_max > max_val:
                        max_val = rg_max
            else:
                stats_available = False

        columns.append(
            ColumnProfile(
                name=col_name,
                dtype=col_type,
                null_count=total_nulls if stats_available else None,
                min_value=str(min_val) if min_val is not None else None,
                max_value=str(max_val) if max_val is not None else None,
            )
        )

    # Optionally read sample rows (requires reading actual data).
    sample_rows = None
    if sample_n and sample_n > 0:
        try:
            table = parquet_file.read_row_groups([0])
            sample_table = table.slice(0, sample_n)
            sample_rows = sample_table.to_pylist()
            # Convert non-serializable types for JSON output.
            for row in sample_rows:
                for k, v in row.items():
                    if not isinstance(v, (str, int, float, bool, type(None))):
                        row[k] = str(v)
        except Exception:
            # If reading data fails, skip samples rather than crashing.
            sample_rows = None

    return FileProfile(
        path=file_path,
        format="Parquet",
        size_bytes=size_bytes,
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
        sample_rows=sample_rows,
    )


# ---------------------------------------------------------------------------
# GCS support
# ---------------------------------------------------------------------------

def download_from_gcs(gcs_uri: str, dest_dir: str) -> list[str]:
    """Download file(s) from a GCS URI to a local temporary directory.

    Supports both single-file URIs (gs://bucket/file.csv) and prefix URIs
    (gs://bucket/path/) which download all matching blobs.

    Args:
        gcs_uri: A GCS URI starting with gs://.
        dest_dir: Local directory to download files into.

    Returns:
        List of local file paths that were downloaded.
    """
    try:
        from google.cloud import storage
    except ImportError:
        print(
            "Error: google-cloud-storage is not installed.\n"
            "Install it with: pip install google-cloud-storage\n"
            "Or:              pip install -r tools/requirements.txt",
            file=sys.stderr,
        )
        sys.exit(1)

    # Parse the GCS URI into bucket and prefix.
    if not gcs_uri.startswith("gs://"):
        print(f"Error: Invalid GCS URI: {gcs_uri}", file=sys.stderr)
        sys.exit(1)

    parts = gcs_uri[5:].split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
    except Exception as exc:
        error_msg = str(exc).lower()
        if "credentials" in error_msg or "auth" in error_msg:
            print(
                "Error: GCS authentication not configured.\n"
                "Run:   gcloud auth application-default login\n"
                "Then retry this command.",
                file=sys.stderr,
            )
        else:
            print(f"Error accessing GCS bucket '{bucket_name}': {exc}", file=sys.stderr)
        sys.exit(1)

    # List blobs matching the prefix, filtering to supported file types.
    blobs = list(bucket.list_blobs(prefix=prefix))
    supported_extensions = {".csv", ".parquet", ".pq"}

    downloaded: list[str] = []
    for blob in blobs:
        ext = Path(blob.name).suffix.lower()
        if ext not in supported_extensions:
            continue
        # Preserve filename in the temp directory.
        local_path = os.path.join(dest_dir, Path(blob.name).name)
        blob.download_to_filename(local_path)
        downloaded.append(local_path)

    if not downloaded:
        print(
            f"Error: No CSV or Parquet files found at {gcs_uri}\n"
            f"Searched prefix: '{prefix}' in bucket '{bucket_name}'.",
            file=sys.stderr,
        )
        sys.exit(1)

    return downloaded


# ---------------------------------------------------------------------------
# File discovery and dispatch
# ---------------------------------------------------------------------------

SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".pq"}


def discover_files(path_arg: str) -> list[str]:
    """Resolve a path argument into a list of data file paths.

    Handles single files, directories, glob patterns, and GCS URIs.

    Args:
        path_arg: User-provided path (file, directory, glob, or gs:// URI).

    Returns:
        List of absolute local file paths to profile.
    """
    # GCS URI -- download first.
    if path_arg.startswith("gs://"):
        tmp_dir = tempfile.mkdtemp(prefix="gcs_profiler_")
        return download_from_gcs(path_arg, tmp_dir)

    # Glob pattern -- expand it.
    expanded = glob.glob(path_arg)
    if expanded:
        files = []
        for p in sorted(expanded):
            if os.path.isfile(p) and Path(p).suffix.lower() in SUPPORTED_EXTENSIONS:
                files.append(os.path.abspath(p))
        if files:
            return files

    # Single file.
    if os.path.isfile(path_arg):
        ext = Path(path_arg).suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            print(
                f"Error: Unsupported file format '{ext}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}",
                file=sys.stderr,
            )
            sys.exit(1)
        return [os.path.abspath(path_arg)]

    # Directory -- find all supported files.
    if os.path.isdir(path_arg):
        files = []
        for entry in sorted(os.listdir(path_arg)):
            full_path = os.path.join(path_arg, entry)
            if os.path.isfile(full_path) and Path(entry).suffix.lower() in SUPPORTED_EXTENSIONS:
                files.append(os.path.abspath(full_path))
        if not files:
            print(
                f"Error: No CSV or Parquet files found in directory: {path_arg}",
                file=sys.stderr,
            )
            sys.exit(1)
        return files

    # Nothing matched.
    print(f"Error: Path not found: {path_arg}", file=sys.stderr)
    sys.exit(1)


def profile_file(file_path: str, sample_n: int | None = None) -> FileProfile:
    """Route a file to the correct profiler based on its extension.

    Args:
        file_path: Absolute path to a CSV or Parquet file.
        sample_n: Number of sample rows to include, or None.

    Returns:
        A FileProfile for the file.
    """
    ext = Path(file_path).suffix.lower()
    if ext == ".csv":
        return profile_csv(file_path, sample_n=sample_n)
    elif ext in (".parquet", ".pq"):
        return profile_parquet(file_path, sample_n=sample_n)
    else:
        print(f"Error: Unsupported format '{ext}' for {file_path}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def print_single_profile(profile: FileProfile) -> None:
    """Print a detailed profile for a single data file as a formatted table.

    Args:
        profile: The FileProfile to display.
    """
    filename = Path(profile.path).name
    print()
    print(f"Data Profile: {filename}")
    print("=" * (len("Data Profile: ") + len(filename)))
    print(f"Path:     {profile.path}")
    print(f"Format:   {profile.format}")
    print(f"Size:     {format_bytes(profile.size_bytes)}")
    print(f"Rows:     {format_number(profile.row_count)}")
    print(f"Columns:  {profile.column_count}")
    print()

    if not profile.columns:
        return

    # Calculate column widths for aligned output.
    headers = ["Column", "Type", "Nulls", "Min", "Max"]
    col_widths = [len(h) for h in headers]

    rows: list[list[str]] = []
    for col in profile.columns:
        row = [
            truncate(col.name, 25),
            truncate(col.dtype, 15),
            format_number(col.null_count) if col.null_count is not None else "N/A",
            truncate(col.min_value, 20) if col.min_value is not None else "N/A",
            truncate(col.max_value, 20) if col.max_value is not None else "N/A",
        ]
        rows.append(row)
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    # Print header row.
    header_line = "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("  ".join("-" * col_widths[i] for i in range(len(headers))))

    # Print data rows.
    for row in rows:
        print("  ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row)))

    # Print sample rows if available.
    if profile.sample_rows:
        print()
        print(f"Sample Rows ({len(profile.sample_rows)}):")
        print("-" * 40)
        for i, row in enumerate(profile.sample_rows, 1):
            print(f"  Row {i}:")
            for k, v in row.items():
                print(f"    {k}: {v}")
    print()


def print_directory_profile(dir_profile: DirectoryProfile) -> None:
    """Print an aggregate summary for a directory of data files.

    Args:
        dir_profile: The DirectoryProfile to display.
    """
    dirname = dir_profile.path
    print()
    print(f"Directory Profile: {dirname}")
    print("=" * (len("Directory Profile: ") + len(dirname)))

    # Build format summary string like "3 CSV, 2 Parquet".
    parts = []
    if dir_profile.csv_count > 0:
        parts.append(f"{dir_profile.csv_count} CSV")
    if dir_profile.parquet_count > 0:
        parts.append(f"{dir_profile.parquet_count} Parquet")
    total_files = dir_profile.csv_count + dir_profile.parquet_count
    format_summary = ", ".join(parts)

    print(f"Files:    {total_files} data files ({format_summary})")
    print(f"Size:     {format_bytes(dir_profile.total_size_bytes)} total")
    print(f"Rows:     {format_number(dir_profile.total_rows)} total")
    print()

    # File listing table.
    headers = ["File", "Format", "Size", "Rows", "Columns"]
    col_widths = [len(h) for h in headers]

    rows: list[list[str]] = []
    for fp in dir_profile.files:
        row = [
            truncate(Path(fp.path).name, 30),
            fp.format,
            format_bytes(fp.size_bytes),
            format_number(fp.row_count),
            str(fp.column_count),
        ]
        rows.append(row)
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    header_line = "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("  ".join("-" * col_widths[i] for i in range(len(headers))))

    for row in rows:
        print("  ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row)))

    print()
    script_name = Path(__file__).name
    print(f"Use: {script_name} <file> for detailed column stats")
    print()


def output_json(profiles: list[FileProfile]) -> None:
    """Print profile results as JSON to stdout.

    Useful for piping into jq or consuming from other scripts.

    Args:
        profiles: List of FileProfile objects to serialize.
    """
    data = [asdict(p) for p in profiles]
    print(json.dumps(data, indent=2, default=str))


# ---------------------------------------------------------------------------
# CLI and main
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list (defaults to sys.argv[1:] when None).

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Profile data files (CSV/Parquet) locally or from GCS.",
        epilog=(
            "Examples:\n"
            "  %(prog)s data/sample_data/claims.csv\n"
            "  %(prog)s data/sample_data/\n"
            "  %(prog)s data/*.parquet --json\n"
            "  %(prog)s gs://my-bucket/claims/ --sample 5\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "path",
        type=str,
        help="File path, directory, glob pattern, or GCS URI (gs://...)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output results as JSON instead of formatted tables",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        metavar="N",
        help="Include N sample rows in the output",
    )

    return parser.parse_args(argv)


def main() -> None:
    """Entry point: discover files, profile them, and print results."""
    args = parse_args()

    # Discover which files to profile.
    file_paths = discover_files(args.path)

    # Profile each file.
    profiles: list[FileProfile] = []
    for fp in file_paths:
        profiles.append(profile_file(fp, sample_n=args.sample))

    # Output results.
    if args.json:
        output_json(profiles)
    elif len(profiles) == 1:
        # Single file -- show detailed column stats.
        print_single_profile(profiles[0])
    else:
        # Multiple files -- show directory summary.
        # Determine the common parent directory for the header.
        if args.path.startswith("gs://"):
            dir_path = args.path
        elif os.path.isdir(args.path):
            dir_path = args.path
        else:
            dir_path = str(Path(file_paths[0]).parent)

        csv_count = sum(1 for p in profiles if p.format == "CSV")
        parquet_count = sum(1 for p in profiles if p.format == "Parquet")

        dir_profile = DirectoryProfile(
            path=dir_path,
            files=profiles,
            total_size_bytes=sum(p.size_bytes for p in profiles),
            total_rows=sum(p.row_count for p in profiles),
            csv_count=csv_count,
            parquet_count=parquet_count,
        )
        print_directory_profile(dir_profile)


if __name__ == "__main__":
    main()
