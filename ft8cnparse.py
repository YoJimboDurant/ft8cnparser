#!/usr/bin/env python3
"""
ft8cnparse.py

Split an ADIF log into separate ADIF files by QSO date and operator,
and inject POTA park info into output records.

Usage:
    python3 ft8cnparse.py input.adi US-2177
    python3 ft8cnparse.py US-2177

If the input file is omitted, the script uses the most recent .adi/.adif
file in the current working directory.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


# ---------------------------------------------------------------------------
# ADIF parsing helpers
# ---------------------------------------------------------------------------

FIELD_RE = re.compile(r"<([^:>]+):(\d+)(?::[^>]+)?>", re.IGNORECASE)


def find_latest_adif_file() -> Path:
    """Return the most recently modified .adi/.adif file in the cwd."""
    candidates = list(Path(".").glob("*.adi")) + list(Path(".").glob("*.adif"))
    if not candidates:
        raise FileNotFoundError("No .adi or .adif files found in current directory.")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def strip_header(adif_text_x: str) -> str:
    """
    Return ADIF content after <eoh>, if present.
    Otherwise return the full text.
    """
    match_x = re.search(r"<eoh>", adif_text_x, flags=re.IGNORECASE)
    if match_x:
        return adif_text_x[match_x.end():]
    return adif_text_x


def split_records(adif_body_x: str) -> List[str]:
    """
    Split ADIF body into raw record strings ending at <eor>.
    """
    parts_lx = re.split(r"(?i)<eor>", adif_body_x)
    records_lx = []

    for part_x in parts_lx:
        record_x = part_x.strip()
        if record_x:
            records_lx.append(record_x)

    return records_lx


def parse_record(record_x: str) -> Dict[str, str]:
    """
    Parse one ADIF record into a dict of upper-case field names -> values.
    """
    fields_dx: Dict[str, str] = {}
    pos_n = 0
    text_len_n = len(record_x)

    while pos_n < text_len_n:
        match_x = FIELD_RE.search(record_x, pos_n)
        if not match_x:
            break

        field_name_x = match_x.group(1).strip().upper()
        field_len_n = int(match_x.group(2))
        value_start_n = match_x.end()
        value_end_n = value_start_n + field_len_n
        value_x = record_x[value_start_n:value_end_n]

        fields_dx[field_name_x] = value_x
        pos_n = value_end_n

    return fields_dx


def format_adif_field(field_name_x: str, value_x: str) -> str:
    """Format one ADIF field."""
    value_x = "" if value_x is None else str(value_x)
    return f"<{field_name_x}:{len(value_x)}>{value_x}"


def serialize_record(fields_dx: Dict[str, str]) -> str:
    """
    Serialize a record dict back to ADIF.
    """
    parts_lx = []
    for key_x, value_x in fields_dx.items():
        parts_lx.append(format_adif_field(key_x, value_x))
    parts_lx.append("<EOR>")
    return "".join(parts_lx)


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------

def get_operator_name(fields_dx: Dict[str, str]) -> str:
    """
    Determine operator name/call from common ADIF fields.
    Priority:
      1. OPERATOR
      2. STATION_CALLSIGN
      3. OWNER_CALLSIGN
      4. CALL
      5. UNKNOWN_OPERATOR
    """
    for key_x in ["OPERATOR", "STATION_CALLSIGN", "OWNER_CALLSIGN", "CALL"]:
        value_x = fields_dx.get(key_x, "").strip()
        if value_x:
            return value_x.upper()

    return "UNKNOWN_OPERATOR"


def get_qso_date(fields_dx: Dict[str, str]) -> str:
    """
    Return QSO_DATE if valid YYYYMMDD, otherwise UNKNOWN_DATE.
    """
    date_x = fields_dx.get("QSO_DATE", "").strip()
    if re.fullmatch(r"\d{8}", date_x):
        return date_x
    return "UNKNOWN_DATE"


def sanitize_filename_part(text_x: str) -> str:
    """Make a safe filename fragment."""
    text_x = text_x.strip().replace(" ", "_")
    text_x = re.sub(r"[^A-Za-z0-9._-]+", "_", text_x)
    text_x = re.sub(r"_+", "_", text_x)
    return text_x.strip("_") or "unknown"


def ensure_pota_fields(fields_dx: Dict[str, str], park_x: str, add_comment_flag: bool) -> Dict[str, str]:
    """
    Add MY_SIG / MY_SIG_INFO if missing.
    Optionally append park to COMMENT if not already present.
    """
    out_dx = dict(fields_dx)

    if not out_dx.get("MY_SIG", "").strip():
        out_dx["MY_SIG"] = "POTA"

    if not out_dx.get("MY_SIG_INFO", "").strip():
        out_dx["MY_SIG_INFO"] = park_x

    if add_comment_flag:
        comment_x = out_dx.get("COMMENT", "").strip()
        if park_x not in comment_x:
            if comment_x:
                out_dx["COMMENT"] = f"{comment_x} | {park_x}"
            else:
                out_dx["COMMENT"] = park_x

    return out_dx


def build_adif_header(program_id_x: str = "ft8cnparse", program_version_x: str = "1.0") -> str:
    """
    Build a simple ADIF header like the one used in your other workflow.
    """
    created_ts_x = datetime.utcnow().strftime("%Y%m%d %H%M%S")
    header_lx = [
        "ADIF Export\n",
        format_adif_field("ADIF_VER", "3.1.1"),
        "\n",
        format_adif_field("CREATED_TIMESTAMP", created_ts_x),
        "\n",
        format_adif_field("PROGRAMID", program_id_x),
        "\n",
        format_adif_field("PROGRAMVERSION", program_version_x),
        "\n",
        "<EOH>\n",
    ]
    return "".join(header_lx)


def write_grouped_files(
    grouped_dx: Dict[Tuple[str, str], List[Dict[str, str]]],
    park_x: str,
    output_dir_x: Path,
) -> List[Path]:
    """
    Write one ADIF file per (date, operator) group.
    """
    output_dir_x.mkdir(parents=True, exist_ok=True)
    written_files_lx: List[Path] = []

    for (date_x, operator_x), records_lx in sorted(grouped_dx.items()):
        safe_date_x = sanitize_filename_part(date_x)
        safe_operator_x = sanitize_filename_part(operator_x)
        safe_park_x = sanitize_filename_part(park_x)

        out_name_x = f"{safe_date_x}_{safe_operator_x}_{safe_park_x}.adi"
        out_path_x = output_dir_x / out_name_x

        with out_path_x.open("w", encoding="utf-8", newline="\n") as fx:
            fx.write(build_adif_header())

            for record_dx in records_lx:
                fx.write(serialize_record(record_dx))
                fx.write("\n")

        written_files_lx.append(out_path_x)

    return written_files_lx


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser_x = argparse.ArgumentParser(
        description="Split FT8CN ADIF file by date and operator, and inject POTA park info."
    )

    parser_x.add_argument(
        "arg1",
        help="Either the ADIF file path or the park (e.g., US-2177).",
    )
    parser_x.add_argument(
        "arg2",
        nargs="?",
        help="Park if arg1 is the input file.",
    )
    parser_x.add_argument(
        "--outdir",
        default="ft8cn_split",
        help="Output directory for split ADIF files (default: ft8cn_split).",
    )
    parser_x.add_argument(
        "--add-comment",
        action="store_true",
        help="Also append the park to COMMENT if not already present.",
    )

    return parser_x.parse_args()


def resolve_inputs(args_x: argparse.Namespace) -> Tuple[Path, str]:
    """
    Support both:
      python3 ft8cnparse.py input.adi US-2177
      python3 ft8cnparse.py US-2177
    """
    arg1_path_x = Path(args_x.arg1)

    if args_x.arg2 is not None:
        input_path_x = arg1_path_x
        park_x = args_x.arg2.strip().upper()
    else:
        park_x = args_x.arg1.strip().upper()
        input_path_x = find_latest_adif_file()

    if not input_path_x.exists():
        raise FileNotFoundError(f"Input file not found: {input_path_x}")

    if not park_x:
        raise ValueError("Park value is blank.")

    return input_path_x, park_x


def main() -> None:
    args_x = parse_args()

    try:
        input_path_x, park_x = resolve_inputs(args_x)
    except Exception as exc:
        print(f"ERROR resolving inputs: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        raw_text_x = input_path_x.read_text(encoding="utf-8", errors="ignore")
        adif_body_x = strip_header(raw_text_x)
        raw_records_lx = split_records(adif_body_x)

        if not raw_records_lx:
            raise ValueError("No ADIF records found in input file.")

        grouped_dx: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
        total_records_n = 0

        for raw_record_x in raw_records_lx:
            fields_dx = parse_record(raw_record_x)

            if not fields_dx:
                continue

            date_x = get_qso_date(fields_dx)
            operator_x = get_operator_name(fields_dx)

            fields_dx = ensure_pota_fields(
                fields_dx=fields_dx,
                park_x=park_x,
                add_comment_flag=args_x.add_comment,
            )

            key_x = (date_x, operator_x)
            grouped_dx.setdefault(key_x, []).append(fields_dx)
            total_records_n += 1

        if total_records_n == 0:
            raise ValueError("No valid ADIF records were parsed.")

        written_files_lx = write_grouped_files(
            grouped_dx=grouped_dx,
            park_x=park_x,
            output_dir_x=Path(args_x.outdir),
        )

        print(f"Input file : {input_path_x}")
        print(f"Park       : {park_x}")
        print(f"Records    : {total_records_n}")
        print(f"Groups     : {len(grouped_dx)}")
        print(f"Output dir : {Path(args_x.outdir).resolve()}")
        print("")
        print("Wrote files:")
        for path_x in written_files_lx:
            print(f"  {path_x}")

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
