#!/usr/bin/env python
# coding: utf-8

"""
Utility functions for HotGlue ETL operations.

This module provides common utility functions used across the ETL pipeline for:
- Data reading and transformation
- Field mapping and column renaming
- Deduplication and snapshot management
- Data type conversions and formatting
- Nested structure transformations

These utilities are designed to work with the HotGlue framework and support
both Salesforce and HubSpot connector operations.
"""

import ast
import glob
import json as _json
import logging
import os
import re
from typing import Dict, Iterator, List, Optional, Tuple

import pandas as pd
import gluestick as gs

logger = logging.getLogger(__name__)


def get_stream_data(reader: gs.Reader, stream: str, catalog_types: bool = True) -> pd.DataFrame:
    """
    Retrieve data for a specific stream from the HotGlue reader.
    
    This wrapper ensures consistent behavior across all data reading operations
    by defaulting catalog_types to True, which preserves data type information
    from the source catalog.
    
    Args:
        reader: The gluestick Reader instance for accessing input data
        stream: Name of the stream to read (e.g., 'contacts', 'accounts')
        catalog_types: Whether to use catalog-defined types (defaults to True)
        
    Returns:
        DataFrame containing the stream data with appropriate typing
        
    Example:
        >>> reader = gs.Reader('/path/to/data')
        >>> contacts_df = get_stream_data(reader, 'contacts')
    """
    return reader.get(stream, catalog_types=catalog_types)


def map_stream_data(
    stream_data: pd.DataFrame, 
    stream: str, 
    mapping: Dict[str, Dict]
) -> Tuple[List[str], pd.DataFrame]:
    """
    Transform data columns based on field mapping configuration.
    
    This function applies field mappings to transform source data into the target
    format required by the destination system. It handles column renaming,
    external ID generation, and ensures all required fields are present.
    
    Args:
        stream_data: DataFrame containing the source data to be transformed
        stream: Name of the data stream being processed (e.g., 'contacts', 'accounts')
        mapping: Nested dictionary with stream-specific field mappings
                Format: {stream: {source_field: target_field, ...}}
        
    Returns:
        Tuple containing:
        - List of column names in the transformed data
        - DataFrame with renamed columns and external ID added
    
    Raises:
        ValueError: If mapping is missing or no mapping found for the stream
        
    Example:
        >>> mapping = {'contacts': {'firstName': 'FirstName', 'email': 'Email'}}
        >>> columns, df = map_stream_data(contacts_df, 'contacts', mapping)
    """
    # Validate inputs
    if not mapping:
        raise ValueError("Mapping configuration is required")
    
    if stream not in mapping:
        raise ValueError(f"No mapping found for stream: {stream}")
    
    # Create a copy to avoid mutating the original mapping
    stream_mapping = dict(mapping.get(stream))
    
    # Ensure remote_id mapping is present (standard field for ID tracking)
    stream_mapping.setdefault("remote_id", "Id")

    target_api_columns = list(stream_mapping.keys())
    connector_columns = list(stream_mapping.values())

    # Preserve the original 'id' column value BEFORE mapping
    # This is critical for write operations where we need to track the source system ID
    original_id = stream_data["id"].copy() if "id" in stream_data.columns else None

    # Identify columns that need to be renamed
    columns_to_rename = [
        column for column in stream_data.columns
        if column in target_api_columns
    ]

    # Apply column renaming based on mapping
    for column in columns_to_rename:
        try:
            target_column = connector_columns[target_api_columns.index(column)]
            stream_data[target_column] = stream_data[column]
        except Exception as e:
            logger.warning(f"Error renaming column {column}: {str(e)}")

    # Build list of columns present in the data
    stream_columns = [
        col for col in connector_columns if col in stream_data.columns
    ]

    # Add external ID for tracking (maps internal ID to external system)
    # Use the ORIGINAL id value (before mapping), not the mapped value
    if original_id is not None:
        stream_columns.append("externalId")
        stream_data["externalId"] = original_id

    # Remove duplicate column names while preserving order
    unique_columns = []
    for col in stream_columns:
        if col not in unique_columns:
            unique_columns.append(col)
    
    # Return only the mapped columns
    stream_data = stream_data[unique_columns].copy()

    return unique_columns, stream_data


# The exact ECMAScript non-numeric number literals. A downstream consumer
# (e.g. the hotglue target-hubspot) re-emits a quoted value EQUAL to one of
# these as the corresponding *bare* JSON token (``NaN``/``Infinity``), which
# strict parsers — notably Jackson on the HubSpot API — reject
# ("Non-standard token 'NaN'"), failing the entire export.
#
# Matching is CASE-SENSITIVE and exact, on purpose: production proved the
# downstream distinguishes ``"NaN"`` (a jobtitle that broke the export) from
# ``"Nan"`` (a common given name that synced fine). A case-insensitive match
# (the original implementation) silently nulled valid ``"Nan"`` first names.
# Other NA-like strings ("N/A", "None", "null", "nan") stay valid JSON and are
# left untouched.
_NONFINITE_FLOAT_TOKENS = frozenset({"NaN", "Infinity", "-Infinity"})


def _spells_nonfinite_float(value: object) -> bool:
    """Return True if ``value`` is exactly one of the non-standard JSON number
    tokens ``"NaN"``, ``"Infinity"``, ``"-Infinity"``.

    Case-sensitive and exact — see ``_NONFINITE_FLOAT_TOKENS`` for why (a
    case-insensitive match nulls valid names like ``"Nan"``).
    """
    return isinstance(value, str) and value in _NONFINITE_FLOAT_TOKENS


# A number written with grouping commas and/or a trailing comma, with an
# optional decimal — e.g. "1,316", "1,234,567", "298,", "12,345.67".
# ast.literal_eval reads the comma form as a Python tuple ("298," -> (298,),
# "1,316" -> (1, 316)), which the downstream serializes as a JSON array and
# strict scalar (String) targets reject. Removing the commas restores the
# intended numeric scalar. The pattern allows empty groups so a trailing comma
# ("298,") is caught.
_NUMERIC_COMMA_RE = re.compile(r"[+-]?\d*(?:,\d*)+(?:\.\d+)?")
# Ambiguous European-decimal form ("12,34"): a single comma followed by 1-2
# digits and no decimal point. There is no safe rewrite (could mean 12.34 or
# 1234), so it is left flagged rather than guessed.
_EURO_DECIMAL_RE = re.compile(r"[+-]?\d+,\d{1,2}")
# A plain (comma-free) number used to validate the result after de-comma'ing.
_PLAIN_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")

# A container literal must contain one of these; used as a cheap pre-filter so
# we only attempt ast.literal_eval on the few values that could parse to one.
_CONTAINER_HINT_CHARS = (",", "[", "{", "(")


def _comma_number_scalar(value: str) -> Optional[str]:
    """Return the intended numeric scalar for a number written with grouping
    and/or a trailing comma ("1,316" -> "1316", "298," -> "298",
    "12,345.67" -> "12345.67"), else None.

    Returns None for anything that is not sign/digits/commas with an optional
    decimal, for the ambiguous European-decimal form (see ``_EURO_DECIMAL_RE``),
    and for values that do not reduce to a real number (e.g. ",").
    """
    s = value.strip()
    if not _NUMERIC_COMMA_RE.fullmatch(s):
        return None
    if _EURO_DECIMAL_RE.fullmatch(s):
        return None
    stripped = s.replace(",", "")
    return stripped if _PLAIN_NUMBER_RE.fullmatch(stripped) else None


def _parses_to_python_container(value: object) -> bool:
    """Return True if ``ast.literal_eval`` would parse ``value`` into a
    container (tuple/list/dict/set).

    The hotglue/gluestick read path (``gluestick.parse_objs``) "unstringifies"
    field values with ``ast.literal_eval``. A value like ``"1,316"`` is a valid
    Python tuple literal ``(1, 316)``, so it gets re-serialized as the JSON
    array ``[1, 316]`` and is rejected by scalar (String) target properties —
    e.g. the HubSpot API raises ``Cannot deserialize value of type
    java.lang.String from Array value`` for the ``zip`` field. (A scalar result
    such as the int from ``"94538"`` is harmless: strict parsers coerce a JSON
    number into a String; only arrays/objects break.)
    """
    if not isinstance(value, str):
        return False
    if not any(c in value for c in _CONTAINER_HINT_CHARS):
        return False
    try:
        return isinstance(ast.literal_eval(value), (tuple, list, dict, set))
    except Exception:
        return False


def _sanitize_singer_value(value: object, counts: Dict[str, int]) -> object:
    """Recursively sanitize string leaves so the Singer output survives strict,
    ``ast.literal_eval``-based downstream parsers (e.g. hotglue target-hubspot).

    Two classes of malformed scalar string are handled:

    1. NaN/Infinity tokens ("NaN", "Infinity", "-Infinity") -> ``None`` —
       otherwise re-emitted as the non-standard JSON tokens ``NaN``/``Infinity``
       and rejected by Jackson. (See ``_spells_nonfinite_float``.)
    2. Strings ``ast.literal_eval`` would turn into a container ("1,316" ->
       ``(1, 316)``, "298," -> ``(298,)``). A comma-formatted integer (grouping
       and/or trailing comma) has its separators removed so it round-trips as
       the intended scalar ("1,316" -> "1316", "298," -> "298"). Any *other*
       container literal (e.g. the ambiguous European decimal "12,34", or
       "[1, 2]") has no safe, connector-agnostic rewrite — stripping commas
       would corrupt a decimal, and ``repr()``-wrapping would inject literal
       quotes for non-literal_eval consumers like the CBX1 read path — so it is
       left UNCHANGED and only flagged. A loud, isolated downstream failure is
       preferable to silently dropping or corrupting data.

    Only values that are safely repairable are altered — every other string
    passes through untouched. Walks nested dicts/lists. ``counts`` accumulates
    per-reason tallies for logging.
    """
    if isinstance(value, str):
        if _spells_nonfinite_float(value):
            counts["nonfinite"] = counts.get("nonfinite", 0) + 1
            return None
        if _parses_to_python_container(value):
            repaired = _comma_number_scalar(value)
            if repaired is not None:
                counts["comma_number"] = counts.get("comma_number", 0) + 1
                return repaired
            counts["flagged_container"] = counts.get("flagged_container", 0) + 1
            return value
        return value
    if isinstance(value, dict):
        return {k: _sanitize_singer_value(v, counts) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_singer_value(v, counts) for v in value]
    return value


def prepare_for_singer(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Prepare DataFrame for Singer format output.

    Singer is a specification for data exchange that requires specific formatting,
    particularly for datetime fields. This function ensures all datetime columns
    are properly serialized to ISO 8601 format strings.

    It also sanitizes string values that strict, ``ast.literal_eval``-based
    downstream parsers (e.g. target-hubspot) would mangle: NaN/Infinity
    spellings (re-emitted as the non-standard JSON tokens ``NaN``/``Infinity``)
    and strings that parse as a Python container — notably grouped numbers like
    ``"1,316"``, which become the tuple ``(1, 316)`` -> JSON array ``[1, 316]``
    and are rejected by scalar (String) properties. See
    ``_sanitize_singer_value``.

    Args:
        df: DataFrame to prepare for Singer output

    Returns:
        DataFrame with datetime columns converted to Singer-compatible strings

    Raises:
        ValueError: If input is None instead of a DataFrame

    Note:
        Singer format requires datetime fields to be in ISO 8601 format:
        YYYY-MM-DDTHH:MM:SS.ffffffZ
    """
    if df is None:
        raise ValueError("Expected DataFrame, received None")

    prepared_df = df.copy()

    # Find all datetime columns
    datetime_cols = prepared_df.select_dtypes(include=["datetime", "datetimetz"]).columns

    # Convert datetime columns to ISO 8601 string format
    for col in datetime_cols:
        series = prepared_df[col]
        formatted = series.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ").where(series.notna(), None)
        prepared_df = prepared_df.astype({col: "object"})
        prepared_df.loc[:, col] = formatted

    # Sanitize string values that strict, ast.literal_eval-based downstream
    # parsers (e.g. hotglue target-hubspot) would mangle: NaN/Infinity spellings
    # and strings that parse as Python containers. Only string/object columns can
    # hold these; numeric NaN is already serialized as JSON null by the writer.
    # See _sanitize_singer_value for the rules.
    counts: Dict[str, int] = {}
    touched_cols: List[str] = []
    _CHANGED = ("nonfinite", "comma_number")  # reasons that mutate the value
    string_cols = prepared_df.select_dtypes(include=["object", "string"]).columns
    for col in string_cols:
        before = sum(counts.get(k, 0) for k in _CHANGED)
        scrubbed = prepared_df[col].map(lambda v: _sanitize_singer_value(v, counts))
        if sum(counts.get(k, 0) for k in _CHANGED) > before:
            prepared_df[col] = scrubbed
            touched_cols.append(col)

    if counts:
        logger.warning(
            "prepare_for_singer: sanitized %d malformed string value(s) for downstream "
            "JSON safety (changed columns: %s; reasons: %s). %d container-literal(s) "
            "left as-is and may fail strict scalar targets.",
            sum(counts.get(k, 0) for k in _CHANGED),
            touched_cols,
            counts,
            counts.get("flagged_container", 0),
        )

    return prepared_df


def drop_sent_records(
    stream: str,
    stream_data: pd.DataFrame,
    sent_data: Optional[pd.DataFrame],
    new_data: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Filter out records that have already been sent to prevent duplicates.

    Compares current data against historical snapshots to keep only records that
    have not been sent yet. When a ``new_data`` frame is supplied, records present
    in it are re-included even if they appear in ``sent_data`` — this is the
    "record changed since last sync" branch used by contacts and accounts.

    Args:
        stream: Name of the data stream being processed (e.g., 'contacts', 'accounts')
        stream_data: DataFrame containing the current batch of data
        sent_data: DataFrame containing previously sent records from snapshots
        new_data: Optional DataFrame of records flagged as updated. When provided,
            any record whose ``externalId`` is in it bypasses the sent-records
            filter so content changes propagate to the target.

    Returns:
        DataFrame containing only records that haven't been sent or have updates

    Raises:
        ValueError: If required ID columns are missing

    Note:
        - Uses 'externalId' in stream_data and 'InputId' in sent_data for comparison
        - The "re-include updated records" branch applies to any stream that
          supplies ``new_data`` (previously contacts-only; extended to fix the
          accounts write-once bug where field updates never reached the CRM).
    """
    # If no sent data, return all stream data
    if sent_data is None:
        return stream_data

    # Validate required columns exist
    if "externalId" not in stream_data.columns:
        raise ValueError("externalId column is required in stream_data")

    if "InputId" not in sent_data.columns:
        raise ValueError("InputId column is required in sent_data")

    # Filter out records that have already been sent
    condition = ~stream_data["externalId"].isin(sent_data["InputId"])

    # Re-include records flagged as updated (content has changed since last sync).
    if new_data is not None and "externalId" in new_data.columns:
        condition = condition | (stream_data["externalId"].isin(new_data["externalId"]))

    return stream_data.loc[condition].copy()


def split_contacts_by_account(
    contacts: pd.DataFrame, 
    sent_accounts: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split contacts into Contacts and Leads based on account associations.
    
    Salesforce distinguishes between Contacts (associated with accounts) and
    Leads (not yet associated with accounts). This function performs that split
    based on whether a contact has a valid account association.
    
    Args:
        contacts: DataFrame containing all contact records
        sent_accounts: DataFrame containing accounts that have been synced
                      Must contain 'AccountId' and 'RemoteAccountId' columns
        
    Returns:
        Tuple of (contacts_df, leads_df):
        - contacts_df: Records with valid account associations
        - leads_df: Records without account associations (will become Leads)
    
    Raises:
        ValueError: If required columns are missing from input DataFrames
        
    Note:
        This is specific to Salesforce's Contact/Lead model. Other CRMs
        may not make this distinction.
    """
    # Validate required columns
    if "accountId" not in contacts.columns:
        raise ValueError("accountId column is required in contacts DataFrame")
    
    if "AccountId" not in sent_accounts.columns or "RemoteAccountId" not in sent_accounts.columns:
        raise ValueError("AccountId and RemoteAccountId columns are required in sent_accounts DataFrame")
    
    # Join contacts with accounts to identify associations
    contacts_data = contacts.copy()
    contacts_data = contacts_data.merge(
        sent_accounts, left_on="accountId", right_on="AccountId", how="left"
    )
    
    # Mark records without account associations
    contacts_data["RemoteAccountId"] = contacts_data["RemoteAccountId"].fillna("missing")
    
    # Split based on account association
    contacts_df = contacts_data[contacts_data["RemoteAccountId"] != "missing"].copy()
    leads_df = contacts_data[contacts_data["RemoteAccountId"] == "missing"].copy()
    
    # Clean up temporary columns
    if "AccountId" in contacts_df.columns:
        contacts_df.drop(columns=['AccountId', 'RemoteAccountId'], inplace=True, errors="ignore")
    
    if "AccountId" in leads_df.columns:
        leads_df.drop(columns=['AccountId', 'RemoteAccountId'], inplace=True, errors="ignore")
    
    logger.info(f"Split {len(contacts)} contacts into {len(contacts_df)} Contacts and {len(leads_df)} Leads")
    
    return contacts_df, leads_df


def transform_dot_notation_to_nested(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform dot-notation and array notation fields to nested objects and arrays.
    
    Supports:
    - Simple dot notation: "hqLocation.city" -> {"hqLocation": {"city": "value"}}
    - Array notation: "fundingHistory[*].amount" -> {"fundingHistory": [{"amount": "value"}]}
    - Nested arrays: "fundingHistory[*].investors[*]" -> {"fundingHistory": [{"investors": ["value"]}]}
    
    Args:
        df: DataFrame containing columns with dot/array notation
        
    Returns:
        DataFrame with nested objects and arrays properly structured
    """
    if df.empty:
        return df
    
    # Find columns with dot notation or array notation
    special_columns = [col for col in df.columns if '.' in col or '[*]' in col]
    
    if not special_columns:
        return df
    
    # Group columns by their structure
    nested_groups = {}
    array_groups = {}
    
    for col in special_columns:
        if '[*]' in col:
            # Handle array notation: "fundingHistory[*].amount" or "fundingHistory[*].investors[*]"
            _parse_array_column(col, array_groups)
        else:
            # Handle simple dot notation: "hqLocation.city"
            _parse_dot_column(col, nested_groups)
    
    # Create result DataFrame
    result_df = df.copy()
    
    # Process simple nested objects first
    for parent, children in nested_groups.items():
        nested_data = []
        for _, row in df.iterrows():
            nested_obj = {}
            for child_key, original_col in children.items():
                if original_col in row and pd.notna(row[original_col]):
                    nested_obj[child_key] = row[original_col]
            nested_data.append(nested_obj if nested_obj else None)
        
        result_df[parent] = nested_data
        result_df = result_df.drop(columns=list(children.values()))
    
    # Process array structures
    for array_parent, array_structure in array_groups.items():
        array_data = []
        for _, row in df.iterrows():
            array_obj = _build_array_structure(row, array_structure)
            array_data.append(array_obj if array_obj else None)
        
        result_df[array_parent] = array_data
        # Drop all original array columns
        cols_to_drop = _get_array_columns_to_drop(array_structure)
        result_df = result_df.drop(columns=cols_to_drop, errors='ignore')
    
    return result_df


def _parse_dot_column(col: str, nested_groups: dict):
    """Parse simple dot notation columns."""
    parts = col.split('.', 1)
    parent = parts[0]
    child = parts[1]
    
    if parent not in nested_groups:
        nested_groups[parent] = {}
    nested_groups[parent][child] = col


def _parse_array_column(col: str, array_groups: dict):
    """Parse array notation columns like 'fundingHistory[*].amount' or 'fundingHistory[*].investors[*]'."""
    # Split on first [*] to get the array parent
    if '[*]' not in col:
        return
    
    parts = col.split('[*]', 1)
    array_parent = parts[0]
    remainder = parts[1]
    
    if array_parent not in array_groups:
        array_groups[array_parent] = {'fields': {}, 'nested_arrays': {}}
    
    if remainder.startswith('.'):
        # Remove leading dot
        remainder = remainder[1:]
        
        if '[*]' in remainder:
            # Nested array case: "investors[*]"
            nested_parts = remainder.split('[*]', 1)
            nested_array_name = nested_parts[0]
            
            if nested_array_name not in array_groups[array_parent]['nested_arrays']:
                array_groups[array_parent]['nested_arrays'][nested_array_name] = []
            
            array_groups[array_parent]['nested_arrays'][nested_array_name].append(col)
        else:
            # Simple field case: "amount"
            array_groups[array_parent]['fields'][remainder] = col


def _build_array_structure(row: pd.Series, array_structure: dict) -> list:
    """Build array structure for a single row."""
    result = []
    
    # Collect all non-null values for this array
    all_values = {}
    
    # Process simple fields
    for field_name, original_col in array_structure['fields'].items():
        if original_col in row and pd.notna(row[original_col]):
            all_values[field_name] = row[original_col]
    
    # Process nested arrays
    for nested_array_name, columns in array_structure['nested_arrays'].items():
        nested_values = []
        for col in columns:
            if col in row and pd.notna(row[col]):
                nested_values.append(row[col])
        if nested_values:
            all_values[nested_array_name] = nested_values
    
    # If we have any values, create an array with one object containing all values
    # This is a simplified approach - in reality, you might need more complex logic
    # to handle multiple array items from different columns
    if all_values:
        result.append(all_values)
    
    return result


def _get_array_columns_to_drop(array_structure: dict) -> list:
    """Get list of all original columns that should be dropped after array transformation."""
    cols_to_drop = []
    
    # Add simple field columns
    cols_to_drop.extend(array_structure['fields'].values())
    
    # Add nested array columns
    for columns in array_structure['nested_arrays'].values():
        cols_to_drop.extend(columns)
    
    return cols_to_drop

def get_contact_data(
    connector_id: str,
    stream: str,
    contacts: Optional[pd.DataFrame],
    leads: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Select the appropriate contact dataset based on connector and stream type.
    
    Different CRM systems handle contacts differently:
    - Salesforce: Distinguishes between Contacts (with accounts) and Leads (without)
    - HubSpot: Has a single contacts stream
    
    This function returns the correct dataset based on these requirements.
    
    Args:
        connector_id: CRM connector identifier ('salesforce', 'hubspot', etc.)
        stream: Stream type - 'Contact' or 'Lead' (only relevant for Salesforce)
        contacts: DataFrame containing contact records
        leads: DataFrame containing lead records (only used for Salesforce)
        
    Returns:
        DataFrame with the appropriate contact/lead data for the connector
        Returns empty DataFrame if the requested data is None
    
    Raises:
        ValueError: If stream type is invalid for Salesforce connector
        
    Example:
        >>> # For Salesforce Contact stream
        >>> df = get_contact_data('salesforce', 'Contact', contacts_df, leads_df)
        >>> # For HubSpot (stream parameter is ignored)
        >>> df = get_contact_data('hubspot', 'any', contacts_df, None)
    """
    if connector_id == "salesforce":
        if stream not in ["Contact", "Lead"]:
            raise ValueError(
                f"Invalid stream type for Salesforce: {stream}. "
                f"Expected 'Contact' or 'Lead'."
            )
        
        # Return appropriate data based on stream type
        if stream == "Contact":
            result = contacts if contacts is not None else pd.DataFrame()
            logger.debug(f"Returning {len(result)} Contact records for Salesforce")
            return result
        else:  # stream == "Lead"
            result = leads if leads is not None else pd.DataFrame()
            logger.debug(f"Returning {len(result)} Lead records for Salesforce")
            return result
    else:
        # For non-Salesforce connectors, always return contacts
        result = contacts if contacts is not None else pd.DataFrame()
        logger.debug(f"Returning {len(result)} contact records for {connector_id}")
        return result


# ---------------------------------------------------------------------------
# Chunked Singer I/O helpers
# ---------------------------------------------------------------------------

READ_CHUNK_SIZE = 10_000


def iter_stream_chunks(
    input_dir: str,
    stream: str,
    chunk_size: int = READ_CHUNK_SIZE,
) -> Iterator[pd.DataFrame]:
    """
    Yield DataFrames of at most chunk_size rows from stream input files.

    HotGlue sync-output commonly stores records as stream-prefixed parquet
    files, such as contacts-20260505T083605.parquet. Some local or target
    variants may write line-delimited Singer/JSON files. Both paths are read
    in chunks so the full stream is not loaded into a single DataFrame.
    """
    parquet_files = _find_stream_files(input_dir, stream, (".parquet",))
    if parquet_files:
        for parquet_file in parquet_files:
            yield from _iter_parquet_stream_chunks(parquet_file, chunk_size)
        return

    record_files = _find_stream_files(input_dir, stream, (".singer", ".json", ".jsonl", ""))
    if record_files:
        for record_file in record_files:
            yield from _iter_record_stream_chunks(record_file, stream, chunk_size)
        return

    try:
        available = sorted(os.listdir(input_dir))
    except OSError:
        available = ["<unreadable>"]

    logger.warning(
        "No data file found for stream '%s' in %s. Available files: %s",
        stream,
        input_dir,
        available,
    )


def _find_stream_files(input_dir: str, stream: str, extensions: Tuple[str, ...]) -> List[str]:
    """Return stream files in deterministic order for exact and timestamped names."""
    files: List[str] = []
    for ext in extensions:
        exact = os.path.join(input_dir, f"{stream}{ext}")
        if os.path.exists(exact):
            files.append(exact)

        if ext:
            files.extend(glob.glob(os.path.join(input_dir, f"{stream}-*{ext}")))

    return sorted(set(files))


def _iter_parquet_stream_chunks(
    parquet_file: str,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    """Yield row batches from a parquet file without loading the full file."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.warning(
            "pyarrow is not installed; reading parquet file %s in one pandas batch",
            parquet_file,
        )
        df = pd.read_parquet(parquet_file)
        if not df.empty:
            yield df
        return

    parquet = pq.ParquetFile(parquet_file)
    for batch in parquet.iter_batches(batch_size=chunk_size):
        df = batch.to_pandas()
        if not df.empty:
            yield df


def _iter_record_stream_chunks(
    record_file: str,
    stream: str,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    """Yield batches from line-delimited Singer or plain JSON records."""
    batch: list = []
    with open(record_file, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue

            try:
                msg = _json.loads(raw)
            except _json.JSONDecodeError:
                logger.debug("Skipping malformed line in %s", record_file)
                continue

            if not isinstance(msg, dict):
                continue

            msg_type = msg.get("type")
            if msg_type == "RECORD":
                if msg.get("stream") not in (None, stream):
                    continue
                batch.append(msg.get("record", msg))
            elif msg_type in ("SCHEMA", "STATE"):
                continue
            else:
                batch.append(msg)

            if len(batch) >= chunk_size:
                yield pd.DataFrame(batch)
                batch = []

    if batch:
        yield pd.DataFrame(batch)


_BASE_SCALAR_TYPES = ["null", "boolean", "string", "integer", "number"]


def _detect_singer_col_type(series: "pd.Series", sample_size: int = 100) -> list:
    """Return Singer JSON Schema type list for a column based on observed values.

    Probes up to ``sample_size`` non-null values. If any are dict, "object" is added;
    if any are list, "array" is added. Scalar columns get the permissive base set so
    boolean / numeric / string values aren't coerced by downstream loaders.

    Manual iteration (rather than pd.isna) avoids the elementwise-NaN check blowing
    up on unhashable dict/list values in object-dtype columns.
    """
    has_dict = False
    has_list = False
    seen = 0
    for v in series:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if isinstance(v, dict):
            has_dict = True
        elif isinstance(v, list):
            has_list = True
        seen += 1
        if seen >= sample_size and (has_dict or has_list):
            break
    types = list(_BASE_SCALAR_TYPES)
    if has_dict:
        types.append("object")
    if has_list:
        types.append("array")
    return types


def append_singer_records(
    df: pd.DataFrame,
    stream_name: str,
    output_dir: str,
    first_chunk: bool,
) -> None:
    """
    Append Singer RECORD messages to the shared ``data.singer`` output file.

    All streams share a single output file (matching the gluestick.to_singer
    convention) so that the downstream target ingests every stream from one
    payload. The file is created on the first write and appended thereafter;
    when ``first_chunk=True`` a SCHEMA message is prepended so each stream's
    schema appears exactly once even across many chunks and multiple streams.

    Columns containing dict / list values are declared with ``object`` / ``array``
    types in the SCHEMA so nested values flow through to downstream consumers as
    real JSON structures. Null values are omitted from each record.
    """
    output_path = os.path.join(output_dir, "data.singer")
    mode = "a" if os.path.isfile(output_path) else "w"

    with open(output_path, mode, encoding="utf-8") as fh:
        if first_chunk:
            # Per-column type detection. boolean is required so HubSpot boolean
            # fields (e.g. hs_email_optout) are not cast to strings by the
            # downstream loader; object/array are required so wrapper columns
            # like ``data`` round-trip as navigable JSON instead of TextNodes.
            properties = {
                col: {"type": _detect_singer_col_type(df[col])} for col in df.columns
            }
            schema_msg = {
                "type": "SCHEMA",
                "stream": stream_name,
                "schema": {"type": "object", "properties": properties},
                "key_properties": [],
            }
            fh.write(_json.dumps(schema_msg) + "\n")

        for record in df.to_dict(orient="records"):
            clean: dict = {}
            for k, v in record.items():
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    continue
                clean[k] = v
            fh.write(
                _json.dumps(
                    {"type": "RECORD", "stream": stream_name, "record": clean},
                    default=str,
                )
                + "\n"
            )
