#!/usr/bin/env python
# coding: utf-8

"""
HotGlue ETL Script - Main Entry Point

This script serves as the main orchestrator for ETL operations between different CRM systems
(Salesforce and HubSpot) and the data warehouse. It coordinates the read and write operations
by delegating to connector-specific handlers.

Environment Variables:
    ROOT_DIR: Root directory for data processing (default: ".")
    JOB_TYPE: Type of job to execute - "write" (to CRM) or "read" (from CRM) (default: "write")
    FLOW: Flow identifier for tracking and snapshot management (default: "AJ3x0LMYI")
    CONNECTOR_ID: Identifier for the CRM connector ("salesforce" or "hubspot")

Directory Structure:
    sync-output/: Input data from source systems
    snapshots/: Historical data snapshots for deduplication
    etl-output/: Transformed data output in Singer format
    
Workflow:
    Write Path: Source Data -> Transform -> Map Fields -> Write to CRM Format
    Read Path: CRM Data -> Transform -> Enrich -> Write to Data Warehouse Format
"""

import json
import logging
import os
from typing import Dict, Optional, Tuple

import gluestick as gs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Standard directories for HotGlue
ROOT_DIR = os.environ.get("ROOT_DIR", ".")
INPUT_DIR = f"{ROOT_DIR}/sync-output"
SNAPSHOT_DIR = f"{ROOT_DIR}/snapshots"
OUTPUT_DIR = f"{ROOT_DIR}/etl-output"


def _load_json(path: str) -> Optional[dict]:
    """
    Load JSON data from a file.
    
    Args:
        path: Path to the JSON file
        
    Returns:
        Parsed JSON data as a dictionary, or None if file doesn't exist
    """
    if not os.path.exists(path):
        logger.debug(f"JSON file not found: {path}")
        return None
    
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to load JSON from {path}: {e}")
        return None


def _load_tenant_mapping(flow_id: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Load tenant-specific field mapping configuration.
    
    This function loads the mapping configuration that defines how fields should be
    transformed between source and target systems. The configuration is stored in
    a tenant-specific JSON file.
    
    Args:
        flow_id: Unique identifier for the flow/job
        
    Returns:
        Tuple of (mapping_for_flow, stream_name_mapping):
        - mapping_for_flow: Dictionary keyed by "target_api/connector" containing field mappings
        - stream_name_mapping: Dictionary mapping target API stream names to connector stream names
        
        Returns (None, None) if no configuration is found.
        
    Example:
        mapping_for_flow: {"contacts/Contact": {"firstName": "FirstName", ...}}
        stream_name_mapping: {"contacts": "Contact"}
    """
    tenant_cfg = _load_json(f"{SNAPSHOT_DIR}/tenant-config.json")
    if not tenant_cfg:
        logger.warning("No tenant configuration found")
        return None, None

    mapping_root = tenant_cfg.get("hotglue_mapping", {}).get("mapping", {})
    mapping_for_flow = mapping_root.get(flow_id)
    if not mapping_for_flow:
        logger.warning(f"No mapping found for flow: {flow_id}")
        return None, None

    # Build stream name mapping from the configuration keys
    stream_name_mapping = {}
    for key in mapping_for_flow.keys():
        parts = key.split("/")
        if len(parts) == 2:
            target_stream, connector_stream = parts
            stream_name_mapping[target_stream] = connector_stream
        else:
            logger.warning(f"Invalid mapping key format: {key}")
    
    logger.info(f"Loaded mapping for {len(stream_name_mapping)} streams")
    return mapping_for_flow, stream_name_mapping


def _load_target_config() -> Dict:
    """
    Load target system configuration.
    
    This configuration contains settings specific to the target CRM system,
    such as API endpoints, authentication details, and feature flags.
    
    Returns:
        Dictionary containing target configuration, or empty dict if not found
    """
    cfg = _load_json(f"{ROOT_DIR}/target-config.json")
    if not cfg:
        logger.info("No target configuration found, using defaults")
        return {}
    
    logger.info(f"Loaded target configuration with {len(cfg)} settings")
    return cfg


def _get_handler(connector_id: str, flow_id: str, reader: gs.Reader, 
                 mapping_for_flow: Optional[Dict], stream_name_mapping: Optional[Dict],
                 target_config: Optional[Dict]):
    """
    Get the appropriate handler for the specified connector.
    
    This factory function creates and returns a connector-specific handler
    that implements the ETL logic for that particular CRM system.
    
    Args:
        connector_id: Identifier for the connector ("salesforce" or "hubspot")
        flow_id: Unique identifier for the flow
        reader: Gluestick reader instance
        mapping_for_flow: Field mapping configuration
        stream_name_mapping: Stream name mappings
        target_config: Target system configuration
        
    Returns:
        Handler instance for the specified connector
        
    Raises:
        ValueError: If the connector_id is not supported
    """
    connector_id_lower = str(connector_id).lower() if connector_id else ""
    
    if connector_id_lower == "salesforce":
        from salesforce_handler import SalesforceHandler
        return SalesforceHandler(
            connector_id=connector_id,
            flow_id=flow_id,
            reader=reader,
            mapping_for_flow=mapping_for_flow,
            stream_name_mapping=stream_name_mapping,
            input_dir=INPUT_DIR,
            snapshot_dir=SNAPSHOT_DIR,
            output_dir=OUTPUT_DIR,
            target_config=target_config,
        )
    elif connector_id_lower == "hubspot":
        from hubspot_handler import HubSpotHandler
        return HubSpotHandler(
            connector_id=connector_id,
            flow_id=flow_id,
            reader=reader,
            mapping_for_flow=mapping_for_flow,
            stream_name_mapping=stream_name_mapping,
            input_dir=INPUT_DIR,
            snapshot_dir=SNAPSHOT_DIR,
            output_dir=OUTPUT_DIR,
            target_config=target_config,
        )
    else:
        raise ValueError(
            f"Unsupported connector: {connector_id}. "
            f"Supported connectors are: salesforce, hubspot"
        )




def main() -> None:
    """
    Main entry point for the ETL script.
    
    This function:
    1. Loads configuration from environment variables
    2. Loads tenant-specific mapping configuration
    3. Creates the appropriate handler based on the connector
    4. Executes either a write or read operation
    
    The operation type is determined by the JOB_TYPE environment variable:
    - "write": Transform and write data to CRM system format
    - "read": Read from CRM system and transform to data warehouse format
    
    Raises:
        ValueError: If required configuration is missing or invalid
        Exception: If the ETL operation fails
    """
    # Load configuration from environment
    job_type = os.environ.get("JOB_TYPE", "write")
    flow_id = os.environ.get("FLOW", "AJ3x0LMYI")
    connector_id = os.environ.get("CONNECTOR_ID")
    
    logger.info(f"Starting ETL job: type={job_type}, flow={flow_id}, connector={connector_id}")
    
    if not connector_id:
        raise ValueError("CONNECTOR_ID environment variable is required")
    
    # Initialize reader for input data
    reader = gs.Reader(INPUT_DIR)
    
    # Load configurations
    mapping_for_flow, stream_name_mapping = _load_tenant_mapping(flow_id)
    target_config = _load_target_config()
    
    # Get the appropriate handler for the connector
    try:
        handler = _get_handler(
            connector_id=connector_id,
            flow_id=flow_id,
            reader=reader,
            mapping_for_flow=mapping_for_flow,
            stream_name_mapping=stream_name_mapping,
            target_config=target_config,
        )
    except ValueError as e:
        logger.error(f"Failed to initialize handler: {e}")
        raise
    
    # Execute the appropriate operation
    try:
        if job_type == "write":
            logger.info("Executing write operation")
            handler.handle_write()
            logger.info("Write operation completed successfully")
        elif job_type == "read":
            logger.info("Executing read operation") 
            handler.handle_read()
            logger.info("Read operation completed successfully")
        else:
            raise ValueError(f"Invalid job type: {job_type}. Must be 'write' or 'read'")
    except Exception as e:
        logger.error(f"ETL operation failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
