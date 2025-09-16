#!/usr/bin/env python
# coding: utf-8

"""
Base handler class for ETL operations.

This module provides the abstract base class for connector-specific handlers,
defining the interface for read and write operations.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

import gluestick as gs
import pandas as pd

logger = logging.getLogger(__name__)


class BaseETLHandler(ABC):
    """
    Abstract base class for ETL handlers.
    
    This class defines the interface that all connector-specific handlers must implement.
    It provides common functionality for reading data, managing mappings, and handling
    snapshots.
    
    Attributes:
        connector_id (str): Identifier for the connector (e.g., 'salesforce', 'hubspot')
        flow_id (str): Unique identifier for the flow/job
        reader (gs.Reader): Gluestick reader instance for accessing input data
        mapping_for_flow (Dict): Field mapping configuration for the flow
        stream_name_mapping (Dict): Mapping between target and connector stream names
        input_dir (str): Directory containing input data
        snapshot_dir (str): Directory for snapshot storage
        output_dir (str): Directory for output data
    """
    
    def __init__(
        self,
        connector_id: str,
        flow_id: str,
        reader: gs.Reader,
        mapping_for_flow: Optional[Dict],
        stream_name_mapping: Optional[Dict],
        input_dir: str,
        snapshot_dir: str,
        output_dir: str,
    ):
        """
        Initialize the base handler with common configuration.
        
        Args:
            connector_id: Identifier for the connector
            flow_id: Unique identifier for the flow
            reader: Gluestick reader instance
            mapping_for_flow: Field mapping configuration
            stream_name_mapping: Stream name mappings
            input_dir: Input data directory
            snapshot_dir: Snapshot storage directory
            output_dir: Output data directory
        """
        self.connector_id = connector_id
        self.flow_id = flow_id
        self.reader = reader
        self.mapping_for_flow = mapping_for_flow or {}
        self.stream_name_mapping = stream_name_mapping or {}
        self.input_dir = input_dir
        self.snapshot_dir = snapshot_dir
        self.output_dir = output_dir
        
    @abstractmethod
    def handle_write(self) -> None:
        """
        Handle the write operation for this connector.
        
        This method should implement the logic to:
        1. Read data from the source
        2. Apply transformations and mappings
        3. Write data to the target system format
        
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    @abstractmethod
    def handle_read(self) -> None:
        """
        Handle the read operation for this connector.
        
        This method should implement the logic to:
        1. Read data from the connector
        2. Transform it to a standardized format
        3. Write it to the data warehouse format
        
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    def list_available_streams(self) -> List[str]:
        """
        Get a sorted list of available streams from the reader.
        
        Returns:
            List of stream names available in the input data
        """
        import ast
        streams = ast.literal_eval(str(self.reader))
        streams.sort()
        return streams
    
    def read_snapshot(self, stream_name: str) -> Optional[pd.DataFrame]:
        """
        Read a snapshot for the given stream.
        
        Args:
            stream_name: Name of the stream to read snapshot for
            
        Returns:
            DataFrame containing the snapshot data, or None if not found
        """
        snapshot_name = f"{stream_name}_{self.flow_id}"
        return gs.read_snapshots(snapshot_name, self.snapshot_dir)
    
    def write_snapshot(self, df: pd.DataFrame, stream_name: str, pk: str = "externalId") -> pd.DataFrame:
        """
        Write a snapshot for the given stream.
        
        Args:
            df: DataFrame to snapshot
            stream_name: Name of the stream
            pk: Primary key column name
            
        Returns:
            DataFrame containing the snapshot data
        """
        return gs.snapshot_records(df, stream_name, self.snapshot_dir, pk=pk)
    
    def write_to_singer(self, df: pd.DataFrame, stream_name: str) -> None:
        """
        Write a DataFrame to Singer format.
        
        Args:
            df: DataFrame to write
            stream_name: Name of the output stream
        """
        from utils import prepare_for_singer
        
        if df is None or df.empty:
            logger.info(f"No data to write for stream: {stream_name}")
            return
            
        output_df = prepare_for_singer(df)
        gs.to_singer(
            output_df,
            stream_name,
            self.output_dir,
            allow_objects=True,
        )
        logger.info(f"Wrote {len(output_df)} records to stream: {stream_name}")
    
    def get_stream_data(self, stream: str) -> pd.DataFrame:
        """
        Get data for a specific stream from the reader.
        
        Args:
            stream: Name of the stream to read
            
        Returns:
            DataFrame containing the stream data
        """
        from utils import get_stream_data
        return get_stream_data(self.reader, stream)
    
    def build_write_mapping(self) -> Dict:
        """
        Build write mapping from the flow mapping.
        
        Converts mapping from "target/connector" format to dict keyed by target.
        
        Returns:
            Dictionary with target stream as key and mapping as value
        """
        result = {}
        for k, v in self.mapping_for_flow.items():
            target = k.split("/")[0]
            result[target] = v
        return result
    
    def build_read_mapping(self) -> Dict:
        """
        Build read mapping from the flow mapping.
        
        Converts mapping to dict keyed by connector stream with remote_id mapping.
        
        Returns:
            Dictionary with connector stream as key and mapping as value
        """
        new_mapping: Dict[str, Dict] = {}
        for k, v in self.mapping_for_flow.items():
            target, connector = k.split("/")
            m = dict(v)
            m["remote_id"] = "Id"
            new_mapping[connector] = m
        return new_mapping
