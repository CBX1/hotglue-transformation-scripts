#!/usr/bin/env python
# coding: utf-8

# # Map custom fields #
# This transformation script reads all input data from the sync source as a Pandas dataframe and outputs directly to the `etl-output` folder. This is intended as a starting point to manipulate the data.


import gluestick as gs
import pandas as pd
import os
import json
import numpy as np
import ast
from utils import map_stream_data, drop_sent_records, split_contacts_by_account, get_contact_data


# Let's establish the standard hotglue input/output directories


# standard directory for hotglue
ROOT_DIR = os.environ.get("ROOT_DIR", ".")
INPUT_DIR = f"{ROOT_DIR}/sync-output"
SNAPSHOT_DIR = f"{ROOT_DIR}/snapshots"
OUTPUT_DIR = f"{ROOT_DIR}/etl-output"

# Get tenant config
config_path = f"{SNAPSHOT_DIR}/tenant-config.json"

config= None
# Verifies if `tenant-config.json` exists
if os.path.exists(config_path):
    with open(config_path) as f:
        config = json.load(f)

mapping = None
if config:
    # Get the mapping information if exists
    mapping = config.get("hotglue_mapping")
    mapping = mapping.get("mapping")


# get tap config
config_path = f"{ROOT_DIR}/config.json"

tap_config= None
# Verifies if `tenant-config.json` exists
if os.path.exists(config_path):
    with open(config_path) as f:
        tap_config = json.load(f)
else:
    print("No tap config found")
    tap_config = {}

# reading data from sync-output folder
input = gs.Reader(INPUT_DIR)

job_type = os.environ.get("JOB_TYPE", "write")
flow_id = os.environ.get("FLOW", "AJ3x0LMYI")
print(f"Running job: {job_type}")

s3_root = os.environ.get("JOB_ROOT")
connector_id = os.environ.get("CONNECTOR_ID", "salesforce")

# first name in mapping is target_api / second name is connector
if mapping is not None:
    mapping_tenant = flow_id
    mapping = mapping.get(mapping_tenant)

    stream_name_mapping = {
        stream.split("/")[0]: stream.split("/")[1] for stream in mapping.keys()
    }

if job_type == "write":
    # target_api -> connector
    if mapping is not None:
        new_mapping = {}
        for stream in mapping.keys():
            new_stream = stream.split("/")[0]
            new_mapping[new_stream] = mapping.get(stream)

    if mapping is not None:
        streams = eval(str(input))
        streams.sort()
        new_mapping = {}
        for stream in mapping.keys():
            new_stream = stream.split("/")[0]
            new_mapping[new_stream] = mapping.get(stream)

        for stream in streams:
            if new_mapping.get(stream):
                sent_data = gs.read_snapshots(
                    f"{stream_name_mapping[stream]}_{flow_id}", SNAPSHOT_DIR
                )

                if stream != "contacts":
                    # map the data
                    stream_data = input.get(stream)
                    stream_columns, stream_data = map_stream_data(stream_data, stream, new_mapping)
                    # we only want the data that we have not been sent before
                    stream_data = drop_sent_records(stream, stream_data, sent_data)
                    # output the data
                    gs.to_singer(
                        stream_data,
                        stream_name_mapping[stream],
                        OUTPUT_DIR,
                        allow_objects=True,
                    )

                else:
                    # 1. get all accounts / filter: have a valid account id in the snapshot
                    sent_accounts = gs.read_snapshots(
                        f"{stream_name_mapping['accounts']}_{flow_id}", SNAPSHOT_DIR
                    )
                    if sent_accounts is None:
                        print("No accounts have been sent yet, skipping contacts export.")
                        continue

                    sent_accounts = sent_accounts.rename(columns={"InputId": "AccountId", "RemoteId": "RemoteAccountId"})
                    # clean accounts without RemoteAccountId
                    sent_accounts = sent_accounts[sent_accounts["RemoteAccountId"].notna()]
                    
                    contacts = input.get(stream)
                    leads = None
                    # if dynamic flag is on we need to divide contacts into two streams: contacts and leads
                    if connector_id == "salesforce" and tap_config.get("dynamic_contact_mapping"):
                        contacts, leads = split_contacts_by_account(contacts, sent_accounts)

                    for _stream in ["Contact", "Lead"]:
                        stream_data = get_contact_data(connector_id, _stream, contacts, leads)
                        if stream_data is None or stream_data.empty:
                            continue

                        mapping_name = "contacts" if connector_id == "hubspot" else {v: k for k, v in stream_name_mapping.items()}.get(_stream)

                        stream_columns, stream_data = map_stream_data(stream_data, mapping_name, new_mapping)
                        new_data = stream_data.copy()
                        # get all contacts
                        data_snap = gs.snapshot_records(
                            stream_data, mapping_name, SNAPSHOT_DIR, pk="externalId"
                        )
                        # we do not want the hash from the snapshot if it's there
                        if "hash" in data_snap.columns:
                            data_snap = data_snap.drop(columns=["hash"])
                        
                        if _stream != "Lead":
                            data_snap = data_snap.merge(
                                sent_accounts, on="AccountId" 
                            ).rename(columns={"AccountId": "InputAccountId", "RemoteAccountId": "AccountId"})
                        
                        if connector_id == "hubspot":
                            data_snap["AccountId"] = data_snap["AccountId"].apply(
                                lambda x: [
                                    {
                                        "to": {"id": x},
                                        "types": [
                                            {
                                                "associationCategory": "HUBSPOT_DEFINED",
                                                "associationTypeId": 279,
                                            }
                                        ],
                                    }
                                ]
                            )
                            data_snap = data_snap.rename(columns={"AccountId": "associations"})
                            stream_columns.remove("AccountId")
                            stream_columns.append("associations")
                        
                        # We can now use this as the final output data
                        stream_data = data_snap[list(set(stream_columns))]
                        # drop sent records
                        stream_data = drop_sent_records(stream, stream_data, sent_data, new_data)

                        gs.to_singer(
                            stream_data,
                            stream_name_mapping[mapping_name],
                            OUTPUT_DIR,
                            allow_objects=True,
                        )
            else:
                stream_data = input.get(stream)
                gs.to_singer(
                    stream_data,
                    stream_name_mapping[stream],
                    OUTPUT_DIR,
                    allow_objects=True,
                )

else:
    # get data from sync-output folder
    data_streams = ast.literal_eval(str(input))

    # connector -> target_api
    if data_streams and mapping is not None:
        new_mapping = {}

        # We need the mapping to be connector stream -> target_api stream
        for stream in mapping.keys():
            old_stream, new_stream = stream.split("/")
            new_mapping[new_stream] = mapping.get(stream)
            new_mapping[new_stream]["remote_id"] = "Id"

        for stream in data_streams:
            # read data
            stream_data = input.get(stream)

            # start processing data
            output_stream = None

            # See if there is a mapping for this connector stream
            if new_mapping.get(stream):
                connector_columns = list(new_mapping.get(stream).keys())
                target_api_columns = list(new_mapping.get(stream).values())
                columns_to_rename = [
                    column
                    for column in stream_data.columns
                    if column in target_api_columns
                ]
                gc = np.array(target_api_columns)

                # Rename columns to their target_api version
                for column in columns_to_rename:
                    # The below line gets all columns mapped to this column in case of multiple matches
                    for ind in np.where(gc == column)[0]:
                        stream_data[connector_columns[ind]] = stream_data[column]

                # Only retain columns that were mapped to target_api
                cols = [c for c in connector_columns if c in stream_data.columns] + ["remote_id"]
                stream_data = stream_data[list(set(cols))]
                # Output will be the target_api name, not the connector name
                output_stream = list(stream_name_mapping.keys())[
                    list(stream_name_mapping.values()).index(stream)
                ]

                # inject the CBX1 id if present
                sent_data = gs.read_snapshots(
                    f"{stream}_{flow_id}", SNAPSHOT_DIR
                )

                if sent_data is not None:
                    sent_data = sent_data.rename(columns={
                        "InputId": "id",
                        "RemoteId": "remote_id"
                    })

                    stream_data = stream_data.merge(sent_data, how="left", on="remote_id")

            # Inject the connector_id as externalIdSource
            stream_data["externalIdSource"] = connector_id
            # CBX1 expects the remote_id to be called externalId
            stream_data = stream_data.rename(columns={
                "remote_id": "externalId"
            })

            stream_name = output_stream or stream
            gs.to_singer(stream_data, stream_name, OUTPUT_DIR, allow_objects=True)
