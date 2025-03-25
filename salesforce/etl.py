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
                stream_mapping = new_mapping.get(stream)
                connector_columns = list(stream_mapping.keys())
                stream_data = input.get(stream)

                if not new_mapping.get(stream).get("remote_id"):
                    new_mapping[stream]["remote_id"] = "Id"

                target_api_columns = list(new_mapping.get(stream).keys())
                connector_columns = list(new_mapping.get(stream).values())

                columns_to_rename = [
                    column
                    for column in stream_data.columns
                    if column in target_api_columns
                ]
                for column in columns_to_rename:
                    try:
                        stream_data[
                            connector_columns[target_api_columns.index(column)]
                        ] = stream_data[column]
                    except:
                        print("Error renaming columns")

                stream_columns = [
                    col for col in connector_columns if col in stream_data.columns
                ]

                # Map id => external_id
                if "id" in stream_data.columns:
                    stream_columns = stream_columns + ["externalId"]
                    stream_data["externalId"] = stream_data["id"]

                stream_data = stream_data[list(set(stream_columns))]

                if stream == "contacts":
                    # determine which contacts have been updated since last job
                    new_contacts = stream_data.copy()
                    # get all contacts
                    contacts_snap = gs.snapshot_records(
                        stream_data, "contacts", SNAPSHOT_DIR, pk="externalId"
                    )
                    # we do not want the hash from the snapshot if it's there
                    if "hash" in contacts_snap.columns:
                        contacts_snap = contacts_snap.drop(columns=["hash"])

                    # filter: have a valid account id in the snapshot
                    sent_accounts = gs.read_snapshots(
                        f"{stream_name_mapping['accounts']}_{flow_id}", SNAPSHOT_DIR
                    )
                    if sent_accounts is None:
                        print("No accounts have been sent yet, skipping contacts export.")
                        continue

                    sent_accounts = sent_accounts.rename(columns={"InputId": "AccountId", "RemoteId": "RemoteAccountId"})
                    # doing an inner join intentionally here
                    contacts_snap = contacts_snap.merge(
                        sent_accounts, on="AccountId" 
                    ).rename(columns={"AccountId": "InputAccountId", "RemoteAccountId": "AccountId"})

                    # if this is hubspot we need to inject an turn our AccountId column into an associations array
                    if connector_id == "hubspot":
                        contacts_snap["AccountId"] = contacts_snap["AccountId"].apply(
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
                        contacts_snap = contacts_snap.rename(columns={"AccountId": "associations"})
                        stream_columns.remove("AccountId")
                        stream_columns.append("associations")

                    # We can now use this as the final output data
                    stream_data = contacts_snap[list(set(stream_columns))]

                # we only want the data that we have not been sent before
                sent_data = gs.read_snapshots(
                    f"{stream_name_mapping[stream]}_{flow_id}", SNAPSHOT_DIR
                )
                if sent_data is not None:
                    condition = ~stream_data["externalId"].isin(sent_data["InputId"])

                    # We want to send contacts that have not been sent before OR have any updates
                    if stream == "contacts":
                        condition = condition | (stream_data["externalId"].isin(new_contacts["externalId"]))

                    stream_data = stream_data[condition]

                gs.to_singer(
                    stream_data,
                    stream_name_mapping[stream],
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
