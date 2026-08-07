### IMPORT LIBRARIES
from orionsdk import SwisClient
import requests
import sys
import os
import json
import urllib3
import logging
from typing import Any, Dict

# Set Logger Config
logger = logging.getLogger(__name__)

# Disable cert warnings
urllib3.disable_warnings()

### ORIONSDK BASE ITEMS

## Load SolarWinds/SWIS config
# Reads orionAPIConfig.json (if present) as a base/local-dev default, then lets
# environment variables override each value. This allows a caller (e.g. an
# Airflow DAG pulling from an Airflow Connection) to inject the real host/
# username/password at runtime without those values ever needing to live in
# the JSON file.
# Env vars: ORION_HOST, ORION_USERNAME, ORION_PASSWORD
def loadOrionConfig(configPath: str = "orionAPIConfig.json") -> Dict[str, Any]:
    fileConfig: Dict[str, Any] = {}
    if os.path.exists(configPath):
        with open(configPath, "r") as configFile:
            fileConfig = json.load(configFile)

    resolvedConfig = {
        "HOST": os.environ.get("ORION_HOST", fileConfig.get("HOST")),
        "USERNAME": os.environ.get("ORION_USERNAME", fileConfig.get("USERNAME")),
        "PASSWORD": os.environ.get("ORION_PASSWORD", fileConfig.get("PASSWORD")),
    }

    missing = [k for k in ("HOST", "USERNAME", "PASSWORD") if not resolvedConfig.get(k)]
    if missing:
        raise ValueError(
            f"Missing required SolarWinds config values: {', '.join(missing)}. "
            f"Set them in {configPath} or via ORION_* environment variables."
        )
    return resolvedConfig

config = loadOrionConfig()

# Connect to the SolarWinds API using the SwisClient
def connectSwisClient():
    logger.info("Connecting to SolarWinds API")
    try:
        swis = SwisClient(config["HOST"], config["USERNAME"], config["PASSWORD"], verify=False)
        return swis
    except Exception as e:
        logger.error(f"Error connecting to SolarWinds API: {e}")
        sys.exit(1)

# Initialize SWIS Client Function
def initializeSWIS():
    global swis
    swis = connectSwisClient()
    logger.info("SWIS authentication complete")

## Build the SWIS Request with query
# Inputs: "query" (SQL query), "params" (if needed for query type)
# Output: Results of SQL query passed (Set [] with "Results" that contains rows)
def buildSwisRequest(query, params=None):
    try:
        if params:
            swisData = swis.query(query, **params)
            return swisData
        else:
            return swis.query(query)
    except Exception as e:
        logger.error(f"Error executing SWIS query: {e}")
        sys.exit(1)


## Build SWIS request to pull store # associated with the firewall node connected to a device Mac Address
# Inputs: "mac_input" (MAC Address, no colons)
# Output: 5 digit store # (0 + 4 digit store number)=, ex: "01691")
def swisIpadMacQuery(mac_input):
    mQuery = f"""
    SELECT TOP 1 n.Caption
    FROM Orion.NodeL3Entries l
    JOIN Orion.Nodes n ON l.NodeID = n.NodeID
    WHERE l.MACAddress = '{mac_input}'
    """
    mData = buildSwisRequest(mQuery)
    if mData and "results" in mData and len(mData["results"]) > 0:
        for row in mData["results"]:
            for k, v in row.items():
                value = v[6:11:1]
        logger.info(f"MAC Address {mac_input} found connected to FW Node USA{value}")
        return value
    else:
        logger.error(f"MAC Address {mac_input}: SWIS query failed for device MAC Address")