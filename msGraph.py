### IMPORT LIBRARIES
import json
import os
import requests
import msal
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import logging

# Set Logger Config
logger = logging.getLogger(__name__)

### MS GRAPH BASE ITEMS

## Load MS Graph config
# Reads msGraphConfig.json (if present) as a base/local-dev default, then lets
# environment variables override each value. This allows a caller (e.g. an
# Airflow DAG pulling from an Airflow Connection) to inject the real client_id/
# secret/authority/scope at runtime without those values ever needing to live
# in the JSON file.
# Env vars: MSGRAPH_CLIENT_ID, MSGRAPH_CLIENT_SECRET, MSGRAPH_AUTHORITY,
#           MSGRAPH_SCOPE (comma-separated), MSGRAPH_ENDPOINT, MSGRAPH_BETA_ENDPOINT
def loadMsGraphConfig(configPath: str = "msGraphConfig.json") -> Dict[str, Any]:
    fileConfig: Dict[str, Any] = {}
    if os.path.exists(configPath):
        with open(configPath, "r") as configFile:
            fileConfig = json.load(configFile)

    envScope = os.environ.get("MSGRAPH_SCOPE")

    resolvedConfig = {
        "authority": os.environ.get("MSGRAPH_AUTHORITY", fileConfig.get("authority")),
        "client_id": os.environ.get("MSGRAPH_CLIENT_ID", fileConfig.get("client_id")),
        "secret": os.environ.get("MSGRAPH_CLIENT_SECRET", fileConfig.get("secret")),
        "scope": envScope.split(",") if envScope else fileConfig.get("scope"),
        "endpoint": os.environ.get("MSGRAPH_ENDPOINT", fileConfig.get("endpoint", "https://graph.microsoft.com/v1.0")),
        "beta_endpoint": os.environ.get("MSGRAPH_BETA_ENDPOINT", fileConfig.get("beta_endpoint", "https://graph.microsoft.com/beta")),
    }

    missing = [k for k in ("authority", "client_id", "secret", "scope") if not resolvedConfig.get(k)]
    if missing:
        raise ValueError(
            f"Missing required MS Graph config values: {', '.join(missing)}. "
            f"Set them in {configPath} or via MSGRAPH_* environment variables."
        )
    return resolvedConfig

config = loadMsGraphConfig()

# Get a token from MSAL
def getMsalToken():
    logger.info("Requesting MS Graph access token")
    try:
        tokenApp = msal.ConfidentialClientApplication(
            config["client_id"], authority=config["authority"],
            client_credential=config["secret"],
            )
        tokenResult = None
        tokenResult = tokenApp.acquire_token_silent(config["scope"], account=None)
        if not tokenResult:
            logger.info("No suitable token exists in cache. Let's get a new one from AAD.")
            tokenResult = tokenApp.acquire_token_for_client(scopes=config["scope"])
        if not tokenResult or "access_token" not in tokenResult:
            logger.error(
                f"Token acquisition failed: "
                f"{tokenResult.get('error')} - "
                f"{tokenResult.get('error_description')}"
                )
            raise Exception("Failed to acquire MS Graph access token")
        logger.info("Successfully acquired MS Graph access token")
        return tokenResult
    except Exception as e:
        logger.error(f"Failed to acquire MS Graph token: {e}")
        raise

# Initialize Graph Function
def initializeGraph():
    global msalToken
    msalToken = getMsalToken()
    logger.info("MS Graph authentication complete")

## Build the MS Graph Request with endpoint suffix and optional Params
# Inputs: "rMethod" ("get" or "postbeta")
# Inputs: "endpointSuffix" (request endpoint suffix to follow "https://graph.microsoft.com/v1.0" for GET or "https://graph.microsoft.com/beta" for POSTbeta)
# Inputs: "rParams" ({} dict with MS Graph specific key/value pairs such as "$filter", "$select", and "$top")
# Outputs: JSON blob for GET requests, response with .status_code attribute for POST requests
# Expandability: An additional ELIF can be added if non-beta POST is required or postbeta can be altered if current beta only endpoints/properties are migrated off beta.
def buildMsalRequest(rMethod, endpointSuffix, rParams=None):
    try:
        if rParams is None:
            rParams = {}
        rMethod = rMethod.lower()
        base = config["endpoint"]
        betaBase = config["beta_endpoint"]
        fullEndpoint = f"{base}{endpointSuffix}"
        fullBetaEndpoint = f"{betaBase}{endpointSuffix}"
        if rMethod == "get":
            response = requests.get(
                fullEndpoint,
                headers={'Authorization': 'Bearer ' + msalToken['access_token'], "Content-Type": "application/json"},
                params=rParams,
                timeout=60
                )
            response.raise_for_status()
            graphData = response.json()
        elif rMethod == "postbeta":
            response = requests.post(
                fullBetaEndpoint,
                headers={'Authorization': 'Bearer ' + msalToken['access_token'], "Content-Type": "application/json"},
                json=rParams,
                timeout=60
                )
            response.raise_for_status()
            graphData = response
        else:
            logger.error(f"Unsupported request method: {rMethod}")
            raise ValueError(f"Unsupported request method: {rMethod}")
        return graphData
    except Exception as e:
        logger.error(f"Graph {rMethod.upper()} request failed: {endpointSuffix} - {e}")
        raise

### MANAGED DEVICES

# Endpoint Suffix
managedDevicesEndpoint = "/deviceManagement/managedDevices"

## Device data class and parsing function
# Inputs: "graph_json" (JSON from MS Graph GET request on managed devices endpoint)
# Outputs: Class set parsed from input
# Expandability: Options to add additional properties to Class as needed
@dataclass
class Device:
    serialNumber: str
    deviceName: Optional[str] = None
    operatingSystem: Optional[str] = None
    id: Optional[str] = None
    wiFiMacAddress: Optional[str] = None
    lastSyncDateTime: Optional[str] = None
    deviceNewName: Optional[str] = None

    @classmethod
    def fromDict(cls, d: Dict[str, Any]) -> "Device":
        return cls(
            serialNumber = d.get("serialNumber", ""),
            deviceName = d.get("deviceName"),
            operatingSystem = d.get("operatingSystem"),
            id = d.get("id"),
            wiFiMacAddress = d.get("wiFiMacAddress"),
            lastSyncDateTime = d.get("lastSyncDateTime"),
        )
def parseDevicesGetResult(graph_json: Dict[str, Any]) -> List[Device]:
    items = graph_json.get("value", [])
    return [Device.fromDict(it) for it in items]

## Function to get managed devices with optional filters and handling for paging
# Inputs: "filterOS" (str, ex: "iOS"), "filterName" (str, processed as a contains function)
# Inputs: "filterLastSync" (str, filters all devices with last sync timestamps after input in format "%Y-%m-%dT%H:%M:%SZ")
# Inputs: "filterSelect" (str, properties to pull in results, ex: "serialNumber,deviceName,operatingSystem")
# Inputs: "filterTop" (str or int, number of devices to pull from filtered query)
# Output: JSON blob from post request response
# Expandability: Options to add further filters/params as needed. Consult Graph documentation for additional filters/params.
def getManagedDevices(filterOS=None, filterName=None, filterLastSync=None, filterSelect=None, filterTop=None):
    try:
        logger.info("Retrieving managed devices from Intune")
        mdParams = {}
        filters = []
        if filterOS:
            filters.append(f"operatingSystem eq '{filterOS}'")
        if filterName:
            filters.append(f"contains(deviceName,'{filterName}')")
        if filterLastSync:
            filters.append(f"lastSyncDateTime ge {filterLastSync}")
        if filters:
            mdParams["$filter"] = " and ".join(filters)
        if filterSelect:
            mdParams["$select"] = filterSelect
        if filterTop:
            try:
                filterTop = int(filterTop)
            except (TypeError, ValueError):
                logger.warning(f"Invalid filterTop value '{filterTop}'. Ignoring.")
                filterTop = None
        if filterTop:
            mdParams["$top"] = str(filterTop)
        graphResult = buildMsalRequest("get", managedDevicesEndpoint, mdParams)
        allDevices = []
        maxResults = filterTop
        pageCount = 0
        while True:
            pageCount += 1
            logger.info(f"Retrieving managed devices page {pageCount}")
            allDevices.extend(graphResult["value"])
            if maxResults and len(allDevices) >= maxResults:
                allDevices = allDevices[:maxResults]
                break
            nextLink = graphResult.get("@odata.nextLink")
            if not nextLink:
                break
            response = requests.get(
                nextLink,
                headers={
                    "Authorization": f"Bearer {msalToken['access_token']}"
                },
                timeout=60
            )
            response.raise_for_status()
            graphResult = response.json()
        graphResult["value"] = allDevices
        return parseDevicesGetResult(graphResult)
    except Exception as e:
        logger.error(f"Failed to retrieve managed devices: {e}")
        raise

# Example Graph API calls
#devicesGetResult = getManagedDevices(filterOS="iOS", filterName="USA01691", filterSelect="serialNumber,deviceName,operatingSystem,WiFiMacAddress,id,lastSyncDateTime", filterTop=2)
#devicesPostResult = postBetaManagedDevice(deviceId=d.id, targetProperty="deviceName", deviceProperty=d.deviceNewName)

## Function to update/trigger (via post) a device property/action that is Read Only in v1.0 but can be accessed in beta
# Inputs: "deviceID" (str, Azure device ID, alphanumeric in a 8-4-4-4-* format)
# Inputs: "deviceSerialNumber" (str), "targetProperty" (str, Azure property to be set/changed), "deviceProperty" (str, value to set "targetProperty" to)
# Output: boolean (True if post request is successful, False if not)
# Expandability: Currently only configured for function "setDeviceName". Can add functionality for other parameter updates. Consult Graph documentation for additional options.
def postBetaManagedDevice(deviceId: str, deviceSerialNumber: str, targetProperty: str, deviceProperty=None):
    try:
        functionSuffix = None
        if targetProperty == "deviceName":
            mdParams = {
                "deviceName": deviceProperty
            }
            functionSuffix = "setDeviceName"
        else:
            logger.error(f"Device '{deviceSerialNumber}': Unsupported target property: {targetProperty}")
            raise ValueError(f"Device '{deviceSerialNumber}': Unsupported target property: {targetProperty}")
        logger.info(f"Device '{deviceSerialNumber}': Attempting to update {targetProperty} to {deviceProperty}")
        postResult = buildMsalRequest("postBeta", f"{managedDevicesEndpoint}/{deviceId}/{functionSuffix}", mdParams)
        if postResult.status_code == 204:
                logger.info(f"Device '{deviceSerialNumber}': Successfully updated {targetProperty} to {deviceProperty}. Initiating sync.")
                syncResult = buildMsalRequest("postBeta", f"{managedDevicesEndpoint}/{deviceId}/syncDevice", rParams=None)
                if syncResult.status_code == 204:
                    logger.info(f"Device '{deviceSerialNumber}': Successfully initiated sync.")
                else:
                    try:
                        logger.error(f"Device '{deviceSerialNumber}': Error syncing device - Status: {syncResult.status_code}")
                        logger.error(f"Device '{deviceSerialNumber}': Sync Error:{syncResult.json()}")
                    except ValueError:
                        logger.error("No JSON response")
                return True
        else:
            try:
                logger.error(f"Device '{deviceSerialNumber}': Error renaming device - Status: {postResult.status_code}")
                logger.error(f"Device '{deviceSerialNumber}': Error Response:{postResult.json()}")
            except ValueError:
                logger.error("No JSON response")
            return False
    except Exception as e:
        logger.error(f"Device '{deviceSerialNumber}': Failed to update device: {e}")
        raise