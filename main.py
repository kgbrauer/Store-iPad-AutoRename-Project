### IMPORT LIBRARIES
from datetime import datetime, timedelta, timezone
import re
import logging
import sys

from jsonLogFormatter import JsonFormatter

# Set Logging Config. Logs structured JSON lines to stdout only -- no local
# file, no direct network calls. On Kubernetes, the container runtime
# captures stdout and a cluster-level log forwarder (e.g. New Relic's
# Kubernetes integration) ships it onward. See jsonLogFormatter.py.
stdoutHandler = logging.StreamHandler(sys.stdout)
stdoutHandler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[stdoutHandler])
logger = logging.getLogger(__name__)

# Import MS Graph and Solar Winds/SWIS modules
from msGraph import *
from orionAPI import *

# Initialize msGraph and orionAPI/SWIS Client
initializeGraph()
initializeSWIS()

# Set filterLastSync to last 7 days. Can be changed as desired.
syncDate = (
    datetime.now(timezone.utc) - timedelta(days=7)
).strftime("%Y-%m-%dT%H:%M:%SZ")

# Intune Managed Devices Get Request
logger.info("QUERYING INTUNE FOR STORE IPADS...")
devicesGetResult = getManagedDevices(filterOS="iOS", filterName=None, filterLastSync=syncDate, filterSelect="serialNumber,deviceName,operatingSystem,WiFiMacAddress,id,lastSyncDateTime", filterTop="25")

# Output total Intune Managed Devices found in query
logger.info(str(len(devicesGetResult))+" total Store iPad devices found.")

## Define Function: Query SolarWinds via SWIS with MacAddress to pull store # from connected Firewall node caption. Combine Store # and Serial # to create dDeviceNewName
# Use Intune BetaManagedDevices Endpoint to send POST call for device rename along with check-in sync
def renameIpadSWISIntune(dDeviceName, dMacAddress, dSerialNumber, dDeviceId):
    swisIpadMacStore = swisIpadMacQuery(dMacAddress)
    if swisIpadMacStore == "None" or not swisIpadMacStore:
        logger.error(f"Device '{dDeviceName}' ({dSerialNumber}) - Error: No SWIS Entry found.")
        return "swis_fail"
    else:
        dDeviceNewName = "USA" + swisIpadMacStore + "-" + dSerialNumber
        logger.info(f"Device '{dDeviceName}' ({dSerialNumber}) to be renamed to: '{dDeviceNewName}'")
        postResult = postBetaManagedDevice(deviceId=dDeviceId, deviceSerialNumber=dSerialNumber, targetProperty="deviceName", deviceProperty=dDeviceNewName)
        if postResult:
            return "rename_success"
             
# Set variable for expected iPad naming convention
namingConventionPattern = re.compile(r"USA0[0-9]{4}-[A-Za-z0-9]+")

# Set variable for counting of misnamed devices
misnamedDevices = 0

# Set variable for counting of successful Intune renames
renameSuccesses = 0

# Set variable for counting of SWIS failures
swisFailures = 0

# Check all queried devices for naming issues, processing misnamed devices with renameIpadSWISSIntune function defined above
logger.info("PARSING STORE IPADS FOR MISNAMED DEVICES TO PROCESS...")
for d in devicesGetResult:
    if d.deviceName.startswith("LOST"):
        continue
    if not namingConventionPattern.fullmatch(d.deviceName):
        misnamedDevices += 1
        logger.info(f"Device '{d.deviceName}' ({d.serialNumber}) is misnamed (naming convention). Rename needed.")
        renameResult = renameIpadSWISIntune(dDeviceName=d.deviceName, dMacAddress=d.wiFiMacAddress, dSerialNumber=d.serialNumber, dDeviceId=d.id)
        if renameResult == "rename_success":
            renameSuccesses += 1
        if renameResult == "swis_fail":
            swisFailures += 1
        continue
    if not d.deviceName.endswith(d.serialNumber.upper()):
        misnamedDevices += 1
        logger.info(f"Device '{d.deviceName}' ({d.serialNumber}) is misnamed (serial number mismatch). Rename needed.")
        renameResult = renameIpadSWISIntune(dDeviceName=d.deviceName, dMacAddress=d.wiFiMacAddress, dSerialNumber=d.serialNumber, dDeviceId=d.id)
        if renameResult == "rename_success":
            renameSuccesses += 1
        if renameResult == "swis_fail":
            swisFailures += 1

# Set and calculate variable for misnamed devices that failed to rename
renameFailures = misnamedDevices - renameSuccesses

# Output results
logger.info("")
logger.info("RESULTS")
logger.info(f"{str(misnamedDevices)} misnamed devices found.")
logger.info(f"{str(renameSuccesses)} misnamed devices successfully renamed.")
logger.info(f"{str(renameFailures)} misnamed devices failed to rename, {str(swisFailures)} due to no SWIS entry found.")
logger.info("=-=" * 25)
logger.info("")
