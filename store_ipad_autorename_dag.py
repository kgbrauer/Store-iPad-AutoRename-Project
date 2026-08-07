"""
Airflow DAG: Store iPad Auto-Rename Tool

Runs the existing Store iPad AutoRename scripts (main.py / msGraph.py / orionAPI.py)
on a daily schedule. Credentials for MS Graph (Intune) and SolarWinds SWIS are
pulled from Airflow Connections at task-execution time (via Jinja `conn.<conn_id>`
templating) and passed into the script as environment variables -- they are never
written to msGraphConfig.json / orionAPIConfig.json on disk. This requires the
env-var-aware config loaders added to msGraph.py and orionAPI.py (loadMsGraphConfig()
/ loadOrionConfig()), which fall back to the JSON files only when an env var isn't set.

Required Airflow Connections (create via UI: Admin > Connections, or `airflow
connections add`):

  msgraph_api   (type: HTTP or Generic)
    - login:  Azure App Registration client_id
    - password: Azure App Registration client secret
    - extra (JSON): {
        "authority": "https://login.microsoftonline.com/<tenant-id>",
        "scope": "https://graph.microsoft.com/.default",
        "endpoint": "https://graph.microsoft.com/v1.0",
        "beta_endpoint": "https://graph.microsoft.com/beta"
      }

  solarwinds_swis   (type: HTTP or Generic)
    - host:     SolarWinds SWIS host (e.g. da2hqpnm32.babgsetc.pvt)
    - login:    SWIS API username
    - password: SWIS API password

Logging: main.py logs structured JSON lines to stdout (see jsonLogFormatter.py)
instead of a local file or a direct New Relic API call. On Kubernetes this is
the standard pattern -- BashOperator streams the subprocess's stdout/stderr
into the task's own log output, which lands in the worker/task pod's container
logs. A cluster-level log forwarder (e.g. New Relic's Kubernetes integration,
Fluent Bit, etc. -- whatever DaemonSet your cluster runs) tails those container
logs and ships them onward; the script itself makes no outbound logging calls
and needs no logging-related credentials.

Prerequisites on the Airflow worker:
  - Airflow 2.6+ (BashOperator's `append_env` param is used below so the
    injected secrets are added to, not a replacement of, the task's environment)
  - Python 3.8+ with packages: msal, requests, orionsdk, urllib3
  - PROJECT_DIR (below) points to the directory containing main.py, msGraph.py,
    orionAPI.py, jsonLogFormatter.py, msGraphConfig.json, and orionAPIConfig.json
  - Network access from the worker to Microsoft Graph API and the SolarWinds
    Orion server
  - The msgraph_api and solarwinds_swis connections above are configured in
    Airflow (UI, CLI, or a secrets backend such as AWS Secrets Manager /
    HashiCorp Vault / Azure Key Vault)
  - Your cluster's log collection is configured to tail this pod's/container's
    stdout if you want these JSON log lines to reach New Relic (or wherever
    else logs are centralized) -- that forwarding is a cluster-level concern,
    not something this DAG or script configures
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Absolute path to the directory containing main.py, msGraph.py, orionAPI.py,
# and both config JSON files on the Airflow worker's filesystem. On the
# AirflowDev podman-compose stack this is bind-mounted via STORE_IPAD_PROJ_DIR
# (see .env / docker-compose.yaml) to /opt/airflow/dags/store_ipad_autorename.
PROJECT_DIR = "/opt/airflow/dags/store_ipad_autorename"

default_args = {
    "owner": "store-systems",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="store_ipad_autorename",
    description="Identifies and renames misnamed store iPads via Intune (MS Graph) + SolarWinds SWIS lookups.",
    default_args=default_args,
    schedule="0 6 * * *",  # daily at 06:00
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["intune", "solarwinds", "store-ipads"],
    doc_md=__doc__,
) as dag:
    run_autorename = BashOperator(
        task_id="run_autorename_script",
        bash_command=f"cd {PROJECT_DIR} && python main.py",
        # `env` is a templated field on BashOperator, so these `conn.<conn_id>`
        # references are resolved at task-execution time (not at DAG-parse
        # time), pulling live from Airflow's encrypted connection store /
        # configured secrets backend.
        env={
            "MSGRAPH_CLIENT_ID": "{{ conn.msgraph_api.login }}",
            "MSGRAPH_CLIENT_SECRET": "{{ conn.msgraph_api.password }}",
            "MSGRAPH_AUTHORITY": "{{ conn.msgraph_api.extra_dejson.authority }}",
            "MSGRAPH_SCOPE": "{{ conn.msgraph_api.extra_dejson.scope }}",
            "MSGRAPH_ENDPOINT": "{{ conn.msgraph_api.extra_dejson.endpoint }}",
            "MSGRAPH_BETA_ENDPOINT": "{{ conn.msgraph_api.extra_dejson.beta_endpoint }}",
            "ORION_HOST": "{{ conn.solarwinds_swis.host }}",
            "ORION_USERNAME": "{{ conn.solarwinds_swis.login }}",
            "ORION_PASSWORD": "{{ conn.solarwinds_swis.password }}",
        },
        # Merge the above into the task's existing environment (PATH, etc.)
        # rather than replacing it. Requires Airflow 2.6+; on older versions,
        # remove this line and instead include `**os.environ` when building
        # the env dict above.
        append_env=True,
        doc_md=(
            "Runs `python main.py`, which:\n"
            "1. Authenticates to MS Graph (msal) and SolarWinds SWIS using "
            "credentials injected from the `msgraph_api` and `solarwinds_swis` "
            "Airflow Connections above.\n"
            "2. Queries Intune for iOS devices synced in the last 7 days.\n"
            "3. Flags devices not matching `USA0[0-9]{4}-<SerialNumber>` "
            "or with a serial mismatch (skipping any named `LOST*`).\n"
            "4. Looks up each device's store number in SolarWinds via its "
            "Wi-Fi MAC address.\n"
            "5. Renames the device in Intune (POST setDeviceName, beta) and "
            "triggers a syncDevice call.\n"
            "6. Logs structured JSON lines to stdout only -- no local log "
            "file and no direct outbound logging call; a cluster-level log "
            "forwarder is responsible for shipping stdout onward."
        ),
    )

    run_autorename
