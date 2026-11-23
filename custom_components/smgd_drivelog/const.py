"""Konstanter för SMGD DriveLog-integrationen."""

from __future__ import annotations

from datetime import timedelta
from typing import Final

DOMAIN: Final = "smgd_drivelog"
NAME: Final = "SMGD DriveLog"
INTEGRATION_VERSION: Final = "1.12.0"

CONF_URL: Final = "url"
CONF_TOKEN: Final = "token"
CONF_NOTIFY_SERVICE: Final = "notify_service"
CONF_SEND_FEEDBACK: Final = "send_feedback"
CONF_COLLECT_ROUTE: Final = "collect_route"  # 1.8.0: telefonens resväg under färd
CONF_LOCATION_INTERVAL: Final = "location_interval"  # 1.8.2: sekunder mellan positionsbegäran under färd
CONF_ROUTE_ENTITY: Final = "route_entity"  # device_tracker; tomt = telefonen som får notiserna

# Händelsen som Companion-appen skickar när användaren trycker på en notisknapp.
EVENT_NOTIFICATION_ACTION: Final = "mobile_app_notification_action"

SERVICE_FLUSH_QUEUE: Final = "flush_queue"

SUMMARY_INTERVAL: Final = timedelta(minutes=5)
DEFAULT_POLL_INTERVAL: Final = timedelta(seconds=30)  # utkorg och svarskö
HEARTBEAT_INTERVAL: Final = timedelta(minutes=5)
ROUTE_INTERVAL: Final = timedelta(minutes=5)
DEFAULT_LOCATION_INTERVAL: Final = 10  # sekunder; Simons val 2026-11-22
MIN_LOCATION_INTERVAL: Final = 10
MAX_LOCATION_INTERVAL: Final = 300
# Utan ett färskt besked från DriveLog om att bilen körs slutar begäran efter så här lång tid.
TRACKING_STALE_AFTER: Final = timedelta(minutes=2)  # resväg: hämta förfrågningar och skicka punkter

STORAGE_VERSION: Final = 1
