"""Diagnostik för felsökning – utan hemligheter och utan svarstexter."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from . import DriveLogConfigEntry
from .const import CONF_TOKEN, CONF_URL

TO_REDACT = {CONF_TOKEN, CONF_URL}


async def async_get_config_entry_diagnostics(hass: HomeAssistant, entry: DriveLogConfigEntry) -> dict[str, Any]:
    data = entry.runtime_data
    summary = dict(data.coordinator.data or {})
    summary.pop("last_trip", None)  # innehåller platsetiketter
    return {
        "entry": {
            "data": async_redact_data(dict(entry.data), TO_REDACT),
            "options": dict(entry.options),
        },
        "coordinator": {
            "last_update_success": data.coordinator.last_update_success,
            "summary": summary,
        },
        "manager": data.manager.diagnostics(),
    }
