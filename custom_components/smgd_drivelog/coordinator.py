"""Hämtar nyckeltal från ``GET /api/v1/ha/summary`` till sensorerna."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import DriveLogApiClient, DriveLogAuthError, DriveLogError
from .const import DOMAIN, SUMMARY_INTERVAL

_LOGGER = logging.getLogger(__name__)


class DriveLogCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, client: DriveLogApiClient) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=SUMMARY_INTERVAL,
        )
        self.client = client

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            data = await self.client.summary()
        except DriveLogAuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err
        except DriveLogError as err:
            raise UpdateFailed(str(err)) from err
        if not isinstance(data, dict):
            raise UpdateFailed("Oväntat svar från SMGD DriveLog.")
        return data
