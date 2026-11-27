"""SMGD DriveLog – interaktiva körjournalsnotiser och sensorer i Home Assistant.

All trafik initieras från Home Assistant: integrationen hämtar notiser (pull-läget),
skickar svar och livstecken till DriveLog. Ingen publik webhook i HA behövs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import voluptuous as vol
from homeassistant.components.http import StaticPathConfig
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_state_change_event, async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.helpers.typing import ConfigType

from .api import DriveLogApiClient, DriveLogAuthError, DriveLogError
from .const import (
    CONF_TOKEN,
    CONF_URL,
    DEFAULT_POLL_INTERVAL,
    DOMAIN,
    EVENT_NOTIFICATION_ACTION,
    HEARTBEAT_INTERVAL,
    PRICE_INTERVAL,
    ROUTE_INTERVAL,
    SERVICE_FLUSH_QUEUE,
    STORAGE_VERSION,
)
from .coordinator import DriveLogCoordinator
from .core import BRAND_ICON_URL, ISSUE_NO_PHONE, is_drivelog_action
from .manager import DriveLogManager

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR]
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


@dataclass
class DriveLogData:
    client: DriveLogApiClient
    coordinator: DriveLogCoordinator
    manager: DriveLogManager


type DriveLogConfigEntry = ConfigEntry[DriveLogData]


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Registrerar tjänsten smgd_drivelog.flush_queue (en gång för alla poster)."""

    async def _flush(call: ServiceCall) -> None:
        for entry in hass.config_entries.async_entries(DOMAIN):
            data = getattr(entry, "runtime_data", None)
            if entry.state is ConfigEntryState.LOADED and isinstance(data, DriveLogData):
                await data.manager.async_flush_queue(force=True)
                await data.manager.async_poll_outbox()

    hass.services.async_register(DOMAIN, SERVICE_FLUSH_QUEUE, _flush, schema=vol.Schema({}))
    # Loggan i notiserna: Companion-appen hämtar den härifrån (relativ adress, ingen inloggning).
    icon = Path(__file__).parent / "brand" / "icon@2x.png"
    try:
        await hass.http.async_register_static_paths([StaticPathConfig(BRAND_ICON_URL, str(icon), True)])
    except RuntimeError:  # redan registrerad (t.ex. efter omladdning)
        _LOGGER.debug("Loggans adress %s finns redan", BRAND_ICON_URL)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: DriveLogConfigEntry) -> bool:
    session = async_get_clientsession(hass)
    client = DriveLogApiClient(session, entry.data[CONF_URL], entry.data[CONF_TOKEN])
    try:
        await client.whoami()
    except DriveLogAuthError as err:
        raise ConfigEntryAuthFailed(str(err)) from err
    except DriveLogError as err:
        # Uppsättningen fortsätter ändå: lyssnaren på knapptryck måste finnas även när
        # DriveLog inte nås, annars tappas svar. De köas på disk och skickas senare.
        _LOGGER.warning("SMGD DriveLog nås inte just nu (%s). Svar köas och skickas när anslutningen fungerar.", err)

    coordinator = DriveLogCoordinator(hass, entry, client)
    manager = DriveLogManager(hass, entry, client)
    # Sensorerna följer DriveLog: när utkorgen (var 30:e sekund) visar att något har ändrats hämtas nyckeltalen direkt.
    manager.on_summary_changed = coordinator.async_request_refresh
    await manager.async_load()
    entry.runtime_data = DriveLogData(client=client, coordinator=coordinator, manager=manager)

    @callback
    def _on_action(event) -> None:
        manager.async_handle_event(event)

    entry.async_on_unload(
        hass.bus.async_listen(
            EVENT_NOTIFICATION_ACTION,
            _on_action,
            event_filter=callback(lambda data: is_drivelog_action(data)),
        )
    )
    # Sensorerna blir otillgängliga tills DriveLog nås; koordinatorn försöker igen enligt intervallet.
    await coordinator.async_refresh()

    await manager.async_heartbeat()
    interval = timedelta(seconds=manager.poll_interval) if manager.poll_interval else DEFAULT_POLL_INTERVAL
    entry.async_on_unload(async_track_time_interval(hass, manager.async_tick, interval, name=f"{DOMAIN} utkorg"))
    entry.async_on_unload(
        async_track_time_interval(hass, _heartbeat_job(manager), HEARTBEAT_INTERVAL, name=f"{DOMAIN} livstecken")
    )
    entry.async_on_unload(
        async_track_time_interval(hass, manager.async_sync_routes, ROUTE_INTERVAL, name=f"{DOMAIN} resväg")
    )
    entry.async_on_unload(
        async_track_time_interval(
            hass,
            manager.async_request_location,
            timedelta(seconds=manager.location_interval),
            name=f"{DOMAIN} position under färd",
        )
    )
    # 1.14.0: elpriset när sensorn ändras och minst var 15:e minut.
    entry.async_on_unload(
        async_track_time_interval(hass, manager.async_send_price, PRICE_INTERVAL, name=f"{DOMAIN} elpris")
    )
    if (price_entity := manager.price_entity()) is not None:

        @callback
        def _on_price(_event) -> None:
            entry.async_create_background_task(hass, manager.async_send_price(), name=f"{DOMAIN} elpris ändrat")

        entry.async_on_unload(async_track_state_change_event(hass, [price_entity], _on_price))
        entry.async_create_background_task(hass, manager.async_send_price(), name=f"{DOMAIN} första elpris")
    entry.async_create_background_task(hass, manager.async_tick(), name=f"{DOMAIN} första hämtning")
    entry.async_create_background_task(hass, manager.async_sync_routes(), name=f"{DOMAIN} första resväg")
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


def _heartbeat_job(manager: DriveLogManager):
    async def _run(_now) -> None:
        await manager.async_heartbeat()

    return _run


async def _async_update_listener(hass: HomeAssistant, entry: DriveLogConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: DriveLogConfigEntry) -> bool:
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def async_remove_entry(hass: HomeAssistant, entry: DriveLogConfigEntry) -> None:
    """Tar bort den sparade svarskön och reparationsärendet när integrationen tas bort."""
    ir.async_delete_issue(hass, DOMAIN, f"{ISSUE_NO_PHONE}_{entry.entry_id}")
    await Store(hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}.responses").async_remove()
