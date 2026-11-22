"""Svarskö, pull-leverans av notiser och livstecken mot SMGD DriveLog."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.network import NoURLAvailableError, get_url
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util
from homeassistant.util import slugify

from .api import DriveLogApiClient, DriveLogAuthError, DriveLogConnectionError, DriveLogError, DriveLogRejectedError
from .const import (
    CONF_COLLECT_ROUTE,
    CONF_LOCATION_INTERVAL,
    CONF_NOTIFY_SERVICE,
    CONF_ROUTE_ENTITY,
    CONF_SEND_FEEDBACK,
    DEFAULT_LOCATION_INTERVAL,
    DOMAIN,
    INTEGRATION_VERSION,
    MAX_LOCATION_INTERVAL,
    MIN_LOCATION_INTERVAL,
    TRACKING_STALE_AFTER,
    STORAGE_VERSION,
)
from .core import (
    LOCATION_REQUEST,
    ISSUE_NO_PHONE,
    OUTAGE_TITLE,
    OutageTracker,
    ResponseQueue,
    brand_icon_url,
    brand_notify_data,
    build_ack,
    build_callback_payload,
    build_heartbeat,
    feedback_message,
    location_interval,
    missing_phone_count,
    mobile_app_services,
    parse_outbox,
    parse_route_requests,
    phone_issue_action,
    platform_from_os,
    route_points,
    select_service,
    should_request_location,
)

_LOGGER = logging.getLogger(__name__)


class DriveLogManager:
    """Allt som rör notiser för en konfigurationspost.

    * Knapptryck (``mobile_app_notification_action`` med prefixet ``DRIVELOG_``)
      läggs i en persistent kö och skickas till ``POST /ha/callback``. Når vi inte
      DriveLog ligger svaret kvar och försöks igen med växande intervall – även
      efter en omstart av Home Assistant.
    * I pull-läget hämtas notiser från ``GET /ha/outbox``, skickas med
      ``notify.mobile_app_<enhet>`` och kvitteras med ``POST /ha/outbox/ack``.
    * Livstecken (``POST /ha/heartbeat``) talar om HA-version och vilka
      ``notify.mobile_app_*``-tjänster som finns.
    """

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, client: DriveLogApiClient) -> None:
        self.hass = hass
        self.entry = entry
        self.client = client
        self.queue = ResponseQueue()
        self._store: Store[dict[str, Any]] = Store(hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}.responses")
        self._flush_lock = asyncio.Lock()
        self._outbox_lock = asyncio.Lock()
        self._route_lock = asyncio.Lock()
        self.last_route_at: datetime | None = None
        self.last_route_error: str | None = None
        self.routes_sent = 0
        # 1.8.2: positionsbegäran medan bilen körs.
        self.tracking = False
        self._tracking_at: float | None = None
        self._last_location_request: float | None = None
        self.location_requests = 0
        self.delivery_mode: str | None = None
        self.server_notify_service: str | None = None
        self.server_version: str | None = None
        self.poll_interval: int | None = None
        self.last_outbox_at: datetime | None = None
        self.last_heartbeat_at: datetime | None = None
        self.last_error: str | None = None
        self.delivered_count = 0
        self.auth_failed = False
        self.outage = OutageTracker()
        self.phone_issue_open = False

    # ------------------------------------------------------------------ lagring

    async def async_load(self) -> None:
        self.queue = ResponseQueue.from_dict(await self._store.async_load())
        if self.queue.prune(time.time()):
            await self._async_save()

    async def _async_save(self) -> None:
        await self._store.async_save(self.queue.to_dict())

    # ------------------------------------------------------------------ hjälpare

    @property
    def configured_service(self) -> str | None:
        if CONF_NOTIFY_SERVICE in self.entry.options:
            return self.entry.options[CONF_NOTIFY_SERVICE]
        return self.entry.data.get(CONF_NOTIFY_SERVICE)

    def available_services(self) -> list[str]:
        return mobile_app_services(self.hass.services.async_services().get("notify", {}).keys())

    def selected_service(self) -> str | None:
        """DriveLogs val, annars integrationens, annars den enda telefonen i Home Assistant."""
        return select_service(self.server_notify_service, self.configured_service, available=self.available_services())

    def _mobile_app_entry(self, service: str | None) -> ConfigEntry | None:
        """Companion-appens konfigurationspost för ``mobile_app_<enhet>``."""
        if not service:
            return None
        for entry in self.hass.config_entries.async_entries("mobile_app"):
            name = entry.data.get("device_name")
            if isinstance(name, str) and f"mobile_app_{slugify(name)}" == service:
                return entry
        return None

    def platform(self, service: str | None) -> str | None:
        """ios | android | None för telefonen bakom notify-tjänsten."""
        entry = self._mobile_app_entry(service)
        return platform_from_os(entry.data.get("os_name")) if entry else None

    def _icon_url(self) -> str:
        """Loggans adress via Home Assistants externa adress (t.ex. Nabu Casa), annars relativ."""
        try:
            base = get_url(self.hass, allow_internal=False, allow_ip=False, prefer_cloud=True)
        except NoURLAvailableError:
            base = None
        return brand_icon_url(base)

    def _brand(self, service: str | None, data: dict[str, Any]) -> dict[str, Any]:
        return brand_notify_data(data, self.platform(service), self._icon_url())

    def route_entity(self) -> str | None:
        """Telefonens ``device_tracker`` för resvägen, eller None när insamlingen är av.

        Valt i alternativen, annars positionen från samma telefon som får notiserna."""
        opts = self.entry.options
        if not opts.get(CONF_COLLECT_ROUTE, True):
            return None
        configured = opts.get(CONF_ROUTE_ENTITY)
        if isinstance(configured, str) and configured.startswith(("device_tracker.", "person.")):
            return configured
        entry = self._mobile_app_entry(self.selected_service())
        if entry is None:
            return None
        for ent in er.async_entries_for_config_entry(er.async_get(self.hass), entry.entry_id):
            if ent.domain == "device_tracker" and not ent.disabled:
                return ent.entity_id
        return None

    @property
    def _issue_id(self) -> str:
        return f"{ISSUE_NO_PHONE}_{self.entry.entry_id}"

    def _update_phone_issue(self, waiting: int) -> None:
        """Reparationsärende i HA när DriveLog har notiser men ingen telefon är vald."""
        action = phone_issue_action(
            has_service=self.selected_service() is not None, waiting=waiting, issue_open=self.phone_issue_open
        )
        if action == "create":
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                self._issue_id,
                is_fixable=False,
                is_persistent=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key=ISSUE_NO_PHONE,
            )
            self.phone_issue_open = True
        elif action == "delete":
            ir.async_delete_issue(self.hass, DOMAIN, self._issue_id)
            self.phone_issue_open = False

    async def _async_outage(self, *, auth: bool = False) -> None:
        """En lokal notis per avbrott: efter 30 minuter, eller direkt när token avvisas."""
        if not self.outage.failure(time.time(), auth=auth):
            return
        service = self.selected_service()
        if service is None:
            return
        try:
            await self.hass.services.async_call(
                "notify",
                service,
                self._brand(service, {"title": OUTAGE_TITLE, "message": self.outage.message()}),
                blocking=True,
            )
        except HomeAssistantError as err:
            _LOGGER.debug("Kunde inte visa notisen om avbrottet: %s", err)

    def _auth_failed(self, err: DriveLogAuthError) -> None:
        self.last_error = str(err)
        if not self.auth_failed:
            self.auth_failed = True
            _LOGGER.warning("SMGD DriveLog avvisade API-token – logga in igen via integrationen")
            self.entry.async_start_reauth(self.hass)

    # ------------------------------------------------------------------ knapptryck

    @callback
    def async_handle_event(self, event: Event) -> None:
        """Lyssnare på ``mobile_app_notification_action`` (reagerar bara på DRIVELOG_-id)."""
        payload = build_callback_payload(dict(event.data), event.context.id)
        if payload is None:
            return
        if self.queue.add(payload, time.time()):
            self.entry.async_create_background_task(
                self.hass, self._async_after_enqueue(), name=f"{DOMAIN} svar {payload['public_id']}"
            )

    async def _async_after_enqueue(self) -> None:
        await self._async_save()  # spara först – då överlever svaret en omstart
        await self.async_flush_queue()

    async def async_flush_queue(self, *, force: bool = False) -> int:
        """Skickar köade svar. Returnerar antalet som gick fram."""
        async with self._flush_lock:
            now = time.time()
            if force:
                self.queue.retry_all_now(now)
            sent = 0
            changed = False
            for item in self.queue.due(now):
                try:
                    result = await self.client.callback(item.payload)
                except DriveLogAuthError as err:
                    self.queue.failed(item.event_id, now, str(err))
                    self._auth_failed(err)
                    changed = True
                    break
                except DriveLogRejectedError as err:
                    _LOGGER.warning("DriveLog avvisade ett svar och det tas bort ur kön: %s", err)
                    self.queue.drop(item.event_id)
                    changed = True
                    continue
                except DriveLogConnectionError as err:
                    # DriveLog nere: allt som återstår försöks senare.
                    self.last_error = str(err)
                    for rest in self.queue.due(now):
                        self.queue.failed(rest.event_id, now, str(err))
                    changed = True
                    break
                self.queue.succeeded(item.event_id)
                changed = True
                sent += 1
                self.auth_failed = False
                await self._async_feedback(result)
            if changed:
                await self._async_save()
            return sent

    async def _async_feedback(self, result: Any) -> None:
        """Talar om för användaren när ett svar inte kunde tillämpas (t.ex. utgången fråga)."""
        message = feedback_message(result)
        if not message or not self.entry.options.get(CONF_SEND_FEEDBACK, True):
            return
        service = self.selected_service()
        if service is None:
            return
        try:
            await self.hass.services.async_call(
                "notify", service, self._brand(service, {"title": "SMGD DriveLog", "message": message}), blocking=True
            )
        except HomeAssistantError as err:
            _LOGGER.debug("Kunde inte visa återkoppling: %s", err)

    # ------------------------------------------------------------------ utkorg (pull)

    async def async_poll_outbox(self) -> int:
        """Hämtar och levererar notiser (pull-läget). Returnerar antalet levererade."""
        async with self._outbox_lock:
            try:
                body = await self.client.outbox()
            except DriveLogAuthError as err:
                self._auth_failed(err)
                await self._async_outage(auth=True)
                return 0
            except DriveLogError as err:
                self.last_error = str(err)
                await self._async_outage()
                return 0
            self.outage.success()
            self.last_outbox_at = dt_util.utcnow()
            self.delivery_mode = body.get("delivery_mode") if isinstance(body, dict) else None
            self.tracking = bool(body.get("track_position")) if isinstance(body, dict) else False
            self._tracking_at = time.time()
            deliveries, failed = parse_outbox(
                body, self.server_notify_service or self.configured_service, available=self.available_services()
            )
            self._update_phone_issue(missing_phone_count(failed))
            delivered: list[str] = []
            for item in deliveries:
                try:
                    await self.hass.services.async_call(
                        "notify", item.service, self._brand(item.service, item.data), blocking=True
                    )
                except HomeAssistantError as err:  # ServiceNotFound ärver HomeAssistantError
                    failed.append({"id": item.id, "error": f"notify.{item.service}: {err}"})
                    continue
                delivered.append(item.id)
            for batch in build_ack(delivered, failed):
                try:
                    await self.client.ack(batch)
                except DriveLogError as err:
                    # Okvitterat ligger kvar i utkorgen och skickas igen; taggen gör att
                    # notisen ersätts i stället för att dyka upp två gånger.
                    self.last_error = str(err)
                    break
            self.delivered_count += len(delivered)
            return len(delivered)

    # ------------------------------------------------------------------ livstecken

    async def async_heartbeat(self) -> None:
        body = build_heartbeat(
            ha_version=HA_VERSION,
            integration_version=INTEGRATION_VERSION,
            services=self.available_services(),
            selected=self.configured_service,
            queue_length=len(self.queue),
            route_entity=self.route_entity(),
        )
        try:
            result = await self.client.heartbeat(body)
        except DriveLogAuthError as err:
            self._auth_failed(err)
            return
        except DriveLogError as err:
            self.last_error = str(err)
            return
        self.last_heartbeat_at = dt_util.utcnow()
        self.last_error = None
        self.auth_failed = False
        if isinstance(result, dict):
            self.delivery_mode = result.get("delivery_mode")
            self.server_notify_service = result.get("notify_service")
            self.server_version = result.get("server_version")
            interval = result.get("poll_interval")
            self.poll_interval = interval if isinstance(interval, int) and 10 <= interval <= 3600 else None

    async def async_tick(self, _now: datetime | None = None) -> None:
        """Körs periodiskt: svarskö först (svar är viktigast), sedan utkorgen."""
        if self.queue.items:
            await self.async_flush_queue()
        await self.async_poll_outbox()

    # ------------------------------------------------------------------ resväg (1.8.0)

    async def _async_history(self, entity_id: str, start: datetime, end: datetime) -> list[tuple[float, Any]]:
        """(unix-tid, attribut) för varje uppdatering av entiteten ur HA:s historik."""
        from homeassistant.components.recorder import get_instance, history

        def _read() -> list[tuple[float, Any]]:
            states = history.get_significant_states(
                self.hass,
                start,
                end,
                entity_ids=[entity_id],
                include_start_time_state=False,
                significant_changes_only=False,
                minimal_response=False,
                no_attributes=False,
            )
            return [
                (st.last_updated.timestamp(), dict(st.attributes))
                for st in states.get(entity_id, [])
                if hasattr(st, "last_updated")
            ]

        return await get_instance(self.hass).async_add_executor_job(_read)

    @property
    def location_interval(self) -> int:
        return location_interval(
            self.entry.options.get(CONF_LOCATION_INTERVAL),
            DEFAULT_LOCATION_INTERVAL,
            MIN_LOCATION_INTERVAL,
            MAX_LOCATION_INTERVAL,
        )

    async def async_request_location(self, _now: datetime | None = None) -> bool:
        """Ber telefonen om en ny position medan bilen körs (dolt kommando, ingen notis syns).

        Kommandot räknas inte mot Companion-appens gräns för notiser. Utan ett färskt besked
        från DriveLog om att bilen körs görs ingenting."""
        now = time.time()
        if not should_request_location(
            tracking=self.tracking,
            tracking_at=self._tracking_at,
            last_request=self._last_location_request,
            now=now,
            interval=self.location_interval,
            stale_after=TRACKING_STALE_AFTER.total_seconds(),
        ):
            return False
        if self.route_entity() is None:
            return False
        service = self.selected_service()
        if service is None:
            return False
        self._last_location_request = now
        try:
            await self.hass.services.async_call("notify", service, dict(LOCATION_REQUEST), blocking=True)
        except HomeAssistantError as err:
            _LOGGER.debug("Kunde inte be telefonen om position: %s", err)
            return False
        self.location_requests += 1
        return True

    async def async_sync_routes(self, _now: datetime | None = None) -> int:
        """Skickar telefonens positioner för resor som DriveLog saknar resväg för.

        Bara punkter inom resans tid lämnar Home Assistant. Returnerar antalet resor som skickats."""
        entity_id = self.route_entity()
        if entity_id is None or "recorder" not in self.hass.config.components:
            return 0
        async with self._route_lock:
            try:
                requests, max_points = parse_route_requests(await self.client.route_requests())
            except DriveLogAuthError as err:
                self._auth_failed(err)
                return 0
            except DriveLogError as err:
                self.last_route_error = str(err)
                return 0
            sent = 0
            for req in requests:
                try:
                    # Historiken tar inte med slutpunkten; en sekund extra fångar en punkt exakt vid målet.
                    rows = await self._async_history(entity_id, req.start, req.end + timedelta(seconds=1))
                except Exception as err:  # noqa: BLE001 – historiken är en bekvämlighet, resten ska fortsätta
                    self.last_route_error = f"Historiken kunde inte läsas: {type(err).__name__}"
                    _LOGGER.debug("Kunde inte läsa historiken för %s: %s", entity_id, err)
                    return sent
                points = route_points(rows, req.start.timestamp(), req.end.timestamp(), max_points)
                try:
                    await self.client.route({"trip_id": req.trip_id, "entity_id": entity_id, "points": points})
                except DriveLogRejectedError as err:
                    _LOGGER.debug("DriveLog avvisade resvägen för %s: %s", req.trip_id, err)
                    continue
                except DriveLogAuthError as err:
                    self._auth_failed(err)
                    return sent
                except DriveLogError as err:
                    self.last_route_error = str(err)
                    return sent
                sent += 1
            self.routes_sent += sent
            self.last_route_at = dt_util.utcnow()
            self.last_route_error = None
            return sent

    def diagnostics(self) -> dict[str, Any]:
        return {
            "delivery_mode": self.delivery_mode,
            "server_version": self.server_version,
            "server_notify_service": self.server_notify_service,
            "configured_notify_service": self.configured_service,
            "available_notify_services": self.available_services(),
            "poll_interval": self.poll_interval,
            "last_outbox_at": self.last_outbox_at.isoformat() if self.last_outbox_at else None,
            "last_heartbeat_at": self.last_heartbeat_at.isoformat() if self.last_heartbeat_at else None,
            "last_error": self.last_error,
            "delivered_count": self.delivered_count,
            "auth_failed": self.auth_failed,
            "outage_since": self.outage.since,
            "phone_issue_open": self.phone_issue_open,
            "queue": self.queue.diagnostics(),
            "route_entity": self.route_entity(),
            "routes_sent": self.routes_sent,
            "last_route_at": self.last_route_at.isoformat() if self.last_route_at else None,
            "last_route_error": self.last_route_error,
            "tracking": self.tracking,
            "location_interval": self.location_interval,
            "location_requests": self.location_requests,
        }
