"""Svarskö, pull-leverans av notiser och livstecken mot SMGD DriveLog."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .api import DriveLogApiClient, DriveLogAuthError, DriveLogConnectionError, DriveLogError, DriveLogRejectedError
from .const import (
    CONF_NOTIFY_SERVICE,
    CONF_SEND_FEEDBACK,
    DOMAIN,
    INTEGRATION_VERSION,
    STORAGE_VERSION,
)
from .core import (
    ISSUE_NO_PHONE,
    OUTAGE_TITLE,
    OutageTracker,
    ResponseQueue,
    build_ack,
    build_callback_payload,
    build_heartbeat,
    feedback_message,
    missing_phone_count,
    mobile_app_services,
    parse_outbox,
    phone_issue_action,
    select_service,
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
                "notify", service, {"title": OUTAGE_TITLE, "message": self.outage.message()}, blocking=True
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
                "notify", service, {"title": "SMGD DriveLog", "message": message}, blocking=True
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
            deliveries, failed = parse_outbox(
                body, self.server_notify_service or self.configured_service, available=self.available_services()
            )
            self._update_phone_issue(missing_phone_count(failed))
            delivered: list[str] = []
            for item in deliveries:
                try:
                    await self.hass.services.async_call("notify", item.service, item.data, blocking=True)
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
        }
