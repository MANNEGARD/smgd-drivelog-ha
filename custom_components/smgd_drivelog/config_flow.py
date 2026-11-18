"""Konfigurationsflöde: adress + API-token, valfri notify-tjänst, reauth och alternativ."""

from __future__ import annotations

import logging
import socket
from collections.abc import Mapping
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    BooleanSelector,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .api import DriveLogApiClient, DriveLogAuthError, DriveLogError
from .const import CONF_NOTIFY_SERVICE, CONF_SEND_FEEDBACK, CONF_TOKEN, CONF_URL, DOMAIN, NAME
from .core import (
    mobile_app_services,
    normalize_base_url,
    notify_service_name,
    secure_base_url,
    token_has_scope,
)

_LOGGER = logging.getLogger(__name__)


def _notify_options(hass: HomeAssistant) -> list[str]:
    return [f"notify.{s}" for s in mobile_app_services(hass.services.async_services().get("notify", {}).keys())]


def _service_selector(hass: HomeAssistant) -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(options=_notify_options(hass), custom_value=True, mode=SelectSelectorMode.DROPDOWN)
    )


async def _validate(hass: HomeAssistant, url: str, token: str) -> tuple[dict[str, Any], dict[str, str]]:
    """Kontrollerar adress och token mot GET /api/v1/auth/whoami."""
    errors: dict[str, str] = {}
    try:
        base = normalize_base_url(url)
    except ValueError:
        return {}, {CONF_URL: "invalid_url"}

    async def _resolve(host: str) -> list[str]:
        infos = await hass.async_add_executor_job(socket.getaddrinfo, host, None)
        return [info[4][0] for info in infos]

    # http mot en publik adress blir https innan token skickas (lokala adresser behåller http).
    base = await secure_base_url(base, _resolve)
    client = DriveLogApiClient(async_get_clientsession(hass), base, token.strip())
    try:
        who = await client.whoami()
    except DriveLogAuthError:
        errors["base"] = "invalid_auth"
    except DriveLogError:
        errors["base"] = "cannot_connect"
    except Exception:  # oväntat – visas som okänt fel
        _LOGGER.exception("Oväntat fel vid kontroll av SMGD DriveLog")
        errors["base"] = "unknown"
    else:
        if not token_has_scope(who):
            errors["base"] = "missing_scope"
        else:
            return {"url": base, "user": who.get("user")}, {}
    return {}, errors


class DriveLogConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            info, errors = await _validate(self.hass, user_input[CONF_URL], user_input[CONF_TOKEN])
            service = user_input.get(CONF_NOTIFY_SERVICE)
            if not errors and service and notify_service_name(service) is None:
                errors[CONF_NOTIFY_SERVICE] = "invalid_service"
            if not errors:
                await self.async_set_unique_id(f"{info['url']}|{info.get('user') or ''}".lower())
                self._abort_if_unique_id_configured()
                data = {CONF_URL: info["url"], CONF_TOKEN: user_input[CONF_TOKEN].strip()}
                if service:
                    data[CONF_NOTIFY_SERVICE] = notify_service_name(service)
                title = f"{NAME} ({info['user']})" if info.get("user") else NAME
                return self.async_create_entry(title=title, data=data)
        schema = vol.Schema(
            {
                vol.Required(CONF_URL, default=(user_input or {}).get(CONF_URL, "")): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.URL)
                ),
                vol.Required(CONF_TOKEN): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
                vol.Optional(CONF_NOTIFY_SERVICE): _service_selector(self.hass),
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_reauth(self, entry_data: Mapping[str, Any]) -> ConfigFlowResult:
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        entry = self._get_reauth_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            _, errors = await _validate(self.hass, entry.data[CONF_URL], user_input[CONF_TOKEN])
            if not errors:
                return self.async_update_reload_and_abort(
                    entry, data={**entry.data, CONF_TOKEN: user_input[CONF_TOKEN].strip()}
                )
        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {vol.Required(CONF_TOKEN): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD))}
            ),
            description_placeholders={"url": entry.data[CONF_URL]},
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return DriveLogOptionsFlow()


class DriveLogOptionsFlow(OptionsFlow):
    """Alternativ: vilken telefon notiser skickas till och om återkoppling visas."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        current = {**self.config_entry.data, **self.config_entry.options}
        if user_input is not None:
            service = user_input.get(CONF_NOTIFY_SERVICE)
            if service and notify_service_name(service) is None:
                errors[CONF_NOTIFY_SERVICE] = "invalid_service"
            else:
                return self.async_create_entry(
                    data={
                        CONF_NOTIFY_SERVICE: notify_service_name(service) if service else None,
                        CONF_SEND_FEEDBACK: bool(user_input.get(CONF_SEND_FEEDBACK, True)),
                    }
                )
        default_service = f"notify.{current[CONF_NOTIFY_SERVICE]}" if current.get(CONF_NOTIFY_SERVICE) else None
        fields: dict[Any, Any] = {}
        if default_service:
            fields[vol.Optional(CONF_NOTIFY_SERVICE, default=default_service)] = _service_selector(self.hass)
        else:
            fields[vol.Optional(CONF_NOTIFY_SERVICE)] = _service_selector(self.hass)
        fields[vol.Optional(CONF_SEND_FEEDBACK, default=current.get(CONF_SEND_FEEDBACK, True))] = BooleanSelector()
        return self.async_show_form(step_id="init", data_schema=vol.Schema(fields), errors=errors)
