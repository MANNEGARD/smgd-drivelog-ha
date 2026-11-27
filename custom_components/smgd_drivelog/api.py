"""Asynkron klient mot SMGD DriveLogs API (endast utgående anslutningar från HA)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from .core import (
    PATH_CALLBACK,
    PATH_ELECTRICITY_PRICE,
    PATH_HEARTBEAT,
    PATH_OUTBOX,
    PATH_OUTBOX_ACK,
    PATH_ROUTE,
    PATH_ROUTE_REQUESTS,
    PATH_SUMMARY,
    PATH_WHOAMI,
    api_url,
    classify_status,
)

_LOGGER = logging.getLogger(__name__)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)


class DriveLogError(Exception):
    """Basfel för DriveLog-anrop."""


class DriveLogConnectionError(DriveLogError):
    """DriveLog kunde inte nås eller svarade med ett tillfälligt fel – försök igen senare."""


class DriveLogAuthError(DriveLogError):
    """API-token är ogiltig, återkallad eller saknar behörigheten "ha"."""


class DriveLogRejectedError(DriveLogError):
    """DriveLog avvisade förfrågan (t.ex. valideringsfel). Ett nytt försök hjälper inte."""


class DriveLogApiClient:
    """Tunn klient. Token skickas som Bearer och loggas aldrig."""

    def __init__(self, session: aiohttp.ClientSession, base_url: str, token: str) -> None:
        self._session = session
        self._base_url = base_url
        self._token = token

    @property
    def base_url(self) -> str:
        return self._base_url

    async def _request(self, method: str, path: str, json: Any | None = None) -> Any:
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        try:
            async with self._session.request(
                method, api_url(self._base_url, path), json=json, headers=headers, timeout=REQUEST_TIMEOUT,
                allow_redirects=False,
            ) as resp:
                verdict = classify_status(resp.status)
                if verdict == "auth":
                    raise DriveLogAuthError(f"DriveLog avvisade API-token (HTTP {resp.status}).")
                if verdict == "retry":
                    raise DriveLogConnectionError(f"DriveLog svarade med HTTP {resp.status}.")
                if verdict == "reject":
                    raise DriveLogRejectedError(f"DriveLog avvisade förfrågan (HTTP {resp.status}).")
                try:
                    return await resp.json(content_type=None)
                except ValueError as err:  # t.ex. en HTML-sida från en proxy
                    raise DriveLogConnectionError("Oväntat svar – adressen verkar inte gå till SMGD DriveLog.") from err
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise DriveLogConnectionError(f"SMGD DriveLog kunde inte nås: {type(err).__name__}") from err

    async def whoami(self) -> dict[str, Any]:
        return await self._request("GET", PATH_WHOAMI)

    async def summary(self) -> dict[str, Any]:
        return await self._request("GET", PATH_SUMMARY)

    async def outbox(self) -> dict[str, Any]:
        return await self._request("GET", PATH_OUTBOX)

    async def ack(self, body: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", PATH_OUTBOX_ACK, body)

    async def heartbeat(self, body: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", PATH_HEARTBEAT, body)

    async def callback(self, body: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", PATH_CALLBACK, body)

    async def route_requests(self) -> dict[str, Any]:
        return await self._request("GET", PATH_ROUTE_REQUESTS)

    async def route(self, body: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", PATH_ROUTE, body)

    async def electricity_price(self, body: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", PATH_ELECTRICITY_PRICE, body)
