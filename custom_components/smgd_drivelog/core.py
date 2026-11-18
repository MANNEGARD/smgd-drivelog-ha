"""Ren hjälplogik för SMGD DriveLog-integrationen.

Modulen importerar ingenting från Home Assistant så att den kan enhetstestas
fristående (se backend/tests/test_ha_integration_core.py). Här finns:

* tolkning av åtgärds-id:n från ``mobile_app_notification_action``,
* bygget av svar, kvittenser och livstecken till DriveLogs API,
* den persistenta svarskön med omförsök och exponentiell backoff.

Sökvägarna nedan måste stämma exakt med backend/app/api/routes/ha.py.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit, urlunsplit

# ----------------------------------------------------------------------------- API

API_PREFIX = "/api/v1"
PATH_WHOAMI = f"{API_PREFIX}/auth/whoami"
PATH_SUMMARY = f"{API_PREFIX}/ha/summary"
PATH_OUTBOX = f"{API_PREFIX}/ha/outbox"
PATH_OUTBOX_ACK = f"{API_PREFIX}/ha/outbox/ack"
PATH_HEARTBEAT = f"{API_PREFIX}/ha/heartbeat"
PATH_CALLBACK = f"{API_PREFIX}/ha/callback"

REQUIRED_SCOPE = "ha"

# ----------------------------------------------------------------------------- åtgärds-id

ACTION_PREFIX = "DRIVELOG_"
_KEY_RE = re.compile(r"^[A-Z]+[0-9]?$")
_ID_RE = re.compile(r"^[a-z0-9]{6,24}$")
_SERVICE_RE = re.compile(r"^mobile_app_[a-z0-9_]{1,200}$")

MAX_REPLY_TEXT = 2000
MAX_ACK_ITEMS = 100


def parse_action_id(value: Any) -> tuple[str, str] | None:
    """``DRIVELOG_WORK_ab12cd34`` → ("WORK", "ab12cd34"). Allt annat ger None."""
    if not isinstance(value, str) or not value.startswith(ACTION_PREFIX):
        return None
    key, sep, public_id = value[len(ACTION_PREFIX) :].rpartition("_")
    if not sep or not _KEY_RE.match(key) or not _ID_RE.match(public_id):
        return None
    return key, public_id


def is_drivelog_action(event_data: Any) -> bool:
    return isinstance(event_data, dict) and parse_action_id(event_data.get("action")) is not None


def build_callback_payload(event_data: dict[str, Any], event_id: str) -> dict[str, Any] | None:
    """Bygger kroppen till ``POST /ha/callback`` ur en ``mobile_app_notification_action``.

    iOS skickar bara ``action``, ``reply_text`` och ``action_data``. Frågans id läses
    ur åtgärdssträngen; ``action_data.drivelog_id`` används som kontroll. Händelser
    som inte kommer från DriveLog ger None och ska ignoreras.
    """
    if not isinstance(event_data, dict):
        return None
    parsed = parse_action_id(event_data.get("action"))
    if parsed is None:
        return None
    _, public_id = parsed
    action_data = event_data.get("action_data")
    if isinstance(action_data, dict):
        other = action_data.get("drivelog_id")
        if isinstance(other, str) and other and other != public_id:
            return None  # motsägelsefull händelse – skicka hellre inget än fel svar
    if not isinstance(event_id, str) or not 8 <= len(event_id) <= 100:
        raise ValueError("event_id måste vara 8–100 tecken.")
    payload: dict[str, Any] = {"public_id": public_id, "action": event_data["action"], "event_id": event_id}
    reply = event_data.get("reply_text")
    if isinstance(reply, str) and reply.strip():
        payload["reply_text"] = reply[:MAX_REPLY_TEXT]
    return payload


# ----------------------------------------------------------------------------- konfiguration


def normalize_base_url(url: str) -> str:
    """Normaliserar DriveLogs adress. Tar bort avslutande snedstreck och ``/api/v1``."""
    raw = (url or "").strip()
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("invalid_url")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ValueError("invalid_url")
    path = parts.path.rstrip("/")
    if path.endswith(API_PREFIX):
        path = path[: -len(API_PREFIX)]
    return urlunsplit((parts.scheme, parts.netloc, path.rstrip("/"), "", ""))


async def secure_base_url(base_url: str, resolve: Callable[[str], Awaitable[list[str]]]) -> str:
    """Byter http mot https när adressen bara pekar på publika adresser.

    Görs innan token skickas första gången: mot en publik server svarar proxyn ändå med
    en omdirigering till https, men då har token redan gått okrypterad. Värdnamn utan
    punkt (tilläggets värdnamn), namn som inte går att slå upp och namn som pekar på
    privata, lokala eller CGNAT-adresser behåller http. ``resolve`` slår upp värdens
    adresser (``getaddrinfo`` i en executor i Home Assistant).
    """
    parts = urlsplit(base_url)
    host = parts.hostname
    if parts.scheme != "http" or not host:
        return base_url
    try:
        addresses = [ipaddress.ip_address(host)]
    except ValueError:
        if "." not in host:
            return base_url
        try:
            addresses = [ipaddress.ip_address(str(a).split("%")[0]) for a in await resolve(host)]
        except (OSError, ValueError):
            return base_url
    if not addresses or not all(a.is_global for a in addresses):
        return base_url
    return urlunsplit(("https", parts.netloc, parts.path, "", ""))


def api_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def notify_service_name(value: Any) -> str | None:
    """``notify.mobile_app_iphone`` → ``mobile_app_iphone``; ogiltigt ger None."""
    if not isinstance(value, str):
        return None
    name = value.strip().lower()
    if name.startswith("notify."):
        name = name[len("notify.") :]
    return name if _SERVICE_RE.match(name) else None


def mobile_app_services(names: Any) -> list[str]:
    """Filtrerar fram Companion-appens legacy-tjänster ``notify.mobile_app_*``."""
    out = {n for n in (notify_service_name(x) for x in (names or [])) if n}
    return sorted(out)


def select_service(item_service: Any, *fallbacks: Any, available: Any = None) -> str | None:
    """Tjänsten en notis ska skickas med: DriveLogs val, annars integrationens.

    Är ingen vald men finns exakt en ``notify.mobile_app_*`` i Home Assistant används
    den (``available`` är listan över tjänster). Med flera telefoner gissas aldrig.
    """
    for candidate in (item_service, *fallbacks):
        name = notify_service_name(candidate)
        if name:
            return name
    only = mobile_app_services(available)
    return only[0] if len(only) == 1 else None


def token_has_scope(whoami: Any) -> bool:
    if not isinstance(whoami, dict):
        return False
    scopes = whoami.get("scopes") or []
    return isinstance(scopes, list) and (REQUIRED_SCOPE in scopes or "*" in scopes)


def build_heartbeat(
    *,
    ha_version: str | None,
    integration_version: str,
    services: list[str],
    selected: str | None,
    queue_length: int,
) -> dict[str, Any]:
    return {
        "ha_version": (ha_version or None) and str(ha_version)[:40],
        "integration_version": integration_version[:40],
        "notify_services": mobile_app_services(services)[:100],
        "notify_service": notify_service_name(selected),
        "queue_length": max(0, int(queue_length)),
    }


# ----------------------------------------------------------------------------- utkorg (pull)


@dataclass
class OutboxDelivery:
    """En notis ur utkorgen, klar att skickas med ``notify.<service>``."""

    id: str
    type: str  # notify | clear
    service: str | None
    data: dict[str, Any]


NO_SERVICE_ERROR = "Ingen notify.mobile_app-tjänst är vald i DriveLog eller i integrationen."
REFUSED_ERROR = "Notisen avvisades: den innehöll annat än DriveLogs eget notisformat."

# Det DriveLog själv bygger (backend/app/services/ha/actions.py, build_notify_data och
# build_clear_data). Allt annat släpps; Companion-kommandon, främmande länkar och
# åtgärder utan DRIVELOG_ avvisar hela notisen.
_TEXT_KEYS = ("title", "message")
_DATA_TEXT_KEYS = ("tag", "group")
_ACTION_TEXT_KEYS = ("title", "textInputButtonTitle", "textInputPlaceholder", "icon")
_ACTION_DATA_KEYS = ("drivelog_id", "kind")
_INTERRUPTION_LEVELS = ("active",)
_CLEAR = "clear_notification"
_COMMAND_RE = re.compile(r"^(?:TTS|[a-z]+(?:_[a-z]+)+)$")


class _Refused(ValueError):
    pass


def _no_html(text: str) -> str:
    """Companion för Android tolkar HTML i notisen; < och > byts mot ‹ › (ser likadana ut)."""
    return text.replace("<", "‹").replace(">", "›")


def _web_link(value: Any) -> str:
    if not isinstance(value, str) or urlsplit(value).scheme.lower() not in ("http", "https"):
        raise _Refused
    return value


def _safe_action(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or parse_action_id(raw.get("action")) is None:
        raise _Refused
    out: dict[str, Any] = {"action": raw["action"]}
    for key in _ACTION_TEXT_KEYS:
        if isinstance(raw.get(key), str):
            out[key] = raw[key]
    if "uri" in raw:
        out["uri"] = _web_link(raw["uri"])
    if "behavior" in raw:
        if raw["behavior"] != "textInput":
            raise _Refused
        out["behavior"] = raw["behavior"]
    return out


def safe_notify_data(data: dict[str, Any]) -> dict[str, Any]:
    """Bygger om tjänstedatan ur en vitlista. Kastar ValueError om notisen ska avvisas."""
    message = data.get("message")
    if not isinstance(message, str):
        raise _Refused
    inner = data.get("data") if isinstance(data.get("data"), dict) else {}
    tag = inner.get("tag")
    if message.strip() == _CLEAR:
        if not isinstance(tag, str) or not tag.startswith("drivelog-"):
            raise _Refused
        return {"message": _CLEAR, "data": {"tag": tag}}
    if _COMMAND_RE.match(message.strip()):
        raise _Refused
    out: dict[str, Any] = {k: _no_html(data[k]) for k in _TEXT_KEYS if isinstance(data.get(k), str)}
    safe: dict[str, Any] = {k: inner[k] for k in _DATA_TEXT_KEYS if isinstance(inner.get(k), str)}
    if "url" in inner:
        safe["url"] = _web_link(inner["url"])
    push = inner.get("push")
    if isinstance(push, dict) and push.get("interruption-level") in _INTERRUPTION_LEVELS:
        safe["push"] = {"interruption-level": push["interruption-level"]}
    action_data = inner.get("action_data")
    if isinstance(action_data, dict):
        safe["action_data"] = {k: action_data[k] for k in _ACTION_DATA_KEYS if isinstance(action_data.get(k), str)}
    if "actions" in inner:
        if not isinstance(inner["actions"], list):
            raise _Refused
        safe["actions"] = [_safe_action(a) for a in inner["actions"]]
    if safe:
        out["data"] = safe
    return out


def parse_outbox(
    body: Any, fallback_service: str | None, available: Any = None
) -> tuple[list[OutboxDelivery], list[dict[str, str]]]:
    """Tolkar svaret från ``GET /ha/outbox``.

    Returnerar (leveranser, misslyckade) där misslyckade är poster som inte kan
    levereras (t.ex. ingen notify-tjänst vald) och ska rapporteras i kvittensen.
    ``available`` är Home Assistants notify-tjänster: finns exakt en telefon används
    den när inget annat är valt.
    """
    deliveries: list[OutboxDelivery] = []
    failed: list[dict[str, str]] = []
    items = body.get("items") if isinstance(body, dict) else None
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        item_id = raw.get("id")
        data = raw.get("data")
        if not isinstance(item_id, str) or not item_id or not isinstance(data, dict):
            continue
        kind = raw.get("type") if raw.get("type") in ("notify", "clear") else "notify"
        service = select_service(raw.get("service"), fallback_service, available=available)
        if service is None:
            failed.append({"id": item_id, "error": NO_SERVICE_ERROR})
            continue
        if not isinstance(data.get("message"), str):
            failed.append({"id": item_id, "error": "Notisen saknar text."})
            continue
        try:
            data = safe_notify_data(data)
        except ValueError:
            failed.append({"id": item_id, "error": REFUSED_ERROR})
            continue
        deliveries.append(OutboxDelivery(id=item_id, type=kind, service=service, data=data))
    return deliveries, failed


def build_ack(delivered: list[str], failed: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Kvittenser till ``POST /ha/outbox/ack`` – uppdelade i omgångar om högst 100."""
    failed = [{"id": f["id"][:24], "error": (f.get("error") or "")[:300]} for f in failed]
    delivered = [d[:24] for d in delivered]
    batches: list[dict[str, Any]] = []
    while delivered or failed:
        batches.append({"delivered": delivered[:MAX_ACK_ITEMS], "failed": failed[:MAX_ACK_ITEMS]})
        delivered, failed = delivered[MAX_ACK_ITEMS:], failed[MAX_ACK_ITEMS:]
    return batches


def feedback_message(result: Any) -> str | None:
    """Text att visa på telefonen när ett svar inte gick igenom (t.ex. utgången fråga).

    Svar på frågor som DriveLog inte känner till (t.ex. en annan användares fråga när
    flera DriveLog-konton finns i samma Home Assistant) ger ingen återkoppling.
    """
    if not isinstance(result, dict) or not result.get("notify_user") or not result.get("public_id"):
        return None
    message = result.get("message")
    return message if isinstance(message, str) and message else None


def missing_phone_count(failed: list[dict[str, str]]) -> int:
    """Antalet notiser i utkorgen som inte kunde skickas för att ingen telefon är vald."""
    return sum(1 for f in failed if f.get("error") == NO_SERVICE_ERROR)


# ----------------------------------------------------------------------------- reparationsärende

ISSUE_NO_PHONE = "no_phone"


def phone_issue_action(*, has_service: bool, waiting: int, issue_open: bool) -> str | None:
    """Vad som ska göras med reparationsärendet "ingen telefon vald".

    ``create`` när DriveLog har notiser som väntar men ingen telefon kan väljas,
    ``delete`` när en telefon finns igen, annars None (lämna som det är).
    """
    if has_service:
        return "delete" if issue_open else None
    if waiting > 0 and not issue_open:
        return "create"
    return None


# ----------------------------------------------------------------------------- avbrott

OUTAGE_NOTIFY_AFTER_SECONDS = 30 * 60
OUTAGE_TITLE = "SMGD DriveLog nås inte"


@dataclass
class OutageTracker:
    """Håller reda på avbrott mot ``GET /ha/outbox``: en lokal notis per avbrott.

    Notisen skickas när hämtningen har misslyckats i mer än 30 minuter, eller direkt
    när DriveLog svarar 401 (token avvisad). Den skickas en gång; nästa lyckade
    hämtning avslutar avbrottet. Tiden är sekunder (``time.time()``).
    """

    since: float | None = None
    auth: bool = False
    notified: bool = False

    def failure(self, now: float, *, auth: bool = False) -> bool:
        """Registrerar ett misslyckande. Sant om den lokala notisen ska skickas nu."""
        if self.since is None:
            self.since = now
        self.auth = self.auth or auth
        if self.notified:
            return False
        if self.auth or now - self.since > OUTAGE_NOTIFY_AFTER_SECONDS:
            self.notified = True
            return True
        return False

    def success(self) -> bool:
        """Hämtningen lyckades. Sant om ett avbrott just tog slut."""
        ended = self.since is not None
        self.since, self.auth, self.notified = None, False, False
        return ended

    def message(self) -> str:
        if self.auth:
            return (
                "DriveLog avvisar Home Assistants API-token. Skapa en ny token i DriveLog och logga in igen "
                "under Inställningar → Enheter och tjänster. Frågor från DriveLog kommer inte fram så länge."
            )
        return (
            "Home Assistant har inte kunnat hämta frågor från DriveLog på över 30 minuter. "
            "Dina svar sparas och skickas när DriveLog nås igen."
        )


# ----------------------------------------------------------------------------- svarskö

MAX_QUEUE_ITEMS = 500
MAX_QUEUE_AGE_SECONDS = 14 * 24 * 3600
BACKOFF_BASE_SECONDS = 15
BACKOFF_MAX_SECONDS = 30 * 60


def backoff_seconds(attempts: int) -> float:
    """15, 30, 60 … sekunder, högst 30 minuter."""
    if attempts <= 0:
        return 0.0
    return float(min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * 2 ** (attempts - 1)))


@dataclass
class QueuedResponse:
    event_id: str
    payload: dict[str, Any]
    created: float
    attempts: int = 0
    next_attempt: float = 0.0
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "payload": self.payload,
            "created": self.created,
            "attempts": self.attempts,
            "next_attempt": self.next_attempt,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueuedResponse | None:
        try:
            payload = data["payload"]
            if not isinstance(payload, dict) or not isinstance(data["event_id"], str):
                return None
            return cls(
                event_id=data["event_id"],
                payload=payload,
                created=float(data.get("created", 0.0)),
                attempts=int(data.get("attempts", 0)),
                next_attempt=float(data.get("next_attempt", 0.0)),
                last_error=data.get("last_error"),
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass
class ResponseQueue:
    """Svar som ännu inte nått DriveLog. Sparas på disk av integrationen (Store).

    Inget svar tappas när DriveLog är nere: det ligger kvar och försöks igen med
    växande intervall. Samma händelse läggs aldrig in två gånger, och DriveLog
    behandlar dessutom ``event_id`` som idempotensnyckel.
    """

    items: list[QueuedResponse] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.items)

    def add(self, payload: dict[str, Any], now: float) -> bool:
        event_id = payload["event_id"]
        if any(i.event_id == event_id for i in self.items):
            return False
        self.items.append(QueuedResponse(event_id=event_id, payload=payload, created=now, next_attempt=now))
        if len(self.items) > MAX_QUEUE_ITEMS:
            del self.items[: len(self.items) - MAX_QUEUE_ITEMS]
        return True

    def due(self, now: float) -> list[QueuedResponse]:
        return [i for i in self.items if i.next_attempt <= now]

    def next_due(self) -> float | None:
        return min((i.next_attempt for i in self.items), default=None)

    def succeeded(self, event_id: str) -> None:
        self.items = [i for i in self.items if i.event_id != event_id]

    def drop(self, event_id: str) -> None:
        self.succeeded(event_id)

    def failed(self, event_id: str, now: float, error: str) -> None:
        for item in self.items:
            if item.event_id == event_id:
                item.attempts += 1
                item.next_attempt = now + backoff_seconds(item.attempts)
                item.last_error = error[:300]

    def retry_all_now(self, now: float) -> None:
        for item in self.items:
            item.next_attempt = now

    def prune(self, now: float) -> int:
        """Tar bort svar som är för gamla för att vara meningsfulla (frågan har gått ut)."""
        before = len(self.items)
        self.items = [i for i in self.items if now - i.created <= MAX_QUEUE_AGE_SECONDS]
        return before - len(self.items)

    def to_dict(self) -> dict[str, Any]:
        return {"items": [i.to_dict() for i in self.items]}

    @classmethod
    def from_dict(cls, data: Any) -> ResponseQueue:
        queue = cls()
        if isinstance(data, dict):
            for raw in data.get("items") or []:
                item = QueuedResponse.from_dict(raw) if isinstance(raw, dict) else None
                if item is not None:
                    queue.items.append(item)
        return queue

    def diagnostics(self) -> list[dict[str, Any]]:
        """Kövy utan svarstexter (de kan innehålla personliga uppgifter)."""
        out: list[dict[str, Any]] = []
        for i in self.items:
            parsed = parse_action_id(i.payload.get("action"))
            out.append(
                {
                    "action": parsed[0] if parsed else None,
                    "attempts": i.attempts,
                    "has_reply_text": bool(i.payload.get("reply_text")),
                    "last_error": i.last_error,
                }
            )
        return out


# ----------------------------------------------------------------------------- fel


def classify_status(status: int) -> str:
    """HTTP-status → ``ok`` | ``auth`` | ``retry`` | ``reject``.

    ``reject`` betyder att DriveLog avvisade förfrågan som ogiltig – ett nytt försök
    med samma innehåll hjälper inte.
    """
    if 200 <= status < 300:
        return "ok"
    if status in (401, 403):
        return "auth"
    if status in (408, 425, 429) or status >= 500:
        return "retry"
    return "reject"
