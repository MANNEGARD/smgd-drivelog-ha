"""Sensorer: resor att granska, tjänstemil och privata mil denna månad, senaste synk."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from . import DriveLogConfigEntry
from .const import DOMAIN, INTEGRATION_VERSION, NAME
from .coordinator import DriveLogCoordinator

UNIT_MIL = "mil"  # svensk mil = 10 km


def _mil(key: str) -> Callable[[dict[str, Any]], float | None]:
    def _value(data: dict[str, Any]) -> float | None:
        km = data.get(key)
        return round(float(km) / 10, 1) if isinstance(km, (int, float)) else None

    return _value


def _timestamp(key: str) -> Callable[[dict[str, Any]], datetime | None]:
    def _value(data: dict[str, Any]) -> datetime | None:
        raw = data.get(key)
        return dt_util.parse_datetime(raw) if isinstance(raw, str) else None

    return _value


@dataclass(frozen=True, kw_only=True)
class DriveLogSensorDescription(SensorEntityDescription):
    value_fn: Callable[[dict[str, Any]], Any]
    attrs_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None


SENSORS: tuple[DriveLogSensorDescription, ...] = (
    DriveLogSensorDescription(
        key="review_count",
        translation_key="review_count",
        icon="mdi:clipboard-check-outline",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("review_count"),
    ),
    DriveLogSensorDescription(
        key="pending_questions",
        translation_key="pending_questions",
        icon="mdi:message-question-outline",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("pending_questions"),
    ),
    DriveLogSensorDescription(
        key="work_mil_month",
        translation_key="work_mil_month",
        icon="mdi:briefcase-outline",
        native_unit_of_measurement=UNIT_MIL,
        suggested_display_precision=1,
        state_class=SensorStateClass.TOTAL,
        value_fn=_mil("work_km_month"),
        attrs_fn=lambda d: {"km": d.get("work_km_month")},
    ),
    DriveLogSensorDescription(
        key="private_mil_month",
        translation_key="private_mil_month",
        icon="mdi:home-outline",
        native_unit_of_measurement=UNIT_MIL,
        suggested_display_precision=1,
        state_class=SensorStateClass.TOTAL,
        value_fn=_mil("private_km_month"),
        attrs_fn=lambda d: {"km": d.get("private_km_month")},
    ),
    DriveLogSensorDescription(
        key="last_sync",
        translation_key="last_sync",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=_timestamp("last_sync_at"),
        attrs_fn=lambda d: {"vehicle_sync_status": d.get("vehicle_sync_status")},
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: DriveLogConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator = entry.runtime_data.coordinator
    async_add_entities(DriveLogSensor(coordinator, entry, description) for description in SENSORS)


class DriveLogSensor(CoordinatorEntity[DriveLogCoordinator], SensorEntity):
    _attr_has_entity_name = True
    entity_description: DriveLogSensorDescription

    def __init__(
        self, coordinator: DriveLogCoordinator, entry: DriveLogConfigEntry, description: DriveLogSensorDescription
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=NAME,
            manufacturer="SMGD",
            model="DriveLog",
            sw_version=INTEGRATION_VERSION,
            entry_type=DeviceEntryType.SERVICE,
            configuration_url=coordinator.client.base_url,
        )

    @property
    def native_value(self) -> Any:
        return self.entity_description.value_fn(self.coordinator.data or {})

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        if self.entity_description.attrs_fn is None:
            return None
        return self.entity_description.attrs_fn(self.coordinator.data or {})
