"""Opt-in HA device areas following explicit current wireless point mappings."""

import asyncio
from collections import Counter, defaultdict

from homeassistant.helpers import area_registry, device_registry
from homeassistant.helpers.storage import Store

from .const import DOMAIN
from .identity import identifier_pairs, normalize_mac


class ClientAreaSync:
    """Change only HA metadata; never infer presence or send network commands."""

    def __init__(self, hass, entry):
        self.hass, self.entry = hass, entry
        self._store = Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.client_areas")
        self._lock = asyncio.Lock()
        self._loaded = False
        self.settings = {}

    async def async_load(self):
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            saved = await self._store.async_load()
            if isinstance(saved, dict):
                self.settings = {
                    nid: {"enabled": value.get("enabled") is True,
                          "include_matched_devices": value.get("include_matched_devices") is True}
                    for nid, value in saved.items() if isinstance(nid, str) and isinstance(value, dict)
                }
            self._loaded = True

    def response(self, nid):
        return {"supported": True, "enabled": self.settings.get(nid, {}).get("enabled", False),
                "include_matched_devices": self.settings.get(nid, {}).get("include_matched_devices", False)}

    async def async_set(self, nid, enabled, include_matched_devices):
        async with self._lock:
            updated = {**self.settings, nid: {"enabled": enabled, "include_matched_devices": include_matched_devices}}
            await self._store.async_save(updated)
            self.settings = updated

    def _area(self, network, client):
        point = network.get("points", {}).get(client.get("ap_id"), {})
        if (network.get("reachable") is False or client.get("connected") is not True
                or client.get("connection_type") != "WIRELESS" or point.get("online") is not True
                or point.get("reachable") is False):
            return None
        area_id = point.get("mapped_area_id")
        return area_id if area_id and area_registry.async_get(self.hass).async_get_area(area_id) else None

    def _own_device(self, nid, sid):
        return device_registry.async_get(self.hass).async_get_device_by_identifier(
            (DOMAIN, f"{self.entry.entry_id}:{nid}:client:{sid}"), self.entry.entry_id
        )

    def sync_new_client(self, snapshot, nid, sid):
        """Assign newly registered client devices before the next poll is needed."""
        if not self.response(nid)["enabled"]:
            return
        network = snapshot.get("networks", {}).get(nid, {})
        client = network.get("clients", {}).get(sid, {})
        area_id = self._area(network, client)
        own = self._own_device(nid, sid)
        if area_id and own and not own.disabled_by and own.area_id != area_id:
            device_registry.async_get(self.hass).async_update_device(own.id, area_id=area_id)

    def apply(self, snapshot):
        """Retain the last area when disconnected, stale, wired or unmapped."""
        registry = device_registry.async_get(self.hass)
        networks = snapshot.get("networks", {})
        mac_counts = Counter(
            normalize_mac(client.get("mac_address"))
            for network in networks.values() for client in network.get("clients", {}).values()
            if client.get("connected") is True
        )
        targets = defaultdict(set)
        for nid, network in networks.items():
            policy = self.response(nid)
            network["client_area_sync"] = policy
            if not policy["enabled"]:
                continue
            for sid, client in network.get("clients", {}).items():
                area_id = self._area(network, client)
                if not area_id:
                    continue
                own = self._own_device(nid, sid)
                if own and not own.disabled_by:
                    targets[own.id].add(area_id)
                mac = normalize_mac(client.get("mac_address"))
                if (not policy["include_matched_devices"] or not mac or mac_counts[mac] != 1
                        or client.get("identity_match") != "mac"):
                    continue
                for device_id in client.get("ha_area_device_ids", []):
                    matched = registry.async_get(device_id)
                    if matched and not matched.disabled_by and not any(d == DOMAIN for d, _ in identifier_pairs(matched.identifiers)):
                        targets[device_id].add(area_id)
        for device_id, areas in targets.items():
            # Never pick a winner when multiple current clients disagree on a target.
            if len(areas) == 1:
                area_id = next(iter(areas))
                if registry.async_get(device_id).area_id != area_id:
                    registry.async_update_device(device_id, area_id=area_id)
        return snapshot
