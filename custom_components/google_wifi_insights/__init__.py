"""Google WiFi Insights custom integration."""

from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.storage import Store

from .const import DOMAIN, PLATFORMS
from .coordinator import WifiCoordinator
from .frontend import async_setup_frontend, async_unload_frontend
from .identity import identifier_pairs
from .services import register_services

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


async def async_setup(hass, config):
    register_services(hass)
    return True


async def async_setup_entry(hass, entry):
    coordinator = WifiCoordinator(hass, entry)
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await async_setup_frontend(hass, entry.entry_id)
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    coordinator.async_start_name_sync()
    return True


async def async_reload_entry(hass, entry):
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass, entry):
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        async_unload_frontend(hass, entry.entry_id)
    return unloaded


async def async_remove_entry(hass, entry):
    """Remove only this entry's private mappings, locations and naming history."""
    await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.areas").async_remove()
    await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.names").async_remove()
    await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.locations").async_remove()
    await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.client_areas").async_remove()


async def async_remove_config_entry_device(hass, entry, device):
    """Allow manual removal only after a successful, complete discovery omits it."""
    coordinator = getattr(entry, "runtime_data", None)
    if not coordinator or not coordinator.last_update_success or (coordinator.data or {}).get("network_errors"):
        return False
    prefix = f"{entry.entry_id}:"
    owned = {identifier for domain, identifier in identifier_pairs(device.identifiers) if domain == entry.domain and identifier.startswith(prefix)}
    if not owned:
        return False
    current = set()
    for nid, network in (coordinator.data or {}).get("networks", {}).items():
        current.add(prefix + nid)
        for kind in ("point", "client"):
            for item_id in network["points" if kind == "point" else "clients"]:
                current.add(f"{prefix}{nid}:{kind}:{item_id}")
    return owned.isdisjoint(current)
