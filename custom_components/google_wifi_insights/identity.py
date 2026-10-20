"""Resolve names from explicit HA network identities, without changing other devices."""

from __future__ import annotations

import base64
import binascii
import ipaddress
import re
from collections import defaultdict
from urllib.parse import urlsplit


def identifier_pairs(values):
    """Ignore nonstandard registry identifiers rather than failing discovery."""
    for value in values:
        if (isinstance(value, (tuple, list)) and len(value) == 2
                and all(isinstance(part, str) for part in value)):
            yield tuple(value)


def normalize_mac(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"(?:[0-9a-fA-F]{12}|(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}|(?:[0-9a-fA-F]{4}\.){2}[0-9a-fA-F]{4})", value.strip()
    ):
        return None
    compact = re.sub(r"[:.\-]", "", value.strip()).lower()
    if compact in ("000000000000", "ffffffffffff"):
        return None
    return ":".join(compact[i:i + 2] for i in range(0, 12, 2))


def normalize_ip(value):
    if not isinstance(value, str):
        return None
    try:
        address = ipaddress.ip_address(value.split("%", 1)[0].strip())
    except ValueError:
        return None
    if address.is_loopback or address.is_unspecified or address.is_multicast:
        return None
    return str(address)


def ip_values(value):
    values = value if isinstance(value, (list, tuple, set)) else [value]
    return {ip for v in values if (ip := normalize_ip(v))}


def identity_index(records):
    """Multiple integration records for one MAC may share a physical identity."""
    macs, ips = defaultdict(list), defaultdict(list)
    for record in records:
        for mac in record.get("macs", []):
            if (value := normalize_mac(mac)):
                macs[value].append(record)
        for ip in record.get("ips", []):
            if (value := normalize_ip(ip)):
                ips[value].append(record)
    return macs, ips


def matter_interface_addresses(interfaces):
    """Decode the cached General Diagnostics NetworkInterfaces (cluster 0x33)."""
    macs, ips = set(), set()
    if not isinstance(interfaces, list):
        return macs, ips

    def field(item, name, tag):
        if isinstance(item, dict):
            return item.get(name, item.get(str(tag), item.get(tag)))
        return getattr(item, name, None)

    def octets(value):
        if isinstance(value, bytes):
            return value
        if not isinstance(value, str):
            return b""
        try:
            return base64.b64decode(value, validate=True)
        except ValueError, binascii.Error:
            return b""

    for interface in interfaces:
        hardware = field(interface, "hardwareAddress", 4)
        mac = normalize_mac(hardware)
        raw = octets(hardware)
        if not mac and len(raw) == 6:
            mac = normalize_mac(raw.hex())
        if mac:
            macs.add(mac)
        if field(interface, "isOperational", 1) is not True:
            continue
        for name, tag in (("IPv4Addresses", 5), ("IPv6Addresses", 6)):
            addresses = field(interface, name, tag)
            if not isinstance(addresses, (list, tuple)):
                continue
            for value in addresses:
                ip = normalize_ip(value)
                raw = octets(value)
                if not ip and len(raw) in (4, 16):
                    ip = normalize_ip(str(ipaddress.ip_address(raw)))
                if ip:
                    ips.add(ip)
    return macs, ips


def add_matter_records(hass, records):
    """Read addresses already cached by HA's Matter client, with no extra polling."""
    entries = hass.config_entries.async_loaded_entries("matter")
    if not entries:
        return
    from homeassistant.components.matter.helpers import get_node_device_identifier

    for entry in entries:
        client = entry.runtime_data.adapter.matter_client
        if not client.server_info:
            continue
        by_identifier = {identifier: record for record in records.values()
                         if record["config_entry_id"] == entry.entry_id
                         for identifier in record["identifiers"]}
        for node in client.get_nodes():
            record = by_identifier.get(get_node_device_identifier(client.server_info, node.node_id))
            if not record:
                continue
            macs, ips = matter_interface_addresses(node.node_data.attributes.get("0/51/0"))
            record["macs"].update(macs)
            if node.available:
                record["ips"].update(ips)


def select_match(candidates, *, mac=None):
    candidates = {c["id"]: c for c in candidates}.values()
    candidates = list(candidates)
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # Separate HA integrations can represent the same MAC. Prefer the user's name,
    # then a deterministic ID. Conflicting user names remain ambiguous.
    if mac and all(mac in c.get("macs", []) for c in candidates):
        named = [c for c in candidates if c.get("user_name")]
        if len({c["name"] for c in named}) > 1:
            return None
        return min(named or candidates, key=lambda c: c["id"])
    return None


def resolve_client(client, index, duplicate_ips=()):
    macs, ips = index
    mac = normalize_mac(client.get("mac_address"))
    exact = macs.get(mac, []) if mac else []
    if exact:
        match = select_match(exact, mac=mac)
        return (match, "mac") if match else (None, "ambiguous")
    if client.get("connected") is not True:
        return None, None
    addresses = ip_values(client.get("ip_addresses", [])) | ip_values(client.get("ip_address"))
    addresses -= set(duplicate_ips)
    candidates = [record for ip in addresses for record in ips.get(ip, [])
                  if not mac or not record.get("macs") or mac in record["macs"]]
    match = select_match(candidates)
    return (match, "ip") if match else (None, "ambiguous" if candidates else None)


def ha_records(hass):
    """Read registries, entity attributes and explicit configured LAN addresses."""
    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er

    from .const import DOMAIN

    registry = dr.async_get(hass)
    records = {}
    for device in registry.devices:
        if any(domain == DOMAIN for domain, _ in identifier_pairs(device.identifiers)) or device.disabled_by:
            continue
        name = device.name_by_user or device.name
        if not name:
            continue
        record = {"id": device.id, "name": name, "user_name": bool(device.name_by_user),
                  "macs": set(), "ips": set(), "config_entry_id": device.config_entry_id,
                  "identifiers": set(identifier_pairs(device.identifiers))}
        for kind, value in identifier_pairs(device.connections):
            if kind == dr.CONNECTION_NETWORK_MAC and (mac := normalize_mac(value)):
                record["macs"].add(mac)
        for kind, value in identifier_pairs(device.identifiers):
            if kind.endswith("_host"):
                record["ips"].update(ip_values(value))
        if device.configuration_url:
            try:
                record["ips"].update(ip_values(urlsplit(str(device.configuration_url)).hostname))
            except ValueError:
                pass
        records[device.id] = record
    entities = er.async_get(hass)
    for state in hass.states.async_all():
        entity = entities.async_get(state.entity_id)
        if not entity or entity.device_id not in records or entity.disabled_by:
            continue
        record = records[entity.device_id]
        attrs = state.attributes
        for key in ("ip", "ip_address", "ip_addresses", "host"):
            record["ips"].update(ip_values(attrs.get(key)))
        for key in ("mac", "mac_address"):
            if (mac := normalize_mac(attrs.get(key))):
                record["macs"].add(mac)
    # A host in a config entry is usable only when the entry owns one physical device.
    # Otherwise it may be a hub address shared by many downstream devices.
    by_entry = defaultdict(list)
    for record in records.values():
        by_entry[record["config_entry_id"]].append(record)
    for entry_id, linked in by_entry.items():
        if len(linked) != 1:
            continue
        entry = hass.config_entries.async_get_entry(entry_id)
        if entry:
            for data in (entry.data, entry.options):
                for key in ("host", "ip_address", "ip_addresses"):
                    linked[0]["ips"].update(ip_values(data.get(key)))
    # Shelly sub-devices expose their parent's URL. Only the network device gets it.
    for device_id, record in records.items():
        device = registry.async_get(device_id, include_child_devices=False)
        if device.via_device_id in records:
            parent = records[device.via_device_id]
            record["ips"] -= parent["ips"]
    add_matter_records(hass, records)
    return list(records.values())


def enrich_snapshot(hass, entry, snapshot):
    from homeassistant.helpers import device_registry as dr

    from .const import DOMAIN

    index = identity_index(ha_records(hass))
    registry = dr.async_get(hass)
    owners = defaultdict(set)
    for network_id, network in snapshot["networks"].items():
        for client_id, client in network["clients"].items():
            if client.get("connected") is True:
                for ip in ip_values(client.get("ip_addresses", [])) | ip_values(client.get("ip_address")):
                    owners[ip].add((network_id, client_id))
    duplicates = {ip for ip, clients in owners.items() if len(clients) > 1}
    for network_id, network in snapshot["networks"].items():
        for client_id, client in network["clients"].items():
            match, method = resolve_client(client, index, duplicates)
            client["google_name"] = client.get("google_name", client["name"])
            client["ha_device_id"] = match["id"] if match else None
            client["ha_device_name"] = match["name"] if match else None
            client["identity_match"] = method
            mac = normalize_mac(client.get("mac_address"))
            client["ha_area_device_ids"] = sorted({record["id"] for record in index[0].get(mac, [])}) if method == "mac" else []
            matched_device = registry.async_get(match["id"]) if match else None
            client["ha_manufacturer"] = matched_device.manufacturer if matched_device else None
            client["ha_model"] = matched_device.model if matched_device else None
            own = registry.async_get_device_by_identifier((DOMAIN, f"{entry.entry_id}:{network_id}:client:{client_id}"), entry.entry_id)
            client["gwi_device_id"] = own.id if own else None
            client["name_source"] = "home_assistant" if match else "google"
            client["name"] = f"{client['google_name']} ({match['name']})" if match and client["google_name"] != match["name"] else client["google_name"]
            identifier = (DOMAIN, f"{entry.entry_id}:{network_id}:client:{client_id}")
            own = registry.async_get_device_by_identifier(identifier, entry.entry_id)
            if own:
                url = f"homeassistant://config/devices/device/{match['id']}" if match else None
                if own.name != client["name"] or own.configuration_url != url:
                    registry.async_update_device(own.id, name=client["name"], configuration_url=url)
                # An explicit name given to this Google WiFi device wins in the map too.
                if own.name_by_user:
                    client["name"] = own.name_by_user
                    client["name_source"] = "user"
        for ap_id, point in network["points"].items():
            point["client_list"] = [c["name"] for c in network["clients"].values()
                                    if c.get("connected") is True and c.get("ap_id") == ap_id]
    return snapshot
