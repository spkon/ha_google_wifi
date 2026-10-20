"""Exercise real HA 2026.10.1 flows, platforms, discovery and unload in isolation."""

import asyncio
import copy
import shutil
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
import pytest
import yaml
from homeassistant import config_entries, loader
from homeassistant.config_entries import ConfigEntries
from homeassistant.core import HomeAssistant
from homeassistant.helpers import (
    area_registry,
    category_registry,
    device_registry,
    entity_registry,
    floor_registry,
    frame,
    issue_registry,
    label_registry,
)
from homeassistant.helpers.template import Template

DOMAIN = "google_wifi_insights"
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.asyncio
async def test_real_ha_restart_feedback_targets_progress_and_unload(real_hass, monkeypatch):
    from types import MappingProxyType

    from homeassistant.core import callback

    from custom_components.google_wifi_insights.api import CloudClient
    from custom_components.google_wifi_insights.model import cloud_network

    group = {"name": "Test WiFi", "accessPoints": [
        {"id": "point", "accessPointSettings": {"name": "Test point"}},
        {"id": "other", "accessPointProperties": {"isGroupRoot": True}},
    ]}
    snapshot = {"networks": {"home": cloud_network(group, {}, [], {})}}
    monkeypatch.setattr(CloudClient, "snapshot", AsyncMock(side_effect=lambda: copy.deepcopy(snapshot)))
    request = AsyncMock(side_effect=[
        {"operation": {"operationId": "restart", "operationState": "CREATED"}},
        {"operation": {"operationId": "mesh", "operationState": "CREATED"}},
    ])
    monkeypatch.setattr(CloudClient, "_request", request)
    monkeypatch.setattr(CloudClient, "_get", AsyncMock(side_effect=[{"operationState": "RUNNING"}, {"operationState": "DONE"}]))
    monkeypatch.setattr("custom_components.google_wifi_insights.coordinator.OPERATION_POLL_SECONDS", 0)
    entry = config_entries.ConfigEntry(data={"mode": "cloud", "email": "test@example.invalid", "master_token": "dummy", "android_id": "0"*16},
        domain=DOMAIN, discovery_keys=MappingProxyType({}), options={}, source="user", title="Test WiFi", unique_id="feedback", version=1, minor_version=1, subentries_data=None)
    await real_hass.config_entries.async_add(entry)
    await real_hass.async_block_till_done()
    assert entry.state == config_entries.ConfigEntryState.LOADED
    states = {s.attributes["gwi_metric"]: s for s in real_hass.states.async_all() if s.attributes.get("gwi_kind") == "point" and s.attributes.get("gwi_item_id") == "point"}
    status = states["operation_status"].entity_id
    assert real_hass.states.get(status).state == "none"
    transitions = []

    @callback
    def observe(event):
        if event.data.get("entity_id") == status and event.data.get("new_state"):
            transitions.append(event.data["new_state"].state)

    unsubscribe = real_hass.bus.async_listen("state_changed", observe)
    await real_hass.services.async_call("button", "press", {"entity_id": states["restart_point"].entity_id}, blocking=True)
    task = entry.runtime_data._operation_tasks.get("home")
    if task:
        await asyncio.wait_for(task, 5)
    await real_hass.async_block_till_done()
    assert {"queued", "sending", "accepted", "running", "completed"} <= set(transitions)
    assert real_hass.states.get(status).state == "completed"
    feedback = real_hass.states.get(states["restart_point"].entity_id).attributes["operation_feedback"]
    assert feedback["phase"] == "completed" and feedback["target_id"] == "point"
    assert "operation_id" not in feedback
    assert real_hass.states.get(states["test_mesh"].entity_id).attributes["operation_feedback"] is None
    foreign = next(s for s in real_hass.states.async_all() if s.attributes.get("gwi_item_id") == "other" and s.attributes.get("gwi_metric") == "operation_status")
    assert foreign.state == "none"
    network = next(s for s in real_hass.states.async_all() if s.attributes.get("gwi_kind") == "network" and s.attributes.get("gwi_metric") == "operation_status")
    assert network.state == "completed" and network.attributes["operation_feedback"]["target_id"] == "point"
    monkeypatch.setattr("custom_components.google_wifi_insights.coordinator.OPERATION_POLL_SECONDS", 60)
    await real_hass.services.async_call("button", "press", {"entity_id": states["test_mesh"].entity_id}, blocking=True)
    assert real_hass.states.get(status).state == "accepted"
    assert request.call_args.args == ("POST", "accesspoints/other/localSpeedTest?prettyPrint=false", None)
    pending_task = entry.runtime_data._operation_tasks["home"]
    coordinator = entry.runtime_data
    unsubscribe()
    assert await real_hass.config_entries.async_unload(entry.entry_id)
    assert pending_task.cancelled()
    assert not coordinator._operation_tasks
    assert request.call_count == 2


@pytest.fixture
async def real_hass(tmp_path, monkeypatch):
    shutil.copytree(
        ROOT / "custom_components",
        tmp_path / "custom_components",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    hass = HomeAssistant(str(tmp_path))
    loader.async_setup(hass)
    frame.async_setup(hass)
    hass.config_entries = ConfigEntries(hass, {})
    await hass.config_entries.async_initialize()
    device_registry.async_setup(hass)
    await asyncio.gather(
        *(
            registry.async_load(hass)
            for registry in (
                area_registry,
                category_registry,
                device_registry,
                entity_registry,
                floor_registry,
                issue_registry,
                label_registry,
            )
        )
    )
    session = aiohttp.ClientSession()
    # Avoid starting DNS/multicast discovery in this isolated framework test.
    # The transport is independently exercised by test_api.py.
    monkeypatch.setattr(
        "custom_components.google_wifi_insights.config_flow.async_get_clientsession",
        lambda hass: session,
    )
    monkeypatch.setattr(
        "custom_components.google_wifi_insights.coordinator.async_get_clientsession",
        lambda hass: session,
    )
    yield hass
    await hass.async_stop(force=True)
    await session.close()


@pytest.mark.asyncio
async def test_static_client_states_do_not_follow_traffic_and_large_diagnostics_are_unrecorded(real_hass, monkeypatch):
    """Preserve map carriers and raw API data while eliminating redundant writes."""
    from types import MappingProxyType

    from custom_components.google_wifi_insights.api import CloudClient
    from custom_components.google_wifi_insights.model import cloud_network
    from custom_components.google_wifi_insights.sensor import WifiSensor

    station = {"id": "client", "friendlyName": "Test client", "connected": True,
               "apId": "point", "ipAddress": "192.168.50.2", "connectionType": "WIRELESS"}
    snapshot = {"networks": {"home": cloud_network(
        {"accessPoints": [{"id": "point"}]},
        {"apStatuses": [{"apId": "point", "apState": "AP_ONLINE"}]},
        [station], {"stationMetrics": [{"station": station, "traffic": {}}]},
        {"client": "aa:bb:cc:dd:ee:ff"},
    )}}
    raw = {str(i): "diagnostic value " * 20 for i in range(100)}
    snapshot["networks"]["home"]["google_telemetry"]["large_info"] = raw
    monkeypatch.setattr(CloudClient, "snapshot", AsyncMock(side_effect=lambda: copy.deepcopy(snapshot)))
    entry = config_entries.ConfigEntry(
        data={"mode": "cloud", "email": "test@example.invalid", "master_token": "dummy", "android_id": "0"*16},
        domain=DOMAIN, discovery_keys=MappingProxyType({}), options={}, source="user",
        title="Test network", unique_id="efficient-state-test", version=1, minor_version=1, subentries_data=None,
    )
    await real_hass.config_entries.async_add(entry)
    await real_hass.async_block_till_done()
    assert entry.state == config_entries.ConfigEntryState.LOADED
    client = {s.attributes["gwi_metric"]: s for s in real_hass.states.async_all()
              if s.attributes.get("gwi_kind") == "client"}
    original_ids = {s.entity_id for s in real_hass.states.async_all()}
    ip_before = client["ip_address"]
    assert "download_mbps" not in ip_before.attributes
    assert client["point"].attributes["download_mbps"] == 0.0
    assert client["point"].attributes["connected"] is True
    snapshot["networks"]["home"]["clients"]["client"].update(
        download_mbps=12.5, upload_mbps=2.0, rssi=-53, rssi_source="shelly_local",
        rssi_observed_at="2026-01-01T12:00:00Z",
    )
    await entry.runtime_data.async_refresh()
    await real_hass.async_block_till_done()
    ip_after = real_hass.states.get(ip_before.entity_id)
    assert ip_after.attributes == ip_before.attributes
    assert ip_after.last_updated == ip_before.last_updated
    carrier = real_hass.states.get(client["point"].entity_id)
    assert carrier.attributes["download_mbps"] == 12.5
    assert carrier.attributes["rssi"] == -53
    assert real_hass.states.get(client["rssi"].entity_id).attributes["rssi_source"] == "shelly_local"
    assert real_hass.states.get(client["download_mbps"].entity_id).state == "12.5"
    telemetry = next(s for s in real_hass.states.async_all() if s.attributes.get("gwi_kind") == "network"
                     and s.attributes.get("gwi_metric") == "google_telemetry_at")
    assert telemetry.attributes["google_telemetry"]["large_info"] == raw
    assert "google_telemetry" in WifiSensor._unrecorded_attributes
    assert "usage_windows" in WifiSensor._unrecorded_attributes
    assert "google_telemetry" in telemetry.state_info["unrecorded_attributes"]
    assert {s.entity_id for s in real_hass.states.async_all()} == original_ids
    assert await real_hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
async def test_local_setup_real_entities_and_unload(real_hass, monkeypatch):
    from custom_components.google_wifi_insights.api import LocalClient
    from custom_components.google_wifi_insights.model import local_point

    point = local_point(
        {"host": "192.168.50.1", "name": "Sala"},
        {
            "system": {"uptime": 123, "software": {}},
            "software": {"softwareVersion": "test"},
            "wan": {"online": True},
        },
    )
    snapshot = {
        "networks": {
            "local": {
                "name": "Google WiFi",
                "source": "local_status",
                "points": {"192.168.50.1": point},
                "clients": {},
                "diagnosis": "insufficient_radio_data",
            }
        }
    }
    monkeypatch.setattr(LocalClient, "snapshot", AsyncMock(return_value=snapshot))
    result = await real_hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] == "menu"
    result = await real_hass.config_entries.flow.async_configure(
        result["flow_id"], {"next_step_id": "local"}
    )
    assert result["step_id"] == "local"
    result = await real_hass.config_entries.flow.async_configure(
        result["flow_id"], {"points": "Sala=192.168.50.1"}
    )
    assert result["type"] == "create_entry"
    entry = result["result"]
    await real_hass.async_block_till_done()
    assert entry.state == config_entries.ConfigEntryState.LOADED
    states = real_hass.states.async_all()
    uptime = next(
        state
        for state in states
        if state.attributes.get("gwi_metric") == "uptime_seconds"
    )
    assert uptime.state == "123.0"
    clients = next(
        state for state in states if state.attributes.get("gwi_metric") == "clients"
    )
    assert clients.state == "unknown"
    connected = next(
        state
        for state in states
        if state.attributes.get("gwi_kind") == "point"
        and state.entity_id.startswith("binary_sensor.")
    )
    assert connected.state == "on"
    dashboard = yaml.safe_load((ROOT / "dashboard.yaml").read_text(encoding="utf-8"))
    rendered = Template(
        dashboard["views"][0]["cards"][1]["content"], real_hass
    ).async_render(parse_result=False)
    assert "| Sala | Online | — |" in rendered
    assert "|:---|:---|---:|\n| Sala" in rendered
    rendered = Template(
        dashboard["views"][0]["cards"][2]["content"], real_hass
    ).async_render(parse_result=False)
    assert "The client list is unavailable in local mode." in rendered
    assert await real_hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
async def test_google_controls_history_permissions_and_readback_in_real_ha(real_hass, monkeypatch):
    from datetime import UTC, datetime, timedelta
    from types import MappingProxyType, SimpleNamespace

    from homeassistant.core import Context
    from homeassistant.exceptions import HomeAssistantError

    from custom_components.google_wifi_insights.api import CloudClient
    from custom_components.google_wifi_insights.capabilities import apply_extras
    from custom_components.google_wifi_insights.history import (
        apply_history,
        normalize_history,
        request_window,
    )
    from custom_components.google_wifi_insights.model import cloud_network
    group = {"name": "Test WiFi", "accessPoints": [{"id": "p", "accessPointSettings": {"lightingSettings": {"intensity": 40}}}],
             "groupSettings": {"familyHubSettings": {"stationPolicies": []}, "lanSettings": {"prioritizedStation": {}}}}
    station = {"id": "s", "friendlyName": "Client", "connected": True, "apId": "p"}
    period, _ = request_window("hour", datetime(2026, 10, 13, 12, tzinfo=UTC))
    rows = [{"timestamp": (datetime(2026, 10, 12, 12, tzinfo=UTC)+timedelta(hours=i)).isoformat(), "bytesReceived": "1000", "bytesTransmitted": "100"} for i in range(1, 25)]
    data = normalize_history({"wanUsageRecords": rows, "stationUsageRecords": [{"station": {"id": "s"}, "usageRecords": rows}]}, period)
    requests = []
    async def snapshot(client):
        client._histories = {"home": {"hour": data}}
        network = cloud_network(group, {}, [station], {})
        apply_history(network, {"hour": data})
        apply_extras(network, group, {}, {}, {})
        network["operations"] = copy.deepcopy(client._operations.get("home", []))
        network["last_operation"] = network["operations"][-1]["state"] if network["operations"] else "NONE"
        return {"networks": {"home": network}}
    async def action(client, nid, name, path, method, payload, **metadata):
        requests.append((nid, name, path, method, payload))
        if name == "set_point_light":
            group["accessPoints"][0]["accessPointSettings"]["lightingSettings"]["intensity"] = payload["intensity"]
        if name in ("pause_client", "resume_client"):
            group["groupSettings"]["familyHubSettings"]["stationPolicies"] = [{"stationId": "s", "blockingPolicy": {"expiryTimestamp": "1970-01-01T00:00:00Z"}}] if payload["blocked"] else []
        if name == "prioritize_client":
            group["groupSettings"]["lanSettings"]["prioritizedStation"] = payload
        result = {"state": "CREATED", "action": name, "operation_id": "mock", "requested_at": datetime.now(UTC).isoformat()}
        client._operations.setdefault(nid, []).append(result)
        if metadata.get("on_update"):
            metadata["on_update"]()
        return result
    monkeypatch.setattr(CloudClient, "snapshot", snapshot)
    monkeypatch.setattr(CloudClient, "action", action)
    entry = config_entries.ConfigEntry(data={"mode": "cloud", "email": "test@example.invalid", "master_token": "dummy", "android_id": "0"*16}, domain=DOMAIN, discovery_keys=MappingProxyType({}), options={}, source="user", title="WiFi", unique_id="wifi", version=1, minor_version=1, subentries_data=None)
    await real_hass.config_entries.async_add(entry)
    await real_hass.async_block_till_done()
    states = {s.attributes["gwi_metric"]: s for s in real_hass.states.async_all() if s.attributes.get("gwi_kind") == "client"}
    assert states["usage_24h_download"].state == "24000" and states["usage_24h_download"].attributes["state_class"] == "measurement"
    assert states["wifi_test_mbps"].state == "unknown"
    response = await real_hass.services.async_call(DOMAIN, "get_usage_history", {"entity_id": states["point"].entity_id, "resolution": "hour"}, blocking=True, return_response=True)
    assert len(response["series"]) == 24 and response["series"][0]["download_bytes"] == 1000
    light = next(s for s in real_hass.states.async_all() if s.attributes.get("gwi_metric") == "set_point_light")
    await real_hass.services.async_call("number", "set_value", {"entity_id": light.entity_id, "value": 41}, blocking=True)
    assert float(real_hass.states.get(light.entity_id).state) == 41
    paused = states["internet_paused"].entity_id
    await real_hass.services.async_call("switch", "turn_on", {"entity_id": paused}, blocking=True)
    assert real_hass.states.get(paused).state == "on"
    await real_hass.services.async_call("switch", "turn_off", {"entity_id": paused}, blocking=True)
    assert real_hass.states.get(paused).state == "off"
    await real_hass.services.async_call("button", "press", {"entity_id": states["prioritize_client"].entity_id}, blocking=True)
    assert real_hass.states.get(states["priority_until"].entity_id).state != "unknown"
    assert [r[1] for r in requests] == ["set_point_light", "pause_client", "resume_client", "prioritize_client"]
    # A forged state cannot redirect a registry entity into another network.
    point = real_hass.states.get(states["point"].entity_id)
    real_hass.states.async_set(point.entity_id, point.state, {**point.attributes, "gwi_network_id": "foreign"})
    with pytest.raises(HomeAssistantError, match="verified"):
        await real_hass.services.async_call(DOMAIN, "get_usage_history", {"entity_id": point.entity_id}, blocking=True, return_response=True)
    await entry.runtime_data.async_refresh()
    real_hass.auth = SimpleNamespace(async_get_user=AsyncMock(return_value=SimpleNamespace(is_admin=False)))
    with pytest.raises(HomeAssistantError, match="administrator"):
        await real_hass.services.async_call(DOMAIN, "google_action", {"entity_id": point.entity_id, "action": "pause_client"}, context=Context(user_id="nonadmin"), blocking=True)
    assert len(requests) == 4
    assert await real_hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
async def test_real_ha_roaming_services_and_radio_discovery(real_hass, monkeypatch):
    from types import MappingProxyType

    from homeassistant.exceptions import HomeAssistantError

    from custom_components.google_wifi_insights.api import CloudClient
    from custom_components.google_wifi_insights.model import cloud_network
    from custom_components.google_wifi_insights.shelly_radio import ShellyRadio

    foreign_entry = config_entries.ConfigEntry(data={}, domain="shelly", discovery_keys=MappingProxyType({}), options={}, source="user", title="Shelly", unique_id="shelly", version=1, minor_version=1, subentries_data=None)
    real_hass.config_entries._entries[foreign_entry.entry_id] = foreign_entry
    device_registry.async_get(real_hass).async_get_or_create(config_entry_id=foreign_entry.entry_id,
        identifiers={("shelly", "aabbccddeeff")}, connections={(device_registry.CONNECTION_NETWORK_MAC, "aa:bb:cc:dd:ee:ff")}, name="Lamp", manufacturer="Shelly", model="Gen4")
    station = {"id": "s", "friendlyName": "Shelly lamp", "connected": True, "apId": "p", "ipAddress": "192.168.50.2"}
    snapshot = {"networks": {"home": cloud_network({"accessPoints": [{"id": "p"}]}, {}, [station], {}, {"s": "aa:bb:cc:dd:ee:ff"})}}
    monkeypatch.setattr(CloudClient, "snapshot", AsyncMock(side_effect=lambda: copy.deepcopy(snapshot)))
    ready = False
    roam = {"rssi_thr": -80, "interval": 60}
    writes = []
    async def rpc(self, host, method, params=None):
        if not ready:
            raise HomeAssistantError("Not yet available")
        if method == "Shelly.GetDeviceInfo":
            return {"mac": "aabbccddeeff", "gen": 4, "auth_en": False}
        if method == "WiFi.GetConfig":
            return {"roam": copy.copy(roam)}
        if method == "WiFi.GetStatus":
            return {"status": "got ip", "rssi": -72, "channel": 6, "bssid": "11:22:33:44:55:66"}
        writes.append(params)
        roam.update(params["config"]["roam"])
        return {}
    monkeypatch.setattr(ShellyRadio, "_rpc", rpc)
    entry = config_entries.ConfigEntry(data={"mode": "cloud", "email": "test@example.invalid", "master_token": "dummy", "android_id": "0"*16}, domain=DOMAIN, discovery_keys=MappingProxyType({}), options={}, source="user", title="WiFi", unique_id="wifi", version=1, minor_version=1, subentries_data=None)
    await real_hass.config_entries.async_add(entry)
    await real_hass.async_block_till_done()
    assert not any(s.attributes.get("gwi_metric") in ("roam_rssi_threshold", "roam_interval") for s in real_hass.states.async_all())
    ready = True
    await entry.runtime_data.async_refresh()
    await real_hass.async_block_till_done()
    states = {s.attributes["gwi_metric"]: s for s in real_hass.states.async_all() if s.attributes.get("gwi_kind") == "client"}
    assert states["rssi"].state == "-72.0" and states["wifi_bssid"].state == "11:22:33:44:55:66"
    threshold, interval, enabled = (states[k].entity_id for k in ("roam_rssi_threshold", "roam_interval", "roaming_enabled"))
    await real_hass.services.async_call("number", "set_value", {"entity_id": threshold, "value": -70}, blocking=True)
    assert float(real_hass.states.get(threshold).state) == -70
    await real_hass.services.async_call("number", "set_value", {"entity_id": interval, "value": 120}, blocking=True)
    await real_hass.services.async_call("switch", "turn_off", {"entity_id": enabled}, blocking=True)
    assert real_hass.states.get(enabled).state == "off" and float(real_hass.states.get(interval).state) == 0
    await real_hass.services.async_call("switch", "turn_on", {"entity_id": enabled}, blocking=True)
    assert real_hass.states.get(enabled).state == "on" and float(real_hass.states.get(interval).state) == 120
    assert writes == [{"config": {"roam": {"rssi_thr": -70}}}, {"config": {"roam": {"interval": 120}}}, {"config": {"roam": {"interval": 0}}}, {"config": {"roam": {"interval": 120}}}]
    ready = False
    await entry.runtime_data.async_refresh()
    await real_hass.async_block_till_done()
    assert real_hass.states.get(threshold).state == "unavailable"
    assert real_hass.states.get(states["rssi"].entity_id).state == "unknown"
    assert await real_hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
async def test_device_identity_sensors_names_links_and_ids_remain_stable(real_hass, monkeypatch):
    import json
    from types import MappingProxyType

    from custom_components.google_wifi_insights.api import CloudClient
    from custom_components.google_wifi_insights.const import DOMAIN
    from custom_components.google_wifi_insights.diagnostics import (
        async_get_config_entry_diagnostics,
    )
    from custom_components.google_wifi_insights.model import cloud_network

    foreign_entry = config_entries.ConfigEntry(
        data={"host":"192.168.50.2"}, domain="shelly", discovery_keys=MappingProxyType({}),
        options={}, source=config_entries.SOURCE_USER, title="Existing device",
        unique_id="existing-lamp", version=1, minor_version=1, subentries_data=None,
    )
    # Register an existing integration without starting its network transport.
    real_hass.config_entries._entries[foreign_entry.entry_id] = foreign_entry
    registry = device_registry.async_get(real_hass)
    foreign = registry.async_get_or_create(
        config_entry_id=foreign_entry.entry_id,
        identifiers={("shelly","aa:bb:cc:dd:ee:ff")},
        connections={(device_registry.CONNECTION_NETWORK_MAC,"aa:bb:cc:dd:ee:ff")},
        name="Generic lamp",
    )
    registry.async_update_device(foreign.id, name_by_user="Kitchen Light")
    station = {"id":"opaque","friendlyName":"0123456789ab*","connected":True,"apId":"point",
               "ipAddress":"192.168.50.2","wirelessBand":"BAND_2400_MHZ","connectionType":"WIRELESS"}
    snapshot = {"networks":{"home":cloud_network(
        {"accessPoints":[{"id":"point"}]}, {"apStatuses":[{"apId":"point","apState":"AP_ONLINE"}]},
        [station], {"stationMetrics":[{"station":station,"traffic":{},"speed":{"speedRating":"OK"}}]},
        {"opaque":"aa:bb:cc:dd:ee:ff"},
    )}}
    mocked = AsyncMock(side_effect=lambda:copy.deepcopy(snapshot))
    monkeypatch.setattr(CloudClient,"snapshot",mocked)
    entry = config_entries.ConfigEntry(
        data={"mode":"cloud","email":"test@example.invalid","master_token":"dummy","android_id":"0"*16},
        domain=DOMAIN, discovery_keys=MappingProxyType({}), options={}, source=config_entries.SOURCE_USER,
        title="Test WiFi", unique_id="test-wifi", version=1, minor_version=1, subentries_data=None,
    )
    await real_hass.config_entries.async_add(entry)
    await real_hass.async_block_till_done()
    assert entry.state == config_entries.ConfigEntryState.LOADED
    client_states = {s.attributes["gwi_metric"]:s for s in real_hass.states.async_all()
                     if s.attributes.get("gwi_kind")=="client"}
    assert client_states["ip_address"].state == "192.168.50.2"
    assert client_states["mac_address"].state == "aa:bb:cc:dd:ee:ff"
    assert client_states["download_mbps"].state == "0.0" and client_states["upload_mbps"].state == "0.0"
    assert client_states["signal_quality"].state == "good" and client_states["rssi"].state == "unknown"
    assert client_states["signal_quality"].attributes["google_rating"] == "OK"
    assert client_states["point"].attributes["device_name"] == "0123456789ab* (Kitchen Light)"
    own = registry.async_get_device_by_identifier((DOMAIN,f"{entry.entry_id}:home:client:opaque"),entry.entry_id)
    assert own.id != foreign.id and own.name == "0123456789ab* (Kitchen Light)"
    assert own.configuration_url == f"homeassistant://config/devices/device/{foreign.id}"
    ip_entity = entity_registry.async_get(real_hass).async_get(client_states["ip_address"].entity_id)
    assert ip_entity.device_id == own.id and ip_entity.disabled_by is None
    original_ids = {s.entity_id for s in real_hass.states.async_all()}
    registry.async_update_device(foreign.id, name_by_user="Renamed Kitchen Light")
    await entry.runtime_data.async_refresh()
    await real_hass.async_block_till_done()
    assert registry.async_get(own.id).name == "0123456789ab* (Renamed Kitchen Light)"
    assert real_hass.states.get(client_states["point"].entity_id).attributes["device_name"] == "0123456789ab* (Renamed Kitchen Light)"
    assert {s.entity_id for s in real_hass.states.async_all()} == original_ids
    registry.async_update_device(own.id, name_by_user="My WiFi name")
    await entry.runtime_data.async_refresh()
    await real_hass.async_block_till_done()
    assert real_hass.states.get(client_states["point"].entity_id).attributes["device_name"] == "My WiFi name"
    assert registry.async_get(foreign.id).name_by_user == "Renamed Kitchen Light"
    diagnostics = json.dumps(await async_get_config_entry_diagnostics(real_hass,entry))
    assert "aa:bb:cc" not in diagnostics and "192.168.50.2" not in diagnostics and "Kitchen" not in diagnostics
    assert await real_hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
async def test_cloud_dynamic_clients_missing_connection_and_reauth(
    real_hass, monkeypatch
):
    from custom_components.google_wifi_insights.api import CloudClient
    from custom_components.google_wifi_insights.model import cloud_network

    def sample(clients):
        return {
            "networks": {
                "home": cloud_network(
                    {"id": "home", "accessPoints": [
                        {"id": "ap1", "accessPointProperties": {"isGroupRoot": True}},
                        {"id": "ap2"},
                    ]},
                    {"apStatuses": [
                        {"apId": "ap1", "apState": "AP_ONLINE"},
                        {"apId": "ap2", "apState": "AP_ONLINE"},
                    ]},
                    clients,
                    {"stationMetrics": [{
                        "station": {"id":"phone","connected":True,"apId":"ap1"},
                        "speed": {"speedRating":"WEAK","numericSpeedRating":"19"},
                    }],"meshMetrics":[{"apId":"ap2","speed":{"speedRating":"OK","numericSpeedRating":"50"}}]},
                )
            }
        }

    snapshot = sample(
        [{"id": "phone", "friendlyName": "Phone", "connected": True, "apId": "ap1"}]
    )
    monkeypatch.setattr(CloudClient, "snapshot", AsyncMock(return_value=snapshot))
    monkeypatch.setattr(CloudClient, "async_validate_account", AsyncMock(return_value=1))
    result = await real_hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await real_hass.config_entries.flow.async_configure(
        result["flow_id"], {"next_step_id": "cloud"}
    )
    result = await real_hass.config_entries.flow.async_configure(
        result["flow_id"], {"next_step_id": "cloud_master"}
    )
    result = await real_hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            "email": "test@example.invalid",
            "oauth_token": "aas_et/fictional",
            "android_id": "0" * 16,
        },
    )
    assert result["type"] == "create_entry"
    entry = result["result"]
    await real_hass.async_block_till_done()
    assert entry.state == config_entries.ConfigEntryState.LOADED
    phone = next(
        state
        for state in real_hass.states.async_all()
        if state.entity_id.startswith("device_tracker.")
    )
    assert phone.state == "home"
    assert phone.attributes["point"] == "ap1"
    assert phone.attributes["quality_source"] == "google_connection_rating"
    assert phone.attributes["signal_quality"] == "weak" and phone.attributes["rssi"] is None
    mesh = next(state for state in real_hass.states.async_all()
                if state.attributes.get("gwi_metric") == "mesh_quality"
                and state.attributes.get("gwi_item_id") == "ap2")
    assert mesh.state == "good" and mesh.attributes["root_ap_id"] == "ap1"
    assert mesh.attributes["mesh_rating"] == "OK"
    assert mesh.attributes["device_class"] == "enum"
    import json

    from custom_components.google_wifi_insights.diagnostics import (
        async_get_config_entry_diagnostics,
    )

    diagnostics = json.dumps(await async_get_config_entry_diagnostics(real_hass, entry))
    assert (
        "fictional" not in diagnostics
        and "test@example" not in diagnostics
        and "Phone" not in diagnostics
    )
    snapshot2 = sample(
        [
            {"id": "phone", "friendlyName": "Phone"},
            {
                "id": "new",
                "friendlyName": "New client",
                "connected": True,
                "apId": "ap1",
            },
        ]
    )
    entry.runtime_data.async_set_updated_data(copy.deepcopy(snapshot2))
    await real_hass.async_block_till_done()
    trackers = [
        state
        for state in real_hass.states.async_all()
        if state.entity_id.startswith("device_tracker.")
    ]
    assert len(trackers) == 2
    assert real_hass.states.get(phone.entity_id).state == "unavailable"
    reauth = await real_hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_REAUTH, "entry_id": entry.entry_id},
        data=entry.data,
    )
    assert reauth["step_id"] == "cloud"
    reauth = await real_hass.config_entries.flow.async_configure(
        reauth["flow_id"], {"next_step_id": "cloud_master"}
    )
    reauth = await real_hass.config_entries.flow.async_configure(
        reauth["flow_id"],
        {
            "email": "test@example.invalid",
            "oauth_token": "aas_et/new-fictional",
            "android_id": "0" * 16,
        },
    )
    assert reauth["type"] == "abort" and reauth["reason"] == "reauth_successful"
    await real_hass.async_block_till_done()
    assert entry.data["master_token"] == "aas_et/new-fictional"
    assert await real_hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
async def test_local_setup_with_nonstandard_foreign_identifiers(real_hass, monkeypatch):
    """Legacy registry identifiers must not prevent the first local refresh."""
    from types import MappingProxyType

    from custom_components.google_wifi_insights.api import LocalClient
    from custom_components.google_wifi_insights.identity import ha_records
    from custom_components.google_wifi_insights.model import local_point

    foreign_entry = config_entries.ConfigEntry(
        data={}, domain="homekit", discovery_keys=MappingProxyType({}), options={},
        source="user", title="Synthetic bridge", unique_id="legacy-bridge",
        version=1, minor_version=1, subentries_data=None,
    )
    real_hass.config_entries._entries[foreign_entry.entry_id] = foreign_entry
    registry = device_registry.async_get(real_hass)
    foreign = registry.async_get_or_create(
        config_entry_id=foreign_entry.entry_id,
        identifiers={("example_host", "192.168.50.9")},
        connections={(device_registry.CONNECTION_NETWORK_MAC, "aa:bb:cc:dd:ee:ff")},
        name="Synthetic bridge",
    )
    # Model a nonstandard identifier already present in a restored registry.
    foreign.identifiers.add(("homekit", "bridge", "legacy-id"))
    original_identifiers = foreign.identifiers.copy()
    point = local_point(
        {"host": "192.168.50.1", "name": "Test point"},
        {"system": {"uptime": 123}, "wan": {"online": True}},
    )
    snapshot = {"networks": {"local": {
        "name": "Test WiFi", "source": "local_status",
        "points": {"192.168.50.1": point}, "clients": {},
        "diagnosis": "insufficient_radio_data",
    }}}
    monkeypatch.setattr(LocalClient, "snapshot", AsyncMock(return_value=snapshot))
    entry = config_entries.ConfigEntry(
        data={"mode": "local", "points": [{"name": "Test point", "host": "192.168.50.1"}]},
        domain=DOMAIN, discovery_keys=MappingProxyType({}), options={}, source="user",
        title="Test WiFi", unique_id="nonstandard-identifiers", version=1,
        minor_version=1, subentries_data=None,
    )
    await real_hass.config_entries.async_add(entry)
    await real_hass.async_block_till_done()
    assert entry.state == config_entries.ConfigEntryState.LOADED
    assert any(s.attributes.get("gwi_metric") == "uptime_seconds"
               for s in real_hass.states.async_all())
    records = {record["id"]: record for record in ha_records(real_hass)}
    assert records[foreign.id]["ips"] == {"192.168.50.9"}
    assert records[foreign.id]["macs"] == {"aa:bb:cc:dd:ee:ff"}
    assert foreign.identifiers == original_identifiers
    assert await real_hass.config_entries.async_unload(entry.entry_id)
