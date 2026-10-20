"""Avoid incorrect aliases from stale, ambiguous, conflicting or truncated addresses."""
import pytest
from gwi_test.identity import (
    identifier_pairs,
    identity_index,
    matter_interface_addresses,
    normalize_ip,
    normalize_mac,
    resolve_client,
)


def record(id, name="Lamp", macs=(), ips=(), user=False):
    return {"id": id, "name": name, "macs": set(macs), "ips": set(ips), "user_name": user}


@pytest.mark.parametrize("value", ["0123456789ab*", "11:22:33", "unknown", None, "00:00:00:00:00:00"])
def test_no_mac_inference_from_partial_google_names(value):
    assert normalize_mac(value) is None


def test_normalize_addresses_and_prefer_exact_mac_over_ip():
    assert normalize_mac("AABB.CCDD.EEFF") == "aa:bb:cc:dd:ee:ff"
    assert normalize_ip("fe80::1%wlan0") == "fe80::1"
    index = identity_index([record("mac", macs=["aa:bb:cc:dd:ee:ff"]), record("wrong", ips=["192.168.50.2"])])
    client = {"mac_address": "AA-BB-CC-DD-EE-FF", "ip_address": "192.168.50.2", "connected": True}
    match, method = resolve_client(client, index)
    assert match["id"] == "mac" and method == "mac"


def test_current_ips_only_and_never_override_a_conflicting_mac():
    index = identity_index([record("ip", ips=["192.168.50.2", "fd00::2"])])
    client = {"ip_addresses": ["fd00::2"], "connected": True}
    assert resolve_client(client, index)[1] == "ip"
    client["connected"] = False
    assert resolve_client(client, index) == (None, None)
    index = identity_index([record("ip", macs=["11:22:33:44:55:66"], ips=["192.168.50.2"])])
    assert resolve_client({"mac_address":"aa:bb:cc:dd:ee:ff", "connected":True, "ip_address":"192.168.50.2"}, index) == (None, None)


def test_duplicate_ips_and_conflicting_user_names_remain_unidentified():
    client = {"connected": True, "ip_address": "192.168.50.2"}
    index = identity_index([record("one", ips=["192.168.50.2"]), record("two", ips=["192.168.50.2"])])
    assert resolve_client(client, index) == (None, "ambiguous")
    assert resolve_client(client, index, ["192.168.50.2"]) == (None, None)
    mac = "aa:bb:cc:dd:ee:ff"
    index = identity_index([record("one", "Kitchen", [mac], user=True), record("two", "Bedroom", [mac], user=True)])
    assert resolve_client({"mac_address":mac}, index) == (None, "ambiguous")


def test_same_mac_across_integrations_uses_user_name_without_merging():
    mac = "aa:bb:cc:dd:ee:ff"
    index = identity_index([record("one", "Generic TV", [mac]), record("two", "Living room TV", [mac], user=True)])
    match, method = resolve_client({"mac_address":mac}, index)
    assert match["id"] == "two" and method == "mac"


def test_cached_matter_interfaces_decode_mac_ipv4_ipv6_and_ignore_inactive_ips():
    macs, ips = matter_interface_addresses([{"1":True,"4":"qrvM3e7/","5":["wKgyAg=="],"6":["/oAAAAAAAAAAAAAAAAAAAQ=="]}])
    assert macs == {"aa:bb:cc:dd:ee:ff"}
    assert ips == {"192.168.50.2", "fe80::1"}
    macs, ips = matter_interface_addresses([{"1":False,"4":"qrvM3e7/","5":["wKgyAg=="]}])
    assert macs and not ips
    assert matter_interface_addresses([{"1":True,"4":"bad","5":["bad"]}]) == (set(), set())


@pytest.mark.parametrize("invalid", [
    ("homekit", "bridge", "legacy-id"), ("legacy",), (), "ab", None, 42,
    ("example", None), (1, "id"),
])
def test_nonstandard_registry_identifiers_are_ignored(invalid):
    values = [invalid, ("example", "identifier"), ["example_host", "192.168.50.2"]]
    assert list(identifier_pairs(values)) == [
        ("example", "identifier"), ("example_host", "192.168.50.2"),
    ]
