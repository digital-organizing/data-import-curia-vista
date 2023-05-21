from conftest import SERVICE_URL
from odata2sql.odata import SettingsBuilder


def test_sync_unconfigured_entities(client):
    assert SettingsBuilder(SERVICE_URL).build().sync_unconfigured_entities
    assert SettingsBuilder(SERVICE_URL).sync_config(
        {'sync_unconfigured_entities': True}).build().sync_unconfigured_entities
    assert not SettingsBuilder(SERVICE_URL).sync_config(
        {'sync_unconfigured_entities': False}).build().sync_unconfigured_entities
