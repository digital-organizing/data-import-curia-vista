from uuid import UUID

import pytest

from odata2sql.odata import SettingsBuilder
from odata2sql.test.conftest import SERVICE_URL


def test_url(client_curia_vista):
    assert SettingsBuilder(SERVICE_URL).build().url == SERVICE_URL
    assert SettingsBuilder().url(SERVICE_URL).build().url == SERVICE_URL


def test_missing_url():
    with pytest.raises(ValueError) as e:
        SettingsBuilder().build()
    assert str(e.value) == 'URL not specified!'


def test_missing_odata_svc_suffix():
    with pytest.raises(ValueError) as e:
        SettingsBuilder('https://notlookingood.example.com').build()
    assert str(e.value) == 'Expecting OData URLs to end with /odata.svc'


def test_default_odata_server_max_connections():
    assert SettingsBuilder(SERVICE_URL).build().odata_server_max_connections == 20


def test_faulty_odata_server_max_connections():
    with pytest.raises(ValueError) as e:
        SettingsBuilder(SERVICE_URL).odata_server_max_connections(-1).build()
    assert str(e.value) == 'Invalid connection count: -1'


def test_custom_session_id():
    assert SettingsBuilder(SERVICE_URL).session_id(
        UUID('48385914-1ca9-46ba-8839-92a8d6c380b9')).build().session_id == UUID('48385914-1ca9-46ba-8839-92a8d6c380b9')
