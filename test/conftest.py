import os

import pyodata
import pytest
import requests

from odata2sql.odata import Context, SettingsBuilder

SERVICE_URL = 'https://ws.parlament.ch/odata.svc'


@pytest.fixture()
def local_metadata() -> bytes:
    local_metadata_filename = os.path.join(os.path.dirname(__file__), '../doc/metadata.xml')
    with open(local_metadata_filename, "rb") as metadata_file:
        return metadata_file.read()


@pytest.fixture()
def live_metadata() -> bytes:
    resp = requests.get(f"{SERVICE_URL}/$metadata")
    assert resp.status_code == 200
    assert resp.headers['content-type'] == 'application/xml;charset=utf-8'
    return resp.content


@pytest.fixture
def client(local_metadata):
    """Client with Curia Vista's OData schema"""
    return pyodata.Client(SERVICE_URL, requests.Session(), metadata=local_metadata)


@pytest.fixture
def settings():
    return SettingsBuilder(SERVICE_URL).build()


@pytest.fixture
def context(client, settings):
    return Context(client, settings)
