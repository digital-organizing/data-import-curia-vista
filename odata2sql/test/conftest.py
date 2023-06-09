import os

import pyodata
import pytest
import requests

from odata2sql.odata import Context, SettingsBuilder


@pytest.fixture
def client():
    """Client with OData schema based on Curia Vista but stripped down, tailored to our needs"""
    with open(fixture_directory + '/metadata.xml', 'rb') as metadata_file:
        local_metadata = metadata_file.read()
    return pyodata.Client(SERVICE_URL, requests.Session(), metadata=local_metadata)


@pytest.fixture
def settings():
    return SettingsBuilder(SERVICE_URL).build()


@pytest.fixture
def context(client, settings):
    return Context(client, settings)


SERVICE_URL = 'https://ws.parlament.ch/odata.svc'
fixture_directory = os.path.join(os.path.dirname(__file__), 'fixture/')
