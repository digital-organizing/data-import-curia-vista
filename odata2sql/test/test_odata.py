import os

import pytest
import requests

import pyodata
from odata2sql.odata import Context, Multiplicity

SERVICE_URL = 'https://ws.parlament.ch/odata.svc'
metadata_fixture = os.path.join(os.path.dirname(__file__), 'fixture/curia-vista-metadata.xml')


@pytest.fixture
def client():
    with open(metadata_fixture, 'rb') as metadata_file:
        local_metadata = metadata_file.read()
    return pyodata.Client(SERVICE_URL, requests.Session(), metadata=local_metadata)


@pytest.fixture
def context(client):
    return Context(client, None, None, SERVICE_URL)


def test_adjust_next_url_not_needed(client):
    o = Context(client, None, None, SERVICE_URL)
    assert o.adjust_next_url(
        "https://ws.parlament.ch/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'") == "https://ws.parlament.ch/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'"


def test_adjust_next_url(client):
    o = Context(client, None, None, 'http://localhost:8080/odata.svc')
    assert o.adjust_next_url(
        "https://ws.parlament.ch/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'") == "http://localhost:8080/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'"


def test_get_dependency_with_multiplicity(context):
    transcript = context.client.schema.entity_type('Transcript')
    results = set(context.get_dependency_with_multiplicity(transcript))
    assert results == {
        Multiplicity(transcript, '*', context.client.schema.entity_type('MemberCouncil'), '1'),
        Multiplicity(transcript, '*', context.client.schema.entity_type('Subject'), '1'),
        Multiplicity(transcript, '*', context.client.schema.entity_type('Business'), '0..1')
    }
