import pytest

from odata2sql.odata import SettingsBuilder
from odata2sql.test.conftest import SERVICE_URL


def test_sync_unconfigured_entities(client_curia_vista):
    assert SettingsBuilder(SERVICE_URL).build().sync_unconfigured_entities
    assert SettingsBuilder(SERVICE_URL).sync_config(
        {'sync_unconfigured_entities': True}).build().sync_unconfigured_entities
    assert not SettingsBuilder(SERVICE_URL).sync_config(
        {'sync_unconfigured_entities': False}).build().sync_unconfigured_entities


@pytest.mark.parametrize('config, configured_entities, include, skip, comment', [
    ({}, set(), set(), set(), 'Empty configuration'),
    ({'entities': {'Bill': {}}}, {'Bill'}, {'Bill'}, set(), 'Sync per default'),
    ({'entities': {'Bill': {'sync': True}}}, {'Bill'}, {'Bill'}, set(), 'Sync when told so'),
    ({'entities': {'Bill': {'sync': False}}}, {'Bill'}, set(), {'Bill'}, 'Skip when not sync'),
])
def test_configured_entities(config, configured_entities, include, skip, comment):
    settings = SettingsBuilder(SERVICE_URL).sync_config(config).build()
    assert settings.configured_entities == configured_entities
    assert settings.include == include
    assert settings.skip == skip


@pytest.mark.parametrize('config, entity_type_name, filter_, comment', [
    ({}, 'Bill', None, 'Empty configuration'),
    ({'entities': {}}, 'Bill', None, 'Entity type not existing'),
    ({'entities': {'Bill': {}}}, 'Bill', None, 'No filter'),
    ({'entities': {'Bill': {'filter': "ID eq '1'"}}}, 'Bill', "ID eq '1'", 'Individual filter'),
    ({'filter': "Language eq 'CH'", 'entities': {'Bill': {'filter': "ID eq '1'"}}}, 'Bill',
     "(Language eq 'CH') and (ID eq '1')", 'Individual and global filter'),
])
def test_filter(config, entity_type_name, filter_, comment):
    settings = SettingsBuilder(SERVICE_URL).sync_config(config).build()
    assert settings.odata_filter_for_entity_type(entity_type_name) == filter_


@pytest.mark.parametrize('config, dependant, principal, comment', [
    ({}, 'Voting', None, 'Empty configuration'),
    ({'entities': {}}, 'Voting', None, 'Dependant not existing'),
    ({'entities': {'Voting': {}}}, 'Voting', None, 'Dependant not configured'),
    ({'entities': {'Voting': {'sync_by': 'Vote'}}}, 'Voting', 'Vote', 'Voting synced by Vote'),
])
def test_sync_by_fk(config, dependant, principal, comment):
    settings = SettingsBuilder(SERVICE_URL).sync_config(config).build()
    assert settings.odata_sync_by_fk(dependant) == principal


@pytest.mark.parametrize('config, entity_type_name, properties, comment', [
    ({}, 'Person', None, 'Empty configuration'),
    ({'entities': {}}, 'Person', None, 'Entity type not existing'),
    ({'entities': {'Person': {}}}, 'Person', None, 'Properties not configured'),
    ({'entities': {'Person': {'selected_properties': ['ID', 'Language']}}}, 'Person', ['ID', 'Language'],
     'Person has two properties'),
])
def test_selected_properties(config, entity_type_name, properties, comment):
    settings = SettingsBuilder(SERVICE_URL).sync_config(config).build()
    assert settings.odata_selected_properties(entity_type_name) == properties
