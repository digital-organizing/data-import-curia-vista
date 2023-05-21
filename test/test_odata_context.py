import pytest

from odata2sql.odata import Multiplicity, Context, SettingsBuilder
from odata2sql.test.conftest import SERVICE_URL


def test_adjust_next_url_not_needed(context):
    assert context.adjust_next_url(
        "https://ws.parlament.ch/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'") == "https://ws.parlament.ch/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'"


def test_adjust_next_url(client):
    ctx = Context(client, SettingsBuilder().url('http://localhost:8080/odata.svc').build())
    assert ctx.adjust_next_url(
        "https://ws.parlament.ch/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'") == "http://localhost:8080/odata.svc/MemberCouncil?$inlinecount=allpages&$skiptoken=4090,'RM'"


def test_get_dependency_with_multiplicity(context):
    transcript = context.client.schema.entity_type('Transcript')
    results = set(context.get_dependencies_with_multiplicity(transcript))
    assert results == {
        Multiplicity(transcript, '*', context.client.schema.entity_type('MemberCouncil'), '1'),
        Multiplicity(transcript, '*', context.client.schema.entity_type('Subject'), '1'),
        Multiplicity(transcript, '*', context.client.schema.entity_type('Business'), '0..1')
    }


@pytest.mark.parametrize('dependant_name, principal_name, expected,comment', [
    ('Tags', 'Tags', False, 'Does not exist: Self reference'),
    ('Voting', 'Session', False, 'Does not exist: Implicit reference'),
    ('BusinessRole', 'External', True, '1 multiplicity on principal'),
    ('External', 'BusinessRole', False, '* multiplicity on principal'),
    ('Business', 'Council', False, '0..1 multiplicity on principal'),
])
def test_can_be_synced_by(context, dependant_name, principal_name, expected, comment):
    dependant = context.client.schema.entity_type(dependant_name)
    principal = context.client.schema.entity_type(principal_name)
    assert expected == context.can_be_synced_by(dependant, principal), comment


@pytest.mark.parametrize('config, entity_type_names_to_include, comment', [
    ({'sync_unconfigured_entities': False, 'entities': {'Tags': {'sync': True}}}, {'Tags'}, 'No dependencies'),
    ({'sync_unconfigured_entities': False, 'entities': {'Canton': {'sync': True}, 'Party': {'sync': True}}},
     {'Canton', 'Party'}, 'Two entity types, no dependencies'),
    ({'sync_unconfigured_entities': False, 'entities': {'PersonAddress': {'sync': True}}},
     {'PersonAddress', 'Person', 'MemberCouncil'}, 'PersonAddress has two dependencies'),
    ({}, {'Bill', 'BillLink', 'BillStatus', 'Business', 'BusinessResponsibility', 'BusinessRole', 'BusinessStatus',
          'BusinessType', 'Canton', 'Citizenship', 'Committee', 'Council', 'External', 'LegislativePeriod', 'Meeting',
          'MemberCommittee', 'MemberCommitteeHistory', 'MemberCouncil', 'MemberCouncilHistory', 'MemberParlGroup',
          'MemberParty', 'Objective', 'ParlGroup', 'ParlGroupHistory', 'Party', 'Person', 'PersonAddress',
          'PersonCommunication', 'PersonEmployee', 'PersonInterest', 'PersonOccupation', 'Preconsultation',
          'Publication', 'Rapporteur', 'RelatedBusiness', 'Resolution', 'SeatOrganisationNr', 'Session', 'Subject',
          'SubjectBusiness', 'Tags', 'Transcript', 'Vote', 'Voting'}, "All entity types"),
])
def test_context_include(client, config, entity_type_names_to_include, comment):
    ctx = Context(client, SettingsBuilder(SERVICE_URL).sync_config(config).build())
    assert {et.name for et in ctx.include} == entity_type_names_to_include, comment


@pytest.mark.parametrize('config, entity_type_names_to_skip, comment', [
    ({'sync_unconfigured_entities': False}, set(), 'Skipping must be requested explicitly #1'),
    ({'sync_unconfigured_entities': True}, set(), 'Skipping must be requested explicitly #2'),
    ({'entities': {'Tags': {'sync': False}}}, {'Tags'}, 'Skip Tags'),
])
def test_context_skip(client, config, entity_type_names_to_skip, comment):
    ctx = Context(client, SettingsBuilder(SERVICE_URL).sync_config(config).build())
    assert {et.name for et in ctx.skip} == entity_type_names_to_skip, comment


def test_setting_invalid_entity_type_name(client):
    with pytest.raises(ValueError) as e:
        Context(client, SettingsBuilder(SERVICE_URL).sync_config({'entities': {'NotValid': {}}}).build())

    assert str(e.value) == 'Invalid entity type name: "NotValid"'


def test_odata_sync_by_fk_invalid_principal(client):
    with pytest.raises(ValueError) as e:
        Context(client,
                SettingsBuilder(SERVICE_URL).sync_config(
                    {'entities': {'Voting': {'sync_by': 'InvalidPrincipal'}}}).build())

    assert str(e.value) == 'Invalid entity type name: "InvalidPrincipal"'


@pytest.mark.parametrize('config, dependant, principal, comment', [
    ({'entities': {'Tags': {'sync_by': 'Tags'}}}, 'Tags', 'Tags', 'Self reference'),
    ({'entities': {'Voting': {'sync_by': 'Session'}}}, 'Voting', 'Session', 'Implicit reference'),
])
def test_odata_faulty_fk_relation(client, config, dependant, principal, comment):
    with pytest.raises(ValueError) as e:
        Context(client, SettingsBuilder(SERVICE_URL).sync_config(config).build())

    assert str(e.value) == f'"{dependant}" is not a dependency of "{principal}"', comment


def test_odata_faulty_fk_multiplicity(client):
    with pytest.raises(ValueError) as e:
        Context(client,
                SettingsBuilder(SERVICE_URL).sync_config({'entities': {'Business': {'sync_by': 'Council'}}}).build())

    assert str(e.value) == '"Business" can not by synced by "Council" due to its multiplicity'


@pytest.mark.parametrize('config, entity_type_name, properties, comment', [
    ({}, 'Tags', ['ID', 'Language', 'TagName'], 'Empty configuration'),
    ({'entities': {'Tags': {'selected_properties': ['ID', 'Language']}}}, 'Tags', ['ID', 'Language'], 'Skip TagName'),
])
def test_selected_properties(client, config, entity_type_name, properties, comment):
    ctx = Context(client, SettingsBuilder(SERVICE_URL).sync_config(config).build())
    entity_type = ctx.get_entity_type_by_name(entity_type_name)
    assert ctx.odata_selected_properties(entity_type) == properties, comment
