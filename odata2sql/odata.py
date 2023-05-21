#!/usr/bin/env python3
import dataclasses
import logging
import re
import uuid
from functools import cached_property
from typing import Optional, List, Iterable, Set, Dict, Union

import pyodata
import requests
from pyodata.v2.model import EntityType, Association, EndRole, Config, ReferentialConstraint
from toposort import toposort

log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Multiplicity:
    dependant: EntityType
    dependant_multiplicity: str
    principal: EntityType
    principal_multiplicity: str


@dataclasses.dataclass(frozen=True)
class Settings:
    """
    Aggregate of all relevant runtime-adjustable parameters. Optimized to be easy to use when coding.

    Please note: This class needs to be pickl-able as it gets sent to sub-processes/-threads.
    """
    # Format:
    # {
    #     'sync_unconfigured_entities': True,
    #     'filter': 'Language eq 'DE', # Defaults to None
    #     'entities': {
    #         'ENTITY_TYPE_NAME': {
    #             'sync': True,
    #             'sync_by': 'REFERENCED_ENTITY_TYPE_NAME',
    #             'filter': 'ODATA_FILTER_EXPRESSION',
    #             'selected_properties': {
    #                 '<Property Name #1>',
    #                 '<Property Name #2>',
    #             },
    #         },
    #     }
    # }
    sync_config: Dict[str, Union[Dict, bool]]
    # URL to the OData service
    url: str
    # Session ID
    # TODO: Allow specifying session_id, resume interrupted sync
    session_id: uuid.UUID
    # Maximal number of simultaneous requests towards the OData server
    odata_server_max_connections: int

    @cached_property
    def sync_unconfigured_entities(self) -> bool:
        """Whether to include unconfigured entities."""
        try:
            return self.sync_config['sync_unconfigured_entities']
        except KeyError:
            return True

    @cached_property
    def configured_entities(self) -> Set[str]:
        """Names of configured entities"""
        try:
            return set(k for k, v in self.sync_config['entities'].items())
        except KeyError:
            return set()

    @cached_property
    def include(self) -> Set[str]:
        """Names of entity types configured to sync"""
        try:
            default = self.sync_unconfigured_entities
            return set(k for k, v in self.sync_config['entities'].items() if v.get('sync', default))
        except KeyError:
            return set()

    @cached_property
    def skip(self) -> Set[str]:
        """Names of entity types explicitly marked not to be synced"""
        try:
            return set(k for k, v in self.sync_config['entities'].items() if not v.get('sync', True))
        except KeyError:
            return set()

    def odata_filter_for_entity_type(self, entity_type_name: str) -> Optional[str]:
        """OData compatible $filter System Query Option for @entity_type_name"""
        global_filter = self.sync_config.get('filter', None)
        try:
            entity_filter = self.sync_config['entities'][entity_type_name]['filter']
            if global_filter:
                return f'({global_filter}) and ({entity_filter})'
            return entity_filter
        except KeyError:
            return global_filter

    def odata_sync_by_fk(self, entity_type_name: str) -> Optional[str]:
        """If existing, name of entity type to sync @entity_type_name by using the FK relationship"""
        try:
            return self.sync_config['entities'][entity_type_name]['sync_by']
        except KeyError:
            pass

    def odata_selected_properties(self, entity_type_name: str) -> Optional[List[str]]:
        """If configured, @entity_type_name's properties to select, retrieve their values from the OData server"""
        try:
            return self.sync_config['entities'][entity_type_name]['selected_properties']
        except KeyError:
            pass


class SettingsBuilder:
    def __init__(self, url=None):
        self._settings = {
            'sync_config': {},
            'odata_server_max_connections': 20,
            'session_id': uuid.uuid4(),
            'url': url,
        }

    def sync_config(self, sync_config: Dict[str, Union[Dict, bool]]) -> 'SettingsBuilder':
        self._settings['sync_config'] = sync_config
        return self

    def url(self, url: str) -> 'SettingsBuilder':
        self._settings['url'] = url
        return self

    def session_id(self, session_id: uuid.UUID) -> 'SettingsBuilder':
        self._settings['session_id'] = session_id
        return self

    def odata_server_max_connections(self, odata_server_max_connections: int) -> 'SettingsBuilder':
        self._settings['odata_server_max_connections'] = odata_server_max_connections
        return self

    def build(self) -> Settings:
        if not self._settings['url']:
            raise ValueError('URL not specified!')
        if not self._settings['url'].endswith('/odata.svc'):
            raise ValueError('Expecting OData URLs to end with /odata.svc')
        if (count := self._settings['odata_server_max_connections']) <= 0:
            raise ValueError(f'Invalid connection count: {count}')
        return Settings(**self._settings)


class Context:
    def __init__(self, client: pyodata.Client, settings: Settings):
        self._client = client
        self._settings = settings
        self._validate_settings_entity_type_names()
        self._validate_settings_sync_by_fk()
        self._validate_settings_selected_properties()

    def _validate_settings_entity_type_names(self):
        for et_name in self._settings.configured_entities:
            try:
                self.get_entity_type_by_name(et_name)
            except KeyError:
                log.error(f'Invalid entity type name: "{et_name}"')
                raise ValueError(f'Invalid entity type name: "{et_name}"')

    def _validate_settings_sync_by_fk(self):
        for dependant_name in self._settings.configured_entities:
            if not (principal_name := self._settings.odata_sync_by_fk(dependant_name)):
                continue
            dependant = self.get_entity_type_by_name(dependant_name)
            try:
                principal = self.get_entity_type_by_name(principal_name)
            except KeyError:
                raise ValueError(f'Invalid entity type name: "{principal_name}"')
            if principal not in self.get_dependencies(dependant, recursive=False):
                raise ValueError(f'"{dependant_name}" is not a dependency of "{principal_name}"')
            if not self.can_be_synced_by(dependant, principal):
                raise ValueError(
                    f'"{dependant_name}" can not by synced by "{principal_name}" due to its multiplicity')

    def _validate_settings_selected_properties(self):
        for entity_type_name in self._settings.configured_entities:
            if not (property_name_list := self._settings.odata_selected_properties(entity_type_name)):
                continue
            try:
                entity_type = self.get_entity_type_by_name(entity_type_name)
            except KeyError:
                raise ValueError(f'Entity type "{entity_type_name}" does not exist')
            missing_keys_names = sorted(set(x.name for x in entity_type.key_proprties) - set(property_name_list))
            if len(missing_keys_names):
                raise ValueError('Entity type "{}" requires key properties: "{}"'.format(entity_type_name, '", "'.join(
                    missing_keys_names)))
            inexistent_property_names = sorted(
                set(property_name_list) - set(get_property_names_of_entity_type(entity_type)))
            if len(inexistent_property_names):
                raise ValueError('Entity type "{}" has no such properties: "{}"'.format(entity_type_name, '", "'.join(
                    inexistent_property_names)))
            missing_non_nullable_property_names = sorted(
                set(x.name for x in entity_type.proprties() if not x.nullable) - set(property_name_list))
            if len(missing_non_nullable_property_names):
                raise ValueError(
                    'Entity type "{}" requires non-nullable properties: "{}"'.format(entity_type_name, '", "'.join(
                        missing_non_nullable_property_names)))

    @classmethod
    def from_settings(cls, settings: Settings):
        client = pyodata.Client(settings.url, requests.Session(), config=Config(retain_null=True))
        return Context(client, settings)

    @cached_property
    def client(self) -> pyodata.Client:
        return self._client

    @cached_property
    def settings(self) -> Settings:
        return self._settings

    @cached_property
    def url(self) -> str:
        return self._settings.url

    @cached_property
    def skip(self) -> Set[EntityType]:
        return set(self.get_entity_type_by_name(et) for et in self._settings.skip)

    @cached_property
    def include(self) -> Set[EntityType]:
        """Included entity types plus their dependencies"""
        if self._settings.sync_unconfigured_entities:
            entity_types = set(self.client.schema.entity_types)
        else:
            entity_types = set(self.get_entity_type_by_name(name) for name in self._settings.include)
        for et in list(entity_types):
            entity_types |= self.get_dependencies(et, recursive=True)

        return entity_types - set(self.get_entity_type_by_name(et) for et in self._settings.skip)

    @cached_property
    def session_id(self) -> uuid.UUID:
        """Sync session ID"""
        return self._settings.session_id

    @cached_property
    def associations(self) -> Set[Association]:
        """All associations in the OData schema."""
        return set(self.client.schema.associations)

    def odata_filter_for_entity_type(self, entity_type: EntityType) -> Optional[str]:
        """OData compatible $filter System Query Option for @entity_type_name"""
        return self._settings.odata_filter_for_entity_type(entity_type.name)

    def can_be_synced_by(self, dependant: EntityType, principal: EntityType) -> bool:
        for multiplicity in self.get_dependencies_with_multiplicity(dependant):
            if multiplicity.principal == principal and multiplicity.principal_multiplicity == EndRole.MULTIPLICITY_ONE:
                return True
        return False

    def odata_sync_by_fk(self, entity_type: EntityType) -> Optional[EntityType]:
        """Entity type to sync @entity_type_name by using the FK relationship"""
        if entity_type_name := self._settings.odata_sync_by_fk(entity_type.name):
            return self.get_entity_type_by_name(entity_type_name)

    def odata_selected_properties(self, entity_type: EntityType) -> List[str]:
        if property_names := self.settings.odata_selected_properties(entity_type.name):
            return property_names
        return get_property_names_of_entity_type(entity_type)

    def get_dependencies(self, dependant: EntityType, recursive: bool) -> Set[EntityType]:
        """Entity types which @entity_type depends on (links to), optionally @recursive-ly"""
        result = set()
        for a in self.associations:
            if dependant.name == a.referential_constraint.dependent.name:
                principal = self.client.schema.entity_type(a.referential_constraint.principal.name)
                result.add(principal)
                if recursive:
                    result |= self.get_dependencies(principal, True)
        return result

    def get_topology(self) -> List[Set[EntityType]]:
        map_of_dependencies = {}
        for entity_type in self.include:
            map_of_dependencies[entity_type] = []
            for target in self.get_dependencies(entity_type, False):
                map_of_dependencies[entity_type].append(target)
        return list(toposort(map_of_dependencies))

    def adjust_next_url(self, next_url: str):
        """Adjust _next URL provided by the server to use specified URL"""
        if next_url.startswith(self.url):
            return next_url
        if m := re.match(r'^.+/odata.svc(?P<interesting_part>.+)$', next_url):
            return self.url + m.group('interesting_part')
        raise ValueError(f'Unexpected URL: "{next_url}"')

    def get_dependencies_with_multiplicity(self, entity_type: EntityType) -> Iterable[Multiplicity]:
        """Multiplicity for all entity types which @entity_type depends on"""
        for association in self.associations:
            assert len(association.end_roles) == 2
            assert association.referential_constraint.principal.name == association.end_roles[0].role
            principal, dependent = association.end_roles
            if dependent.entity_type != entity_type:
                continue
            assert dependent.multiplicity == EndRole.MULTIPLICITY_ZERO_OR_MORE
            yield Multiplicity(dependent.entity_type, dependent.multiplicity, principal.entity_type,
                               principal.multiplicity)

    def get_referential_constrain(self, dependant: EntityType, principal: EntityType) -> Optional[
        ReferentialConstraint]:
        for association in self.associations:
            referential_constraint = association.referential_constraint
            if principal.name == referential_constraint.principal.name and dependant.name == referential_constraint.dependent.name:
                return referential_constraint

    def get_entity_type_by_name(self, entity_type_name: str) -> EntityType:
        """Shortcut - throws KeyError if @entity_type_name is unknown"""
        return self.client.schema.entity_type(entity_type_name)

    def get_entity_types_by_names(self, entity_type_names: Iterable[str]) -> Set[EntityType]:
        return {self.get_entity_type_by_name(entity_type_name) for entity_type_name in entity_type_names}

    def get_entity_type_total_count(self, entity_type: EntityType) -> int:
        """Total number of entities of type @entity_type the remote server stores"""
        request = getattr(self.client.entity_sets, entity_type.name).get_entities()
        if filter_ := self.odata_filter_for_entity_type(entity_type):
            request.filter(filter_)
        return request.count().execute()


def get_property_names_of_entity_type(entity_type: EntityType) -> List[str]:
    """Name of all properties for @entity_type

    Returning a list in order to retain the original ordering, which can be important.
    """
    return [p.name for p in entity_type.proprties()]
