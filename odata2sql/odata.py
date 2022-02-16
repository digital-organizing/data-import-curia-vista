#!/usr/bin/env python3
import dataclasses
import logging
import re
import uuid
from functools import cached_property
from typing import Optional, List, Iterable, Set

import requests
from toposort import toposort

import pyodata
from pyodata.v2.model import EntityType, Association, Config, EndRole

log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Multiplicity:
    dependant: EntityType
    dependant_multiplicity: str
    principal: EntityType
    principal_multiplicity: str


class Context:
    def __init__(self,
                 client: pyodata.Client,
                 include: Optional[Iterable[str]],
                 skip: Optional[Iterable[str]],
                 url: str,
                 odata_filter: Optional[str],
                 ):
        self._client = client
        self._include = set(include) if include else set()
        self._skip = set(skip) if skip else set()
        for et_name in (self.skip | self.include):
            try:
                self.client.schema.entity_type(et_name)
            except KeyError as e:
                log.error(f'Invalid entity type name: "{et_name}"')
                raise e
        self._url = url
        if not self.url.endswith('/odata.svc'):
            raise ValueError('Expecting OData URLs to end with /odata.svc')
        self._odata_filter = odata_filter
        self.session_id = uuid.uuid4()

    @cached_property
    def client(self) -> pyodata.Client:
        return self._client

    @cached_property
    def skip(self) -> Optional[Iterable[str]]:
        return self._skip

    @cached_property
    def include(self) -> Optional[Iterable[str]]:
        return self._include

    @cached_property
    def url(self) -> str:
        return self._url

    @cached_property
    def odata_filter(self) -> str:
        return self._odata_filter

    @cached_property
    def skipped_entity_types(self) -> Set[EntityType]:
        """All skipped (ignored) entity types."""
        return set(x for x in self.client.schema.entity_types if x.name in self.skip)

    @cached_property
    def included_entity_types(self) -> Set[EntityType]:
        """Included entity types including their dependencies"""
        if not self.include:
            return set(self.client.schema.entity_types)
        result = set()
        for et in set(et for et in self.client.schema.entity_types if et.name in self.include):
            result.add(et)
            result |= self.get_dependencies(et, recursive=True)
        return result

    def get_dependencies(self, dependant: EntityType, recursive: bool) -> Set[EntityType]:
        """Entity types which @entity_type depends on (links to), optionally @recursive-ly"""
        result = set()
        for a in self.client.schema.associations:
            if dependant.name == a.referential_constraint.dependent.name:
                principal = self.client.schema.entity_type(a.referential_constraint.principal.name)
                result.add(principal)
                if recursive:
                    result |= self.get_dependencies(principal, True)
        return result

    @cached_property
    def associations(self) -> Set[Association]:
        """All associations in the OData schema."""
        return set(x for x in self.client.schema.associations)

    def get_topology(self) -> List[Set[EntityType]]:
        map_of_dependencies = {}
        for entity_type in self.included_entity_types:
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

    def get_dependency_with_multiplicity(self, entity_type: EntityType) -> Iterable[Multiplicity]:
        """Multiplicity for all entity types which @entity_type depends on"""
        for association in self.client.schema.associations:
            assert len(association.end_roles) == 2
            assert association.referential_constraint.principal.name == association.end_roles[0].role
            principal, dependent = association.end_roles
            if dependent.entity_type != entity_type:
                continue
            assert dependent.multiplicity == EndRole.MULTIPLICITY_ZERO_OR_MORE
            yield Multiplicity(dependent.entity_type, dependent.multiplicity, principal.entity_type,
                               principal.multiplicity)
