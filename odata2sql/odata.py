#!/usr/bin/env python3
import logging
import re
import uuid
from functools import cached_property
from typing import Optional, List, Iterable, Set

import requests
from toposort import toposort

import pyodata
from pyodata.v2.model import EntityType, Association, Config

log = logging.getLogger(__name__)


class Context:
    def __init__(self,
                 client: pyodata.Client,
                 include: Optional[Iterable[str]],
                 skip: Optional[Iterable[str]],
                 url: str):
        self.client = client
        self.include = set(include) if include else set()
        self.skip = set(skip) if skip else set()
        for et_name in (self.skip | self.include):
            try:
                self.client.schema.entity_type(et_name)
            except KeyError as e:
                log.error(f'Invalid entity type name: "{et_name}"')
                raise e
        self.url = url
        if not self.url.endswith('/odata.svc'):
            raise ValueError('Expecting OData URLs to end with /odata.svc')
        self.session_id = uuid.uuid4()

    @classmethod
    def from_args(cls, args):
        if args.requests_cache:
            import requests_cache
            requests_cache.install_cache(args.requests_cache)

        client = pyodata.Client(args.url, requests.Session(), config=Config(retain_null=True))
        return Context(client,
                       getattr(args, 'include', set()),
                       getattr(args, 'skip', set()),
                       args.url)

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
