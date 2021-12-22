import logging
from pathlib import Path
from typing import Iterable, Optional

from psycopg2._psycopg import cursor

from odata2sql.sql import database_connection, to_pg_name
from odata2sql.odata import Context
from pyodata.v2.model import EntityType, StructTypeProperty, Association

log = logging.getLogger(__name__)

EDM_TO_SQL_SIMPLE = {
    "Edm.Boolean": "boolean",
    "Edm.Int16": "smallint",
    "Edm.Int32": "integer",
    "Edm.Int64": "bigint",
    "Edm.DateTime": "timestamp",
    "Edm.Guid": "uuid",
    "Edm.DateTimeOffset": "timestamp",
}

# Needs to be in sync with the schemas being created by various scripts (e.g in the pre-init.d folder)
SCHEMAS = [
    'inconsistent',
    'stable',
    'private',
    'odata',
]


def key_to_schema(key_properties: Iterable[StructTypeProperty]) -> str:
    return f"PRIMARY KEY ({', '.join(to_pg_name(x.name) for x in key_properties)})"


def _property_to_schema(p: StructTypeProperty) -> str:
    res = to_pg_name(p.name)
    type_name = p.typ.name

    try:
        type_sql = EDM_TO_SQL_SIMPLE[type_name]
    except KeyError:
        if type_name == "Edm.String":
            if p.max_length > 0:
                # Can not use char as pyodata does not extract the FixedLength attribute
                type_sql = f"varchar({p.max_length})"
            else:
                type_sql = "TEXT"
        else:
            raise RuntimeError(f"Unknown type: {type_name}")
    res += f" {type_sql}"
    if not p.nullable:
        res += " NOT NULL"
    return res


def _entity_type_to_schema(entity_type: EntityType) -> str:
    res = f"CREATE TABLE odata.{to_pg_name(entity_type.name)} (\n  "
    res += ",\n  ".join([_property_to_schema(p) for p in entity_type.proprties()]) + ","
    res += f"\n  {key_to_schema(entity_type.key_proprties)}"
    res += "\n);"
    return res


def _association_to_schema(association: Association) -> Optional[str]:
    assert len(association.end_roles) == 2
    if association.referential_constraint.principal.name == association.end_roles[0].role:
        principal, dependent = association.end_roles
    else:
        dependent, principal = association.end_roles
    if principal.multiplicity == '0..1' and dependent.multiplicity == '*':
        # No restrictions needed
        return None
    if principal.multiplicity == '1' and dependent.multiplicity == '*':
        principal_table_name = to_pg_name(principal.role)
        principal_depend_properties = ", ".join(
            to_pg_name(x) for x in association.referential_constraint.principal.property_names)
        dependent_table_name = to_pg_name(dependent.role)
        dependent_depend_properties = ", ".join(
            to_pg_name(x) for x in association.referential_constraint.dependent.property_names)
        return (f"""ALTER TABLE odata.{dependent_table_name}\n"""
                f"""ADD CONSTRAINT odata_{to_pg_name(association.name)} FOREIGN KEY ({dependent_depend_properties}) REFERENCES odata.{principal_table_name} ({principal_depend_properties});""")

    raise NotImplemented(f"Principal='{principal.multiplicity}', Dependent='{dependent.multiplicity}'")


def _odata_to_ddl(odata: Context):
    sections = []
    for entity_type in odata.all_entity_types:
        sections.append(_entity_type_to_schema(entity_type))
    for association in odata.associations:
        if s := _association_to_schema(association):
            sections.append(s)
    return "\n\n".join(sections)


def _run_sql_scripts(cur: cursor, directory_name: Path) -> None:
    directory_name = directory_name.resolve()  # Allow caller to be lenient
    if not directory_name.is_dir():
        raise ValueError(f'Not a valid path: {directory_name}')
    for entry in directory_name.glob('*.sql'):
        with open(entry) as f:
            script_content = f.read()
        log.info(f'Executing script {entry}')
        log.debug(f'Content of {entry}: {script_content}')
        cur.execute(script_content)


def work(odata: Context, args):
    """Initialize database structure"""
    with database_connection(args) as con, con.cursor() as cur:
        if args.force:
            cur.execute(f'DROP SCHEMA IF EXISTS {", ".join(SCHEMAS)} CASCADE;')
        log.info(f'Run pre-init scripts')
        _run_sql_scripts(cur, Path(__file__).parent.joinpath('pre-init.d'))
        schema = _odata_to_ddl(odata)
        log.info(f'Create OData reflecting schema')
        log.debug(f'Schema being applied: {schema}')
        cur.execute(schema)
        log.info(f'Run post-init scripts')
        _run_sql_scripts(cur, Path(__file__).parent.joinpath('post-init.d'))
        log.info(f'Run service specific scripts')
        _run_sql_scripts(cur, Path(__file__).parent.joinpath('../post-init.d'))
