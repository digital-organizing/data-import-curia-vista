from typing import Iterable

from odata2sql import sql
from odata2sql.odata import Context
from pyodata.v2.model import StructTypeProperty, EntityType


def _key_to_ddl(key_properties: Iterable[StructTypeProperty]) -> str:
    return f"PRIMARY KEY ({', '.join(sql.to_pg_name(x.name) for x in key_properties)})"


class LeanAndMean:
    """
    Enforce derivable constrains EXCEPT for the foreign key (FK) ones

    The FK constraints get violated too often by Curia Vista.
    """

    EDM_TO_SQL_SIMPLE = {
        "Edm.Boolean": "boolean",
        "Edm.DateTime": "timestamp",
        "Edm.DateTimeOffset": "timestamp",
        "Edm.Guid": "uuid",
        "Edm.Int16": "smallint",
        "Edm.Int32": "integer",
        "Edm.Int64": "bigint",
    }

    def __init__(self, context: Context):
        self._context = context

    def __str__(self):
        return "Serious schema generator"

    @classmethod
    def _property_to_ddl(cls, p: StructTypeProperty) -> str:
        res = sql.to_pg_name(p.name)
        type_name = p.typ.name

        try:
            type_sql = cls.EDM_TO_SQL_SIMPLE[type_name]
        except KeyError:
            if type_name == "Edm.String":
                if p.max_length > 0:
                    if p.fixed_length:
                        type_sql = f"char({p.max_length})"
                    else:
                        type_sql = f"varchar({p.max_length})"
                else:
                    type_sql = "text"
            else:
                raise RuntimeError(f"Unknown type: {type_name}")
        res += f" {type_sql}"
        if not p.nullable:
            res += " NOT NULL"
        return res

    @classmethod
    def _entity_type_to_ddl(cls, entity_type: EntityType) -> str:
        res = f"CREATE TABLE odata.{sql.to_pg_name(entity_type.name)} (\n  "
        res += ",\n  ".join([cls._property_to_ddl(p) for p in entity_type.proprties()]) + ","
        res += f"\n  {_key_to_ddl(entity_type.key_proprties)}"
        res += "\n);"
        return res

    def odata_to_ddl(self) -> str:
        sections = []
        for entity_type in self._context.client.schema.entity_types:
            sections.append(LeanAndMean._entity_type_to_ddl(entity_type))
        return '\n\n'.join(sections)
