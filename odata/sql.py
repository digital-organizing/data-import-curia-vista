from odata.odata import create_parser, Association, _to_snake_case

EDM_TO_SQL_SIMPLE = {
    'Edm.Boolean': 'boolean',
    'Edm.Int16': 'smallint',
    'Edm.Int32': 'integer',
    'Edm.Int64': 'bigint',
    'Edm.DateTime': 'timestamp',
    'Edm.Guid': 'uuid',
    'Edm.DateTimeOffset': 'timestamp',
}


def key_to_schema(self):
    return f"PRIMARY KEY ({', '.join(self.property_refs)})"


def property_to_schema(self):
    res = _to_snake_case(self.name)

    if self.type in EDM_TO_SQL_SIMPLE:
        type_sql = EDM_TO_SQL_SIMPLE[self.type]
    else:
        if self.type == "Edm.String":
            if isinstance(self.max_length, int):
                if self.fixed_length:
                    type_sql = f"char({self.max_length})"
                else:
                    type_sql = f"varchar({self.max_length})"
            else:
                assert self.fixed_length is False
                type_sql = "TEXT"
        else:
            raise RuntimeError("Unknown type: " + self.type)
    res += f" {type_sql}"
    if not self.nullable:
        res += " NOT NULL"
    return res


def entity_type_to_schema(self):
    res = f"CREATE TABLE odata.{self.table_name} (\n  "
    res += ",\n  ".join([property_to_schema(p) for p in self.properties]) + ","
    res += f"\n  {key_to_schema(self.key)}"
    res += "\n);"
    return res


def principal_or_dependent_to_schema(principal_or_dependent):
    return ", ".join(principal_or_dependent.property_ref)


def association_to_schema(self):
    if self.principal.multiplicity == '0..1' and self.dependent.multiplicity == '*':
        return None
    elif self.principal.multiplicity == '1' and self.dependent.multiplicity == '*':
        dependent = Association.BROKEN_REFERENTIAL_CONSTRAINTS[
            self.constrain_name] if self.constrain_name in Association.BROKEN_REFERENTIAL_CONSTRAINTS else principal_or_dependent_to_schema(
            self.referential_constraint.dependent)
        return (f"""ALTER TABLE odata.{self.dependent.table_name}\n"""
                f"""ADD CONSTRAINT {self.constrain_name} FOREIGN KEY ({dependent}) REFERENCES odata.{self.principal.table_name} ({principal_or_dependent_to_schema(self.referential_constraint.principal)});""")
    else:
        raise RuntimeError(
            f"Not implemented Association: Principal='{self.principal.multiplicity}', Dependent='{self.dependent.multiplicity}'")


def odata_to_schema(odata):
    sections = []
    if odata.entity_types:
        sections.append('\n\n'.join([entity_type_to_schema(e) for e in odata.entity_types]))
    if odata.associations:
        sections.append(
            '\n\n'.join(association_to_schema(a) for a in odata.associations if association_to_schema(a)))

    return "CREATE SCHEMA IF NOT EXISTS odata;\n\n" + "\n\n".join(sections)


def to_schema(metadata: str):
    """
    Parse an XML string of an Open Data Protocol (OData) schema and return the SQL query to construct the respective
    table in a SQL database.
    """
    return odata_to_schema(create_parser(metadata))
