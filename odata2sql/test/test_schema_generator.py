from odata2sql.schema_generator import LeanAndMean


def test_schema_generator_serious(context):
    ddl = LeanAndMean(context).odata_to_ddl()
    assert ddl == """CREATE TABLE odata.person (
  "id" integer NOT NULL,
  "language" char(2) NOT NULL,
  last_name varchar(60),
  date_of_birth timestamp,
  PRIMARY KEY ("id", "language")
);

CREATE TABLE odata.person_address (
  person_number integer,
  "id" uuid NOT NULL,
  "language" char(2) NOT NULL,
  city text,
  PRIMARY KEY ("id", "language")
);"""
