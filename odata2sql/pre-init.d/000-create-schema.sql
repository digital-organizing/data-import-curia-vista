-- Create schemas, which allows to express the guarantees given by their respective tables/views/etc.

CREATE SCHEMA odata;
COMMENT ON SCHEMA odata is 'Data as provided by the OData service';

CREATE SCHEMA private;
COMMENT ON SCHEMA private is 'Implementation details for the OData import tool. Do not rely on its content!';

CREATE SCHEMA stable;
COMMENT ON SCHEMA stable is 'Public API. The one and only place to expect some kind of backward compatibility. Program against its content.';

CREATE SCHEMA inconsistent;
COMMENT ON SCHEMA inconsistent is 'Content in here points towards detected problems';
