-- Create schemas which allow to express the guarantees given by individual tables/views/etc.

CREATE SCHEMA IF NOT EXISTS odata;
COMMENT ON SCHEMA odata is 'Data imported form OData service. Exactly as (un)stable as the fetched service is.';

CREATE SCHEMA IF NOT EXISTS private;
COMMENT ON SCHEMA private is 'Implementation details for the Curia Vista import tools. Do not rely on its content!';

CREATE SCHEMA IF NOT EXISTS inconsistent;
COMMENT ON SCHEMA inconsistent is 'Content in here points towards detected problems';

CREATE SCHEMA IF NOT EXISTS stable;
COMMENT ON SCHEMA stable is 'The one and only place to expect some kind of backward compatibility. Program against its content.';
