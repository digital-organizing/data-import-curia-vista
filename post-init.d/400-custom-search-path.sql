-- This is a temporary workaround. The votelog backend does not yet explicitly use the tables in the odata schema.

DO $$
BEGIN
EXECUTE '
ALTER DATABASE ' || current_database() || ' SET SEARCH_PATH TO odata, public';
END; $$;
