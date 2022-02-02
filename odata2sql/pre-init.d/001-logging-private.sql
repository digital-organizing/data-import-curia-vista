CREATE TABLE private.import_log(
    id SERIAL PRIMARY KEY,
    session_id uuid NOT NULL,
    occurence_at timestamp DEFAULT NOW(),
    level_number INT,
    level_name TEXT,
    message TEXT,
    entity_type TEXT,
    entity_id TEXT,
    context TEXT
);
